package com.ledgerflow.security;

import java.util.*;
import java.math.BigDecimal;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.test.context.support.WithMockUser;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.junit.jupiter.api.Assertions.*;
import com.ledgerflow.banking.*;

@SpringBootTest @AutoConfigureMockMvc @ActiveProfiles("test")
class ApiSecurityTests {
    @Autowired MockMvc mvc;
    @Autowired JdbcTemplate jdbc;
    @Autowired PasswordEncoder passwords;
    @Autowired SessionTokens tokens;
    @Autowired BankingService bank;
    UUID user; String username;
    @BeforeEach void setup() {
        user=UUID.randomUUID(); username="security-"+user.toString().substring(0,8);
        jdbc.update("INSERT INTO app_users(id,username,password_hash,role) VALUES(?,?,?,'CUSTOMER')",user,username,passwords.encode("a-long-test-password"));
    }
    @Test void anonymousDeniedButFrontendAndHealthPublic() throws Exception {
        mvc.perform(get("/api/accounts")).andExpect(status().isUnauthorized());
        mvc.perform(get("/api/analytics/pipeline")).andExpect(status().isUnauthorized());
        mvc.perform(get("/")).andExpect(status().isOk());
        mvc.perform(get("/actuator/health")).andExpect(status().isOk());
    }
    @Test void validLoginProducesHashedRevocableToken() throws Exception {
        var response=mvc.perform(post("/api/auth/login").contentType("application/json")
            .content("{\"username\":\""+username+"\",\"password\":\"a-long-test-password\"}"))
            .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        String token=new tools.jackson.databind.ObjectMapper().readTree(response).get("token").asString();
        assertEquals(43,token.length());
        assertEquals(1,jdbc.queryForObject("SELECT COUNT(*) FROM app_sessions WHERE token_hash=?",Integer.class,SessionTokens.hash(token)));
        mvc.perform(get("/api/auth/me").header("Authorization","Bearer "+token)).andExpect(status().isOk());
        mvc.perform(post("/api/auth/logout").header("Authorization","Bearer "+token)).andExpect(status().isOk());
        mvc.perform(get("/api/accounts").header("Authorization","Bearer "+token)).andExpect(status().isUnauthorized());
    }
    @Test void expiredAndDisabledSessionsDenied() throws Exception {
        String token=tokens.issue(user);
        jdbc.update("UPDATE app_users SET enabled=FALSE WHERE id=?",user);
        mvc.perform(get("/api/accounts").header("Authorization","Bearer "+token)).andExpect(status().isUnauthorized());
        jdbc.update("UPDATE app_users SET enabled=TRUE WHERE id=?",user);
        jdbc.update("UPDATE app_sessions SET expires_at=TIMESTAMP '2020-01-01 00:00:00' WHERE user_id=?",user);
        mvc.perform(get("/api/accounts").header("Authorization","Bearer "+token)).andExpect(status().isUnauthorized());
    }
    @Test void clientCannotReadOrSpendOthersAccounts() throws Exception {
        var own=bank.create(new BankingApi.CreateAccount("Security own","EUR",new BigDecimal("100")));
        var other=bank.create(new BankingApi.CreateAccount("Security other","EUR",new BigDecimal("100")));
        jdbc.update("INSERT INTO account_access(user_id,account_id) VALUES(?,?)",user,own.id());
        String bearer="Bearer "+tokens.issue(user);
        mvc.perform(get("/api/accounts/"+own.id()).header("Authorization",bearer)).andExpect(status().isOk());
        mvc.perform(get("/api/accounts/"+other.id()).header("Authorization",bearer)).andExpect(status().isForbidden());
        mvc.perform(get("/api/accounts/"+other.id()+"/journal").header("Authorization",bearer)).andExpect(status().isForbidden());
        mvc.perform(post("/api/accounts/"+own.id()+"/deposits").header("Authorization",bearer).header("Idempotency-Key",UUID.randomUUID()).contentType("application/json").content("{\"amount\":1}")).andExpect(status().isForbidden());
        String payload="{\"sourceAccountId\":\""+other.id()+"\",\"destinationAccountId\":\""+own.id()+"\",\"amount\":1}";
        mvc.perform(post("/api/transfers").header("Authorization",bearer).header("Idempotency-Key",UUID.randomUUID()).contentType("application/json").content(payload)).andExpect(status().isForbidden());
        mvc.perform(get("/api/analytics/daily").header("Authorization",bearer)).andExpect(status().isForbidden());
        var list=mvc.perform(get("/api/accounts").header("Authorization",bearer)).andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        assertTrue(list.contains(own.id().toString()));assertFalse(list.contains(other.id().toString()));
        String allowed="{\"sourceAccountId\":\""+own.id()+"\",\"destinationAccountId\":\""+other.id()+"\",\"amount\":1}";
        String key=UUID.randomUUID().toString();
        mvc.perform(post("/api/transfers").header("Authorization",bearer).header("Idempotency-Key",key).contentType("application/json").content(allowed)).andExpect(status().isOk());
        mvc.perform(post("/api/transfers").header("Authorization",bearer).header("Idempotency-Key",key).contentType("application/json").content(allowed)).andExpect(status().isOk());
        assertEquals(0,bank.account(own.id()).balance().compareTo(new BigDecimal("99")));
    }
    @Test @WithMockUser(roles="VIEWER") void viewerCannotWriteOrAdminister() throws Exception {
        mvc.perform(post("/api/accounts").contentType("application/json").content("{\"owner\":\"No\",\"currency\":\"MAD\",\"openingBalance\":0}")).andExpect(status().isForbidden());
        mvc.perform(get("/api/admin/users")).andExpect(status().isForbidden());
        mvc.perform(get("/api/analytics/pipeline")).andExpect(status().isOk());
    }
    @Test void fiveWrongPasswordsLockLogin() throws Exception {
        for(int i=0;i<5;i++) mvc.perform(post("/api/auth/login").contentType("application/json").content("{\"username\":\""+username+"\",\"password\":\"wrong\"}")).andExpect(status().isUnauthorized());
        assertNotNull(jdbc.queryForObject("SELECT locked_until FROM app_users WHERE id=?",java.sql.Timestamp.class,user));
        mvc.perform(post("/api/auth/login").contentType("application/json").content("{\"username\":\""+username+"\",\"password\":\"a-long-test-password\"}")).andExpect(status().isUnauthorized());
    }
    @Test @WithMockUser(roles="ADMIN") void invalidPasswordsAreRejectedWithoutServerError() throws Exception {
        mvc.perform(post("/api/admin/users").contentType("application/json").content("{\"username\":\"bad-password-user\",\"password\":\"            \",\"role\":\"CUSTOMER\"}")).andExpect(status().isBadRequest());
        String unicode="é".repeat(60);
        mvc.perform(post("/api/admin/users").contentType("application/json").content("{\"username\":\"bad-byte-user\",\"password\":\""+unicode+"\",\"role\":\"CUSTOMER\"}")).andExpect(status().isBadRequest());
    }
}
