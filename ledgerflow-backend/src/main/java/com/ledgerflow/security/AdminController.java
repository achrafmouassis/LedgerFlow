package com.ledgerflow.security;

import java.util.*;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.http.HttpStatus;

@RestController @RequestMapping("/api/admin")
public class AdminController {
    private final JdbcTemplate jdbc; private final PasswordEncoder passwords;
    public AdminController(JdbcTemplate jdbc,PasswordEncoder passwords) { this.jdbc=jdbc;this.passwords=passwords; }
    public record CreateUser(@Pattern(regexp="[a-zA-Z0-9._-]{3,80}") @NotNull String username,
        @Size(min=12,max=72) @NotBlank String password,@Pattern(regexp="ADMIN|OPERATOR|VIEWER|CUSTOMER") @NotNull String role) {}
    public record UserView(UUID id,String username,String role,boolean enabled) {}
    @GetMapping("/users") public List<UserView> users() {
        return jdbc.query("SELECT id,username,role,enabled FROM app_users ORDER BY username",(r,n)->new UserView(r.getObject(1,UUID.class),r.getString(2),r.getString(3),r.getBoolean(4)));
    }
    @PostMapping("/users") @ResponseStatus(HttpStatus.CREATED) public UserView create(@Valid @RequestBody CreateUser body) {
        if(body.password().getBytes(java.nio.charset.StandardCharsets.UTF_8).length>72)
            throw new org.springframework.web.server.ResponseStatusException(HttpStatus.BAD_REQUEST,"Password exceeds bcrypt's 72-byte limit");
        UUID id=UUID.randomUUID(); jdbc.update("INSERT INTO app_users(id,username,password_hash,role) VALUES(?,?,?,?)",id,body.username(),passwords.encode(body.password()),body.role());
        return new UserView(id,body.username(),body.role(),true);
    }
    @PutMapping("/users/{user}/accounts/{account}") public Map<String,String> grant(@PathVariable UUID user,@PathVariable UUID account) {
        if(jdbc.queryForObject("SELECT COUNT(*) FROM account_access WHERE user_id=? AND account_id=?",Long.class,user,account)==0)
            jdbc.update("INSERT INTO account_access(user_id,account_id) VALUES(?,?)",user,account);
        return Map.of("status","GRANTED");
    }
    @DeleteMapping("/users/{user}/accounts/{account}") public Map<String,String> revoke(@PathVariable UUID user,@PathVariable UUID account) {
        jdbc.update("DELETE FROM account_access WHERE user_id=? AND account_id=?",user,account); return Map.of("status","REVOKED");
    }
}
