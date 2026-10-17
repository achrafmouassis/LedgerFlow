package com.ledgerflow.security;

import java.nio.charset.StandardCharsets;
import java.security.*;
import java.time.Instant;
import java.sql.Timestamp;
import java.util.*;
import org.springframework.stereotype.Service;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

@Service
public class SessionTokens {
    private final JdbcTemplate jdbc;
    private final SecureRandom random = new SecureRandom();
    public SessionTokens(JdbcTemplate jdbc) { this.jdbc = jdbc; }
    public static String hash(String token) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(token.getBytes(StandardCharsets.UTF_8))); }
        catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }
    public String issue(UUID user) {
        byte[] bytes = new byte[32]; random.nextBytes(bytes);
        String token = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        jdbc.update("DELETE FROM app_sessions WHERE expires_at < CURRENT_TIMESTAMP");
        jdbc.update("INSERT INTO app_sessions(token_hash,user_id,expires_at) VALUES(?,?,?)",hash(token),user,Timestamp.from(Instant.now().plusSeconds(28800)));
        return token;
    }
    public UsernamePasswordAuthenticationToken authenticate(String token) {
        if (!token.matches("[A-Za-z0-9_-]{43}")) return null;
        var found = jdbc.query("""
            SELECT u.username,u.role FROM app_sessions s JOIN app_users u ON u.id=s.user_id
            WHERE s.token_hash=? AND s.expires_at>CURRENT_TIMESTAMP AND u.enabled=TRUE
            """,(r,n) -> new UsernamePasswordAuthenticationToken(r.getString(1),null,
                List.of(new SimpleGrantedAuthority("ROLE_"+r.getString(2)))),hash(token));
        return found.isEmpty() ? null : found.getFirst();
    }
    public void revoke(String token) { jdbc.update("DELETE FROM app_sessions WHERE token_hash=?",hash(token)); }
}
