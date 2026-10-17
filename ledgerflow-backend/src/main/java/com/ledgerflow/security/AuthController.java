package com.ledgerflow.security;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.util.*;
import java.sql.Timestamp;
import java.time.Instant;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.transaction.annotation.Transactional;

@RestController
@RequestMapping("/api/auth")
public class AuthController {
    private final JdbcTemplate jdbc; private final PasswordEncoder passwords;
    private final SessionTokens tokens; private final BankingAccess access;
    private final String dummy;
    public AuthController(JdbcTemplate jdbc,PasswordEncoder passwords,SessionTokens tokens,BankingAccess access) {
        this.jdbc=jdbc;this.passwords=passwords;this.tokens=tokens;this.access=access;
        this.dummy=passwords.encode(UUID.randomUUID().toString());
    }
    public record Login(@NotBlank @Size(max=80) String username,@NotBlank @Size(max=72) String password) {}
    private record User(UUID id,String password,String role,boolean enabled,int failures,Timestamp locked) {}
    @PostMapping("/login") @Transactional
    public ResponseEntity<?> login(@Valid @RequestBody Login body) {
        if(body.password().getBytes(java.nio.charset.StandardCharsets.UTF_8).length>72)
            return ResponseEntity.status(401).body(Map.of("code","INVALID_CREDENTIALS"));
        var rows=jdbc.query("SELECT id,password_hash,role,enabled,failed_logins,locked_until FROM app_users WHERE username=? FOR UPDATE",
            (r,n)->new User(r.getObject(1,UUID.class),r.getString(2),r.getString(3),r.getBoolean(4),r.getInt(5),r.getTimestamp(6)),body.username());
        User u=rows.isEmpty()?null:rows.getFirst();
        boolean matched=passwords.matches(body.password(),u==null?dummy:u.password());
        if(u==null || !u.enabled() || (u.locked()!=null && u.locked().toInstant().isAfter(Instant.now())) || !matched) {
            if(u!=null && u.enabled() && (u.locked()==null || !u.locked().toInstant().isAfter(Instant.now()))) {
                int failures=u.locked()!=null?1:u.failures()+1;
                jdbc.update("UPDATE app_users SET failed_logins=?,locked_until=? WHERE id=?",failures,
                    failures>=5?Timestamp.from(Instant.now().plusSeconds(300)):null,u.id());
            }
            return ResponseEntity.status(401).body(Map.of("code","INVALID_CREDENTIALS"));
        }
        jdbc.update("UPDATE app_users SET failed_logins=0,locked_until=NULL WHERE id=?",u.id());
        return ResponseEntity.ok(Map.of("token",tokens.issue(u.id()),"username",body.username(),"role",u.role(),"expiresIn",28800));
    }
    @GetMapping("/me") public Map<String,String> me() {
        return Map.of("username",access.username(),"role",jdbc.queryForObject("SELECT role FROM app_users WHERE username=?",String.class,access.username()));
    }
    @PostMapping("/logout") public Map<String,String> logout(@RequestHeader("Authorization") String header) {
        tokens.revoke(header.substring(7)); return Map.of("status","LOGGED_OUT");
    }
}
