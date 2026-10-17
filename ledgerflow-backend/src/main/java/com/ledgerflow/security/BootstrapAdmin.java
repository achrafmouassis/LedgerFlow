package com.ledgerflow.security;

import java.util.UUID;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.core.env.Environment;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.transaction.annotation.Transactional;

@Component @Profile("!test")
public class BootstrapAdmin implements ApplicationRunner {
    private final JdbcTemplate jdbc; private final Environment env; private final PasswordEncoder passwords;
    public BootstrapAdmin(JdbcTemplate jdbc,Environment env,PasswordEncoder passwords) { this.jdbc=jdbc;this.env=env;this.passwords=passwords; }
    @Override @Transactional public void run(ApplicationArguments args) {
        // Exclusive table lock: a simultaneous scale-out must not create two admins.
        jdbc.execute("LOCK TABLE app_users IN EXCLUSIVE MODE");
        if(jdbc.queryForObject("SELECT COUNT(*) FROM app_users",Long.class)>0) return;
        String password=env.getProperty("LEDGERFLOW_ADMIN_PASSWORD","");
        if(password.isBlank() || password.length()<16 || password.getBytes(java.nio.charset.StandardCharsets.UTF_8).length>72) throw new IllegalStateException("Set LEDGERFLOW_ADMIN_PASSWORD (16..72 bytes) for first startup");
        jdbc.update("INSERT INTO app_users(id,username,password_hash,role) VALUES(?,?,?,'ADMIN')",UUID.randomUUID(),"admin",passwords.encode(password));
    }
}
