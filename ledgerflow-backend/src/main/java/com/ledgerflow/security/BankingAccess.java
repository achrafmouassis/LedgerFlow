package com.ledgerflow.security;

import java.util.UUID;
import org.springframework.stereotype.Component;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.access.AccessDeniedException;

@Component
public class BankingAccess {
    private final JdbcTemplate jdbc;
    public BankingAccess(JdbcTemplate jdbc) { this.jdbc=jdbc; }
    public String username() { return SecurityContextHolder.getContext().getAuthentication().getName(); }
    public boolean role(String role) { return SecurityContextHolder.getContext().getAuthentication().getAuthorities().stream().anyMatch(a -> a.getAuthority().equals("ROLE_"+role)); }
    public boolean globalRead() { return role("ADMIN") || role("OPERATOR") || role("VIEWER"); }
    public void operator() { if(!role("ADMIN") && !role("OPERATOR")) throw new AccessDeniedException("Operator required"); }
    public void read(UUID id) {
        if(globalRead()) return;
        long count=jdbc.queryForObject("SELECT COUNT(*) FROM account_access a JOIN app_users u ON u.id=a.user_id WHERE u.username=? AND a.account_id=?",Long.class,username(),id);
        if(count==0) throw new AccessDeniedException("Account not authorized");
    }
    public void transfer(UUID source) {
        if(role("VIEWER")) throw new AccessDeniedException("Read only");
        read(source);
    }
}
