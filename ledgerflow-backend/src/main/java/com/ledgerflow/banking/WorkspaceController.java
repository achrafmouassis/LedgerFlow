package com.ledgerflow.banking;

import java.util.*;
import java.math.BigDecimal;
import org.springframework.web.bind.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import com.ledgerflow.security.BankingAccess;
import jakarta.validation.constraints.*;

@RestController @RequestMapping("/api")
public class WorkspaceController {
    private final JdbcTemplate jdbc; private final BankingAccess access;
    public WorkspaceController(JdbcTemplate jdbc,BankingAccess access) { this.jdbc=jdbc;this.access=access; }
    public record AccountRow(UUID id,String owner,String currency,BigDecimal balance) {}
    @GetMapping("/accounts") public List<AccountRow> accounts(@RequestParam(defaultValue="0") @Min(0) int page,
        @RequestParam(defaultValue="20") @Min(1) @Max(100) int size) {
        String where=access.globalRead()?"":" WHERE EXISTS(SELECT 1 FROM account_access x JOIN app_users u ON u.id=x.user_id WHERE x.account_id=a.id AND u.username=?)";
        Object[] params=access.globalRead()?new Object[]{size,(long)page*size}:new Object[]{access.username(),size,(long)page*size};
        return jdbc.query("SELECT a.id,a.owner,a.currency,a.balance FROM accounts a"+where+" ORDER BY a.owner,a.id LIMIT ? OFFSET ?",
            (r,n)->new AccountRow(r.getObject(1,UUID.class),r.getString(2),r.getString(3),r.getBigDecimal(4)),params);
    }
    @GetMapping("/overview") public Map<String,Object> overview() {
        Map<String,Object> result=new LinkedHashMap<>();
        result.put("accounts",jdbc.queryForObject("SELECT COUNT(*) FROM accounts",Long.class));
        result.put("operations",jdbc.queryForObject("SELECT COUNT(*) FROM journals",Long.class));
        result.put("balances",jdbc.queryForList("SELECT currency,SUM(balance) AS total FROM accounts GROUP BY currency ORDER BY currency"));
        result.put("recent",jdbc.queryForList("SELECT id,type,amount,currency,created_at FROM journals ORDER BY created_at DESC,id DESC LIMIT 10"));
        result.put("instance",System.getenv().getOrDefault("HOSTNAME","local"));
        return result;
    }
}
