package com.ledgerflow.security;

import org.springframework.context.annotation.*;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

@Configuration
public class ApiSecurity {
    @Bean PasswordEncoder passwordEncoder() { return new BCryptPasswordEncoder(12); }
    @Bean org.springframework.security.core.userdetails.UserDetailsService unusedPasswordLogin() {
        // No default generated user or secondary password-authentication mechanism.
        return username -> { throw new org.springframework.security.core.userdetails.UsernameNotFoundException("Use /api/auth/login"); };
    }
    @Bean SecurityFilterChain api(HttpSecurity http, SessionTokens tokens) throws Exception {
        // Only explicit Bearer headers authenticate; no cookie/basic/form authentication.
        // Thus cross-origin forms cannot automatically attach authentication credentials.
        return http.csrf(c -> c.disable())
            .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .authorizeHttpRequests(a -> a
                .requestMatchers("/", "/index.html", "/app.js", "/app.css", "/favicon.ico", "/actuator/health", "/actuator/health/**", "/api/auth/login", "/error").permitAll()
                .requestMatchers("/api/admin/**").hasRole("ADMIN")
                .requestMatchers("/api/analytics/**", "/api/overview").hasAnyRole("ADMIN","OPERATOR","VIEWER")
                .anyRequest().authenticated())
            .exceptionHandling(e -> e
                .authenticationEntryPoint((r,s,x) -> { s.setStatus(401); s.setContentType("application/json"); s.getWriter().write("{\"code\":\"UNAUTHORIZED\"}"); })
                .accessDeniedHandler((r,s,x) -> { s.setStatus(403); s.setContentType("application/json"); s.getWriter().write("{\"code\":\"FORBIDDEN\"}"); }))
            .headers(h -> h.contentSecurityPolicy(c -> c.policyDirectives("default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")))
            .addFilterBefore(new BearerFilter(tokens), UsernamePasswordAuthenticationFilter.class).build();
    }
}
