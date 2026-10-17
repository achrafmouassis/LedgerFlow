package com.ledgerflow.security;

import jakarta.servlet.*;
import jakarta.servlet.http.*;
import java.io.IOException;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.security.core.context.SecurityContextHolder;

final class BearerFilter extends OncePerRequestFilter {
    private final SessionTokens tokens;
    BearerFilter(SessionTokens tokens) { this.tokens = tokens; }
    @Override protected void doFilterInternal(HttpServletRequest request,HttpServletResponse response,FilterChain chain) throws ServletException,IOException {
        String header=request.getHeader("Authorization");
        if(header != null && header.startsWith("Bearer ")) {
            try { SecurityContextHolder.getContext().setAuthentication(tokens.authenticate(header.substring(7))); }
            catch(org.springframework.dao.DataAccessException unavailable) {
                response.setStatus(503); response.setContentType("application/json");
                response.getWriter().write("{\"code\":\"AUTH_STORE_UNAVAILABLE\"}"); return;
            }
        }
        response.setHeader("Cache-Control","no-store");
        chain.doFilter(request,response);
    }
}
