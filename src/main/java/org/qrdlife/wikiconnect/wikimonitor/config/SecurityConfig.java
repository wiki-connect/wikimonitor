package org.qrdlife.wikiconnect.wikimonitor.config;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
@EnableWebSecurity
@RequiredArgsConstructor
public class SecurityConfig {

        @Bean
        public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
                http
                                .authorizeHttpRequests(auth -> auth
                                                .requestMatchers(
                                                                "/js/**",
                                                                "/css/**",
                                                                "/images/**",
                                                                "/lib/**",
                                                                "/login",
                                                                "/oauth2/callback",
                                                                "/auth/wikimedia",
                                                                "/access-denied",
                                                                "/actuator/**")
                                                .permitAll()
                                                .requestMatchers("/admin/**").hasRole("ADMIN")
                                                .requestMatchers("/").hasRole("USER")
                                                .anyRequest().hasRole("USER"))
                                .exceptionHandling(ex -> ex
                                                .accessDeniedHandler((request, response, accessDeniedException) -> {
                                                        if (request.getRequestURI().startsWith("/api/")) {
                                                                response.setStatus(
                                                                                jakarta.servlet.http.HttpServletResponse.SC_FORBIDDEN);
                                                                response.setContentType("application/json");
                                                                response.getWriter().write(
                                                                                "{\"error\": \"Forbidden\", \"message\": \""
                                                                                                + accessDeniedException
                                                                                                                .getMessage()
                                                                                                + "\"}");
                                                        } else {
                                                                response.setStatus(
                                                                                jakarta.servlet.http.HttpServletResponse.SC_FORBIDDEN);
                                                                request.getRequestDispatcher("/access-denied").forward(
                                                                                request,
                                                                                response);
                                                        }
                                                }))

                                .formLogin(form -> form
                                                .loginPage("/login")
                                                .defaultSuccessUrl("/", true)
                                                .permitAll())

                                .logout(logout -> logout
                                                .logoutUrl("/logout")
                                                .logoutSuccessUrl("/login")
                                                .invalidateHttpSession(true)
                                                .deleteCookies("JSESSIONID", "refresh_token")
                                                .permitAll())

                                .headers(headers -> headers
                                                .contentSecurityPolicy(csp -> csp
                                                                .policyDirectives(
                                                                                "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline' https://www.mediawiki.org; img-src 'self' data:; font-src 'self' data:; connect-src 'self'; frame-src 'self'; object-src 'none'; base-uri 'self'; form-action 'self'; frame-ancestors 'self';")))

                                .csrf(csrf -> csrf
                                                .csrfTokenRepository(
                                                                org.springframework.security.web.csrf.CookieCsrfTokenRepository
                                                                                .withHttpOnlyFalse())
                                                .csrfTokenRequestHandler(
                                                                new org.springframework.security.web.csrf.CsrfTokenRequestAttributeHandler()))
                                .addFilterAfter(new CsrfCookieFilter(),
                                                org.springframework.security.web.authentication.www.BasicAuthenticationFilter.class);

                return http.build();
        }

        private static class CsrfCookieFilter extends org.springframework.web.filter.OncePerRequestFilter {

                @Override
                protected void doFilterInternal(jakarta.servlet.http.HttpServletRequest request,
                                jakarta.servlet.http.HttpServletResponse response,
                                jakarta.servlet.FilterChain filterChain)
                                throws jakarta.servlet.ServletException, java.io.IOException {
                        org.springframework.security.web.csrf.CsrfToken csrfToken = (org.springframework.security.web.csrf.CsrfToken) request
                                        .getAttribute(org.springframework.security.web.csrf.CsrfToken.class.getName());
                        // Render the token value to a cookie by causing the deferred token to be loaded
                        if (csrfToken != null) {
                                csrfToken.getToken();
                        }
                        filterChain.doFilter(request, response);
                }
        }
}
