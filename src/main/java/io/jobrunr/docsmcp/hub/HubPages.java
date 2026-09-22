package io.jobrunr.docsmcp.hub;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.CacheControl;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

import static org.springframework.web.reactive.function.server.RequestPredicates.GET;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;

/**
 * Clean urls for the hub pages in {@code static/}.
 */
@Configuration
public class HubPages {

    @Bean
    public RouterFunction<ServerResponse> hubPageRoutes() {
        return route(GET("/"), request -> page("signup.html"))
                .andRoute(GET("/signup"), request -> page("signup.html"))
                .andRoute(GET("/verify"), request -> page("verify.html"))
                .andRoute(GET("/account"), request -> page("account.html"));
    }

    private static reactor.core.publisher.Mono<ServerResponse> page(String name) {
        return ServerResponse.ok()
                .contentType(MediaType.TEXT_HTML)
                .cacheControl(CacheControl.noCache())
                .bodyValue(new ClassPathResource("static/" + name));
    }
}
