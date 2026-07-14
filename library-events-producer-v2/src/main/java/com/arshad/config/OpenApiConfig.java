package com.arshad.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI libraryEventsOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Library Events Producer API")
                        .version("1.0.0")
                        .description("API for publishing library events to Kafka.")
                        .contact(new Contact().name("Library Events Team"))
                        .license(new License().name("Proprietary")));
    }
}
