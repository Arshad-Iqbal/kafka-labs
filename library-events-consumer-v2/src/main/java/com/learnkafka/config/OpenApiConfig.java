package com.learnkafka.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI libraryEventsOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Library Events Consumer API")
                        .description("REST API for managing library books consumed from Kafka events")
                        .version("v1")
                        .contact(new Contact()
                                .name("Library Events Team")
                                .email("library-events@learnkafka.com")));
    }
}
