package com.epam.simplekafka.engine;

import com.epam.simplekafka.models.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class Consumer {
    private final Logger logger = LoggerFactory.getLogger(Consumer.class);

    @KafkaListener(topics = "users", groupId = "group_id")
    public void consume(@Payload User user,
                        @Headers MessageHeaders headers) throws IOException {
        logger.info("consumer got: " + user);
        headers.keySet().forEach(key -> {
            logger.info("{}: {}", key, headers.get(key));
        });
    }
}
