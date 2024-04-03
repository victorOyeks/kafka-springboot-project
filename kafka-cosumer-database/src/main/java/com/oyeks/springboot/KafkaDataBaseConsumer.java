package com.oyeks.springboot;

import com.oyeks.springboot.entity.WikimediaData;
import com.oyeks.springboot.repository.WikimediaDataRepository;
import org.apache.kafka.common.protocol.types.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaDataBaseConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDataBaseConsumer.class);

    private WikimediaDataRepository dataRepository;

    public KafkaDataBaseConsumer(WikimediaDataRepository dataRepository){
        this.dataRepository=dataRepository;
    }

    @KafkaListener(topics = "wikimedia_recentchange", groupId = "myGroup")
    public void consume(String eventMessage) {

        LOGGER.info(String.format("Event Message received -> %s", eventMessage));

        WikimediaData wikimediaData = new WikimediaData();
        wikimediaData.setWikiEventData(eventMessage);

        dataRepository.save(wikimediaData);
    }
}
