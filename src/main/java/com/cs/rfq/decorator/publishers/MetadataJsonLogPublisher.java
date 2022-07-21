package com.cs.rfq.decorator.publishers;

import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.google.gson.GsonBuilder;
import org.apache.spark.sql.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MetadataJsonLogPublisher implements MetadataPublisher {

    private static final Logger log = LoggerFactory.getLogger(MetadataJsonLogPublisher.class);

    @Override
    public void publishMetadata(Map<RfqMetadataFieldNames, Object> metadata) {
        String s = new GsonBuilder().setPrettyPrinting().create().toJson(metadata);

        try{
            KafkaPublisher.runProducer(String.format("Publishing metadata:%n%s", s));
            log.info(String.format("Publishing metadata:%n%s", s));
        } catch(Exception e){
            e.printStackTrace();
        }


    }
}
