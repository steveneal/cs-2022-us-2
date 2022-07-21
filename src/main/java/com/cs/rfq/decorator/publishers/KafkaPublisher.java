package com.cs.rfq.decorator.publishers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaPublisher {

    private final static String TOPIC = "rfq-return";
    private final static String BOOTSTRAP_SERVERS =
            "localhost:9092";

    private static Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    static void runProducer(final String message) throws Exception {
        final Producer<Long, String> producer = createProducer();
        long time = System.currentTimeMillis();

        try {
            final ProducerRecord<Long, String> record =
                    new ProducerRecord<>(TOPIC, 1l,message
                            );

            RecordMetadata metadata = producer.send(record).get();
        } finally {
            producer.flush();
            producer.close();
        }
    }
}
