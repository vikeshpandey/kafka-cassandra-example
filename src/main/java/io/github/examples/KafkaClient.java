package io.github.examples;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

public class KafkaClient {
    public static void main(String[] args) throws IOException {
        // set up house-keeping
        ObjectMapper mapper = new ObjectMapper();
        CassandraConsumer cassandraConsumer = new CassandraConsumer();
        cassandraConsumer.connect("localhost",9042);

        // and the consumer
        KafkaConsumer<String, String> consumer;
        try (InputStream props = Resources.getResource("consumer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            if (properties.getProperty("group.id") == null) {
                properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
            }
            consumer = new KafkaConsumer<>(properties);
        }
        consumer.subscribe(Arrays.asList("cricket-lovers"));
        int timeouts = 0;

        //noinspection InfiniteLoopStatement
        while (true) {
            // read records with a short timeout. If we time out, we don't really care.
            ConsumerRecords<String, String> records = consumer.poll(10);
            if (records.count() == 0) {
                timeouts++;
            } else {
                System.out.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
                timeouts = 0;
            }

            for (ConsumerRecord<String, String> record : records) {
                JsonNode msg = mapper.readTree(record.value());
                if (record.topic().equalsIgnoreCase("cricket-lovers")) {
                        System.out.println("Received cricket message" + msg.toString());
                        cassandraConsumer.insertData( msg.toString());
                }
            }
        }
    }
}
