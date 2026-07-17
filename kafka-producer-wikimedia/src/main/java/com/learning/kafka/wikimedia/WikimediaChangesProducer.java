package com.learning.kafka.wikimedia;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventSource;

import okhttp3.Headers;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    private static final String URL = "https://stream.wikimedia.org/v2/stream/recentchange";
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String TOPIC = "demo_java";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(32 * 1024));

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        WikimediaChangeHandler wikimediaChangeHandler = new WikimediaChangeHandler(producer, TOPIC);
        BackgroundEventSource.Builder builder = new BackgroundEventSource.Builder(
                wikimediaChangeHandler,
                new EventSource.Builder(
                        ConnectStrategy.http(URI.create(URL))
                                .headers(Headers.of("User-Agent",
                                        "kafka-wikimedia-changes/1.0 (https://github.com/user/kafka-wikimedia-changes; user@example.com)"))));
        BackgroundEventSource backgroundEventSource = builder.build();

        backgroundEventSource.start();

        try {
            TimeUnit.MINUTES.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            producer.close();
        }
    }
}
