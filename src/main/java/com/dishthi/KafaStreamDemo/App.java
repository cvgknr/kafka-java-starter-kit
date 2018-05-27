package com.dishthi.KafaStreamDemo;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.TimeWindows;

public class App {

	public static void main(String[] args) {
		Properties settings = new Properties();

		// Set a few key parameters
		settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-demo-app");
		settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		settings.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

		// Any further settings

		// Create an instance of StreamsConfig from the Properties instance
		StreamsConfig config = new StreamsConfig(settings);

		StreamsBuilder builder = new StreamsBuilder();

		KStream<String, String> source = builder.stream("demo-input");

		KStream<String, String> countStream = source
											.groupByKey()
											.windowedBy(TimeWindows.of(5000))
											.count()
											.toStream()
											.map((windowId, value) -> new KeyValue<String, String>(windowId.key(), "" + value));
		countStream.print(Printed.toSysOut());
		countStream.to("demo-count");
		
		final KafkaStreams streams = new KafkaStreams(builder.build(), config);
		final CountDownLatch latch = new CountDownLatch(1);

		// attach shutdown handler to catch control-c
		Runtime.getRuntime().addShutdownHook(new Thread("streams-dishthi-shutdown-hook") {
			@Override
			public void run() {
				streams.close();
				latch.countDown();
			}
		});

		try {
			streams.start();
			latch.await();
		} catch (Throwable e) {
			System.exit(1);
		}
		System.exit(0);
	}

}
