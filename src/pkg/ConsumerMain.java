package pkg;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerMain {

	public static void main(String[] args) throws UnsupportedEncodingException, InterruptedException {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "kafka:9092");
		properties.put("kafka.topic", "my-topic");
		properties.put("compression.type", "gzip");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("max.partition.fetch.bytes", "2097152");
		properties.put("max.poll.records", "500");
		properties.put("group.id", "my-group");

		runMainLoop(args, properties);
	}

	private static void runMainLoop(String[] args, Properties properties)
			throws InterruptedException, UnsupportedEncodingException {

		// Create Kafka producer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

		try {

			consumer.subscribe(Arrays.asList(properties.getProperty("kafka.topic")));

			System.out.println("Sottoscritto al topic " + properties.getProperty("kafka.topic"));
			System.out.println("Attesa del consumer...");

			while (true) {
				@SuppressWarnings("deprecation")
				ConsumerRecords<String, String> records = consumer.poll(100);

				for (ConsumerRecord<String, String> record : records) {
					System.out.printf("partition = %s, offset = %d, key = %s, value = [%s]\n", record.partition(),
							record.offset(), record.key(), record.value());
				}

			}
		}

		finally {
			consumer.close();
		}
	}
}
