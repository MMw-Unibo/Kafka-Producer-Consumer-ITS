package pkg;

import java.io.UnsupportedEncodingException;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerMain {

	public static void main(String[] args) throws UnsupportedEncodingException, InterruptedException {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "kafka:9092");
		properties.put("kafka.topic", "my-topic");
		properties.put("acks", "0");
		properties.put("retries", "1");
		properties.put("batch.size", "20971520");
		properties.put("linger.ms", "33");
		properties.put("max.request.size", "2097152");
		properties.put("compression.type", "gzip");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		runMainLoop(args, properties);
	}

	private static void runMainLoop(String[] args, Properties properties)
			throws InterruptedException, UnsupportedEncodingException {
		String messaggio = "Questo è il mio messaggio ";
		String temp;
		int numero = 0;
		// Create Kafka producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		System.out.println("Avvio producer in corso...");
		try {
			while (true) {
				temp = messaggio + (numero++);
				String id = "device-" + getRandomNumberInRange(1, 10);
				producer.send(new ProducerRecord<String, String>(properties.getProperty("kafka.topic"), id, temp));
				System.out.println("Messaggio #" + numero + " mandato: " + temp);
				Thread.sleep(5000);
			}

		} finally {
			producer.close();
		}

	}

	private static int getRandomNumberInRange(int min, int max) {
		Random r = new Random();
		return r.ints(min, (max + 1)).findFirst().getAsInt();

	}
}
