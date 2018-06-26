/*
 *
 * Process route and alerts by thresholds pattern 
 * 
*/

package xstep_ProcessingFilterRoutingSplit;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import avroTransactionRaw.Transaction;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class RouteTransactionsBySize {

	@SuppressWarnings("resource")
	public static void main(String[] args) {

		// receive stream data from
		String topicNameTransactionsAvro = "RawTransactionsAvro";

		// produce processed stream data to branches 0-5
		String topicNameTransactionsTotalInputCat1 = "TransactionsTotalInputCategory1"; // value < 1
		String topicNameTransactionsTotalInputCat2 = "TransactionsTotalInputCategory2"; // value >= 1 && value < 10
		String topicNameTransactionsTotalInputCat3 = "TransactionsTotalInputCategory3"; // value >= 10 && value < 50
		String topicNameTransactionsTotalInputCat4 = "TransactionsTotalInputCategory4"; // value >= 50 && value < 100
		String topicNameTransactionsTotalInputCat5 = "TransactionsTotalInputCategory5"; // value >= 100 && value < 1000
		String topicNameTransactionsTotalInputCat6 = "TransactionsTotalInputCategory6"; // value >= 1000

		// Write Alerts to file:
		String pathHigh = "src/main/java/xstep_ProcessingFilterRoutingSplit/AlertsDueToSplitHigh.txt";
		String pathVeryHigh = "src/main/java/xstep_ProcessingFilterRoutingSplit/AlertsDueToSplitVeryHigh.txt";

		Properties properties = new Properties();

		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "SplitTransactions");
		properties.put(StreamsConfig.CLIENT_ID_CONFIG, "SplitTransactionsClient");

		// Where to find the Confluent schema registry instance(s)
		properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
		// Specify default (de)serializers for record keys and for record values.
		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

		final StreamsBuilder builder = new StreamsBuilder();

		// build stream for topicname
		KStream<String, Transaction> rawTransactions = builder.stream(topicNameTransactionsAvro);

		@SuppressWarnings("unchecked")
		// Branch (route or split) a KStream based on the supplied predicates into one or more KStream instances.
		KStream<String, Transaction>[] branches = rawTransactions.branch(
				(key, value) -> value.getX().getTotalInput() < 1, // branches[0]
				(key, value) -> value.getX().getTotalInput() >= 1 && value.getX().getTotalInput() < 10, // branches[1]
				(key, value) -> value.getX().getTotalInput() >= 10 && value.getX().getTotalInput() < 50, // branches[2]
				(key, value) -> value.getX().getTotalInput() >= 50 && value.getX().getTotalInput() < 100, // branches[3]
				(key, value) -> value.getX().getTotalInput() >= 100 && value.getX().getTotalInput() < 1000, // branches[4]
				(key, value) -> value.getX().getTotalInput() >= 1000 // branches[5]
		);

		// Peek: Performs a stateless action on each record, and returns an unchanged stream.
		branches[4].peek((key, value) -> {
			System.out
			.println("1) ALERT_high: transaction with hash: " + key + "\n100 <= Value: " + value + " < 1000 btc\n");
			
			// Write to text-file
			try {
				new PrintWriter(new FileWriter(pathHigh, true))
						.printf(//"%s" + "%n",
								"ALERT_high: transaction with hash: " + key + "\n100 <= Value: " + value + " < 1000 btc\n")
						.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
		branches[5].peek((key, value) -> {
			System.out
			.println("2) ALERT_very_high: transaction with hash: " + key + "\n1000 <= Value: " + value + " btc\n");
			
			// Write to text-file
			try {
				new PrintWriter(new FileWriter(pathVeryHigh, true))
						.printf(//"%s" + "%n",
								"ALERT_very_high: transaction with hash: " + key + "\n1000 <= Value: " + value + " btc\n")
						.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});

		// produce streams by size to topics
		branches[0].to(topicNameTransactionsTotalInputCat1);
		branches[1].to(topicNameTransactionsTotalInputCat2);
		branches[2].to(topicNameTransactionsTotalInputCat3);
		branches[3].to(topicNameTransactionsTotalInputCat4);
		branches[4].to(topicNameTransactionsTotalInputCat5);
		branches[5].to(topicNameTransactionsTotalInputCat6);

		KafkaStreams streams = new KafkaStreams(builder.build(), properties);
		streams.cleanUp();
		streams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

	}

}
