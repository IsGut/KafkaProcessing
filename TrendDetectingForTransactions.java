/*
 *
 * Process detecting trend pattern 
 * 
*/

package stepProcessing_DetectingTrends;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;

import avroDetectingTrends.DetectingTrendsForTransaction;
import avroTransactionRaw.Transaction;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class TrendDetectingForTransactions {
	
	// The calculation example for ADX below is based on a 14-period indicator setting, as recommended by Welles Wilder. 
	public static int numberOfWindowsForADX = 14;

	public static void main(String[] args) {

		// receive stream data from
		String topicNameTransactionsAvro = "RawTransactionsAvro";

		// Write processed stream to
		String topicNameTrendDetectingForTransactions = "TrendDetectingForTransactions";
		String topicNameTrendDetectingTrendAD = "chartTrendDetectingTrendAD";
		String topicNameTrendDetectingTrendRSI = "chartTrendDetectingTrendRSI";
		String topicNameTrendDetectingTrendADX = "chartTrendDetectingTrendADX";

		Properties properties = new Properties();

		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "TrendDetecting");
		properties.put(StreamsConfig.CLIENT_ID_CONFIG, "TrendDetectingClient");

		// Where to find the Confluent schema registry instance(s)
		properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
		// Specify default (de)serializers for record keys and for record values.
		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

		// When you want to override serdes explicitly/selectively. Necessary to set the schema registry url.
		final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", "http://localhost:8081");

		final Serde<DetectingTrendsForTransaction> valueSpecificAvroSerde = new SpecificAvroSerde<>();
		valueSpecificAvroSerde.configure(serdeConfig, false);

		// Records should be flushed every 1000ms. This is less than the default of 30seconds.
		properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

		// // Necessary SerDe to produce windowed data
		StringSerializer stringSerializer = new StringSerializer();
		StringDeserializer stringDeserializer = new StringDeserializer();
		WindowedSerializer<String> windowedStringSerializer = new WindowedSerializer<>(stringSerializer);
		WindowedDeserializer<String> windowedStringDeserializer = new WindowedDeserializer<>(stringDeserializer);
		Serde<Windowed<String>> windowedStringSerde = Serdes.serdeFrom(windowedStringSerializer, windowedStringDeserializer);

		final StreamsBuilder builder = new StreamsBuilder();

		// Set time window in ms
		long timewindowInMs = 10000;

		// build stream for topicname
		KStream<String, Transaction> rawTransactions = builder.stream(topicNameTransactionsAvro);

		// Takes one record and produces one record or more records, while retaining the key of the original record.
		// Storage necessary value of transactions for detecting trends
		KStream<String, DetectingTrendsForTransaction> streamInDetectingTrendsTransactions = rawTransactions
				.mapValues(value -> new DetectingTrendsForTransaction(0.0, value.getX().getTotalInput(), 0.0, 0.0, 0.0, Double.MAX_VALUE,
						0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0L));

		// Windowed aggregation. Combines the values of records, per window, by the grouped key.
		// The current record value is combined with the last reduced value, and a new reduced value is returned.
		// Records with null key or value are ignored. The result value type cannot be changed, unlike aggregate.
		KTable<Windowed<String>, DetectingTrendsForTransaction> detectingTrendsTransactionsKTable = streamInDetectingTrendsTransactions
				.selectKey((key, value) -> "keyForDetectingTrends").groupByKey()
				.windowedBy(TimeWindows.of(timewindowInMs))
				.aggregate(() -> new DetectingTrendsForTransaction(0.0, 0.0, 0.0, 0.0, 0.0, Double.MAX_VALUE,	// Initializer
						0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0L), // Initializer
						(aggKey, newValue, aggregate) -> new DetectingTrendsForTransaction(aggregate, newValue)); // Aggregate

		// WindowedKTable to stream
		KStream<Windowed<String>, DetectingTrendsForTransaction> streamOutDetectingTrendsTransactions = detectingTrendsTransactionsKTable
				.toStream();
		
		// map for charts
		KStream<Windowed<String>, String> streamADforChart = streamOutDetectingTrendsTransactions.mapValues(
				value -> String.valueOf(value.getTrendAdvcancedDeclineLine()));
		KStream<Windowed<String>, String> streamRSIforChart = streamOutDetectingTrendsTransactions.mapValues(
				value -> String.valueOf(value.getTrendRelativeStrengthIndex()));
		KStream<Windowed<String>, String> streamADXforChart = streamOutDetectingTrendsTransactions.mapValues(
				value -> String.valueOf(value.getTrendAverageDirectionalMovementIndex()));

		// Produce stream to topic with windowedStringSerde for key
		streamOutDetectingTrendsTransactions.to(topicNameTrendDetectingForTransactions, Produced.with(windowedStringSerde, valueSpecificAvroSerde));
		streamADforChart.to(topicNameTrendDetectingTrendAD, Produced.with(windowedStringSerde, Serdes.String()));
		streamRSIforChart.to(topicNameTrendDetectingTrendRSI, Produced.with(windowedStringSerde, Serdes.String()));
		streamADXforChart.to(topicNameTrendDetectingTrendADX, Produced.with(windowedStringSerde, Serdes.String()));

		KafkaStreams streams = new KafkaStreams(builder.build(), properties);
		streams.cleanUp();
		streams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

	}

}
