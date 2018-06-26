/*
 *
 * Process join and data correlation pattern 
 * 
*/

package xstep_ProcessingJoinAndDataCorrelation;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import avroBlockRaw.BlockRaw;
import avroJoinTransactionWithBlockData.JoinTransactionWithBlockData;
import avroJoinTransactionWithBlockData.Xblock;
import avroJoinTransactionWithBlockData.Xtx;
import avroTransactionRaw.Transaction;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class JoinBlockWithTransactions {

	public static void main(String[] args) {

		// receive stream data from
		String topicNameRawTransactionsAvro = "RawTransactionsAvro";
		String topicNameRawBlocksAvro = "RawBlocksAvro";
		String topicNameBlockSplittedTxIds = "BlockSplittedTxIds";

		// produce processed stream data to
		String topicNameJoinedDataTxBlock = "JoinedDataTxBlock";

		Properties properties = new Properties();

		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "JoinedDataTxBlock");
		properties.put(StreamsConfig.CLIENT_ID_CONFIG, "JoinedDataTxBlockClient");

		// Where to find the Confluent schema registry instance(s)
		properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
		// Specify default (de)serializers for record keys and for record values.
		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

		// To override serdes explicitly/selectively it isecessary to set the schema registry url.
		final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
				"http://localhost:8081");
		
		final Serde<Transaction> valueSpecificAvroSerdeTransaction = new SpecificAvroSerde<>();
		valueSpecificAvroSerdeTransaction.configure(serdeConfig, false);

		final Serde<BlockRaw> valueSpecificAvroSerdeBlock = new SpecificAvroSerde<>();
		valueSpecificAvroSerdeBlock.configure(serdeConfig, false);

		final StreamsBuilder builder = new StreamsBuilder();

		// build stream for topicname
		final KStream<String, Transaction> transactionData = builder.stream(topicNameRawTransactionsAvro);
		final KStream<String, BlockRaw> blockData = builder.stream(topicNameRawBlocksAvro);
		final KStream<String, Long> blockDataJustTxIdAndHash = builder.stream(topicNameBlockSplittedTxIds,
				Consumed.with(Serdes.String(), Serdes.Long()));

		// set txid as key for transactions
		KStream<String, Transaction> transactionwithtxidaskey = transactionData
				.selectKey((key, value) -> String.valueOf(value.getX().getTxIndex()));

		// swap (txid as key and hash as value) from (hash as key and txid as value) due to joining operations
		KStream<String, String> blockwithtxidaskey = blockDataJustTxIdAndHash
				.map((key, value) -> KeyValue.pair(String.valueOf(value), key));

		// join transaction-data with block-hash and set block hash as key due to joining operations
		KStream<String, Transaction> txWithBlockHash = blockwithtxidaskey.join(transactionwithtxidaskey,
				(streamBlockValue, streamTxValue) -> new Transaction(streamBlockValue, streamTxValue.getOp(),
						streamTxValue.getX()), /* ValueJoiner */
				JoinWindows.of(TimeUnit.MINUTES.toMillis(5)), Joined.with(Serdes.String(), /* key */
						Serdes.String(), /* left value */
						valueSpecificAvroSerdeTransaction) /* right value */
		);
		KStream<String, Transaction> txWithBlockHashAsKey = txWithBlockHash
				.selectKey((key, value) -> String.valueOf(value.getKey()));

		
		// join block- and transaction-data
		KStream<String, JoinTransactionWithBlockData> txWithBlockCorrelation = txWithBlockHashAsKey.join(blockData,
				(transaction, block) -> new JoinTransactionWithBlockData(transaction.getKey(),
						"txWithBlockCorrelation", new Xtx(transaction.getX()), new Xblock(block.getXBlock())), /* ValueJoiner */
				JoinWindows.of(TimeUnit.MINUTES.toMillis(5)), Joined.with(Serdes.String(), /* key */
						valueSpecificAvroSerdeTransaction, /* left value */
						valueSpecificAvroSerdeBlock) /* right value */
		);

		// produce data to topic
		txWithBlockCorrelation.to(topicNameJoinedDataTxBlock);

		KafkaStreams streams = new KafkaStreams(builder.build(), properties);
		streams.cleanUp();
		streams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

}
