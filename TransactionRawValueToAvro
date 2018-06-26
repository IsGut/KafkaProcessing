/*
 *
 * Serialize String to Avro
 * 
*/

package step2_TransformRawValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.json.JSONObject;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

// Based on https://blockchain.info/api/api_websocket 
import avroTransactionRaw.Inputs;
import avroTransactionRaw.Out;
import avroTransactionRaw.Prev_out;
import avroTransactionRaw.Transaction;
import avroTransactionRaw.X;

// Transform transaction value from String to Avro
public class TransactionRawValueToAvro {

	@SuppressWarnings({ "resource" })
	public static void main(String[] args) throws Exception {

		// For Consumer: consume topic NewTransactionsRaw
		String topicNameRawTransactions = "NewTransactionsRaw";

		// Set properties for consumer
		Properties propertiesConsumer = new Properties();
		propertiesConsumer.put("bootstrap.servers", "localhost:9092");
		propertiesConsumer.put("group.id", "TransactionRawValueToAvroGroup");
		propertiesConsumer.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		propertiesConsumer.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		propertiesConsumer.put("enable.auto.commit", "true");

		// Instantiate consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(propertiesConsumer);
		consumer.subscribe(Arrays.asList(topicNameRawTransactions));

		// For Producer: produce to topic RawTransactionsAvro
		String topicNameTransactionsAvro = "RawTransactionsAvro";
		
		// Init producer and record. Sending key is String, value is Transaction
		Producer<String, Transaction> producer;
		ProducerRecord<String, Transaction> produceRecord;
		
		// Set properties for producer
		Properties propertiesProducer = new Properties();
		propertiesProducer.put("bootstrap.servers", "localhost:9092");
		propertiesProducer.put("acks", "1");
		propertiesProducer.put("retries", "10");
		propertiesProducer.put("key.serializer", StringSerializer.class.getName());
		propertiesProducer.put("value.serializer", KafkaAvroSerializer.class.getName());
		propertiesProducer.put("schema.registry.url", "http://127.0.0.1:8081");
		
		producer = new KafkaProducer<>(propertiesProducer);

		// Poll messages, serialize and storage
		while (true) {
			ConsumerRecords<String, String> consumeRecords = consumer.poll(100);
			for (ConsumerRecord<String, String> consumeRecord : consumeRecords) {
				String messageIn = consumeRecord.value();

				// Init jsonObject with incoming message
				JSONObject jsonObj = new JSONObject(messageIn);
				JSONObject jsonObjx = jsonObj.getJSONObject("x");
				JSONArray valueInJASON = jsonObjx.getJSONArray("inputs");
				JSONArray valueOutJASON = jsonObjx.getJSONArray("out");

				// Set variables for serialisation
				String op = jsonObj.getString("op");
				long locktime = jsonObjx.getLong("lock_time");
				long ver = jsonObjx.getLong("ver");
				long size = jsonObjx.getLong("size");
				long time = jsonObjx.getLong("time");
				long txindex = jsonObjx.getLong("tx_index");
				long vinsz = jsonObjx.getLong("vin_sz");
				String hash = jsonObjx.getString("hash");
				long voutsz = jsonObjx.getLong("vout_sz");
				String relayedby = jsonObjx.getString("relayed_by");

				double totalInput = 0;
				double totalOutput = 0;
				double fees = 0;

				// Because array: inputs[]
				ArrayList<Inputs> inputsList = new ArrayList<Inputs>();
				// loop here for inputs depending on vinsz
				for (int i = 0; i < vinsz; i++) {

					// fetch data for prev_out
					long sequence = valueInJASON.getJSONObject(i).getLong("sequence");
					boolean spentprevout = valueInJASON.getJSONObject(i).getJSONObject("prev_out").getBoolean("spent");
					long txindexprevout = valueInJASON.getJSONObject(i).getJSONObject("prev_out").getLong("tx_index");
					long typeprevout = valueInJASON.getJSONObject(i).getJSONObject("prev_out").getLong("type");
					String addrprevout = valueInJASON.getJSONObject(i).getJSONObject("prev_out").getString("addr");
					double valueprevout = valueInJASON.getJSONObject(i).getJSONObject("prev_out").getDouble("value");
					valueprevout = valueprevout / 100000000; // satoshi to btc
					// add totalInput
					totalInput += valueprevout;

					long nprevout = valueInJASON.getJSONObject(i).getJSONObject("prev_out").getLong("n");
					String scriptprevout = valueInJASON.getJSONObject(i).getJSONObject("prev_out").getString("script");
					String scriptinputs = valueInJASON.getJSONObject(i).getString("script");

					// Instatiate Prev_out
					Prev_out prevout = Prev_out.newBuilder().setSpent(spentprevout).setTxIndex(txindexprevout)
							.setType(typeprevout).setAddr(addrprevout).setValue(valueprevout).setN(nprevout)
							.setScript(scriptprevout).build();

					// Instatiate Inputs
					Inputs inputs = Inputs.newBuilder().setSequence(sequence).setPrevOut(prevout)
							.setScript(scriptinputs).build();

					// add input to List
					inputsList.add(inputs);
				} // loop for inputs until here

				// Becasuse array: out[]
				ArrayList<Out> outList = new ArrayList<Out>();
				// loop here for out depending on voutsz
				for (int i = 0; i < voutsz; i++) {

					// fetch data for prev_out
					boolean spentout = valueOutJASON.getJSONObject(i).getBoolean("spent");
					long txindexout = valueOutJASON.getJSONObject(i).getLong("tx_index");
					long typeout = valueOutJASON.getJSONObject(i).getLong("type");
					double valueout = valueOutJASON.getJSONObject(i).getLong("value");
					String addrout = "null";
					
					// Sometimes there is no addr and would terminate the execution
					if (valueOutJASON.getJSONObject(i).get("addr") instanceof String) {
						addrout = valueOutJASON.getJSONObject(i).getString("addr");
					}
					// convert to btc
					valueout = valueout / 100000000;
					// add tot totalOutput
					totalOutput += valueout;

					long nout = valueOutJASON.getJSONObject(i).getLong("n");
					String scriptout = valueOutJASON.getJSONObject(i).getString("script");

					// Instatiate Out
					Out out = Out.newBuilder().setSpent(spentout).setTxIndex(txindexout).setType(typeout)
							.setAddr(addrout).setValue(valueout).setN(nout).setScript(scriptout).build();

					// add out to List
					outList.add(out);
				} // loop for out until here

				// calc fees
				fees = totalInput - totalOutput;

				// Instantiate X
				X x = X.newBuilder().setLockTime(locktime).setVer(ver).setSize(size).setInputs(inputsList).setTime(time)
						.setTxIndex(txindex).setVinSz(vinsz).setHash(hash).setVoutSz(voutsz).setRelayedBy(relayedby)
						.setTotalInput(totalInput).setTotalOutput(totalOutput).setFees(fees).setOut(outList).build();

				// Instantiate Transaction
				Transaction transaction = Transaction.newBuilder().setKey(hash).setOp(op).setX(x).build();
				
				// param1: topicname; param2: key; param3: value;
				produceRecord = new ProducerRecord<String, Transaction>(topicNameTransactionsAvro, hash, transaction);
				producer.send(produceRecord);
				producer.flush();
			}
		}
	}
}
