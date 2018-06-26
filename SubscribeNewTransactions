/*
 *
 * Receive incoming bitcoin transactions and store as String
 * 
*/

package step1_ReceiveData;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.neovisionaries.ws.client.WebSocket;
import com.neovisionaries.ws.client.WebSocketAdapter;
import com.neovisionaries.ws.client.WebSocketException;
import com.neovisionaries.ws.client.WebSocketExtension;
import com.neovisionaries.ws.client.WebSocketFactory;

public class SubscribeNewTransactions {
	/**
	 * The echo server on websocket.org.
	 */
	private static final String SERVER = "wss://ws.blockchain.info/inv";

	/**
	 * The timeout value in milliseconds for socket connection.
	 */
	private static final int TIMEOUT = 5000;

	
	// Initialize properties
	private static final Properties props = new Properties();
	private static Producer<String, String> producer;
	private static ProducerRecord<String, String> record;
	

	public static void main(String[] args) throws Exception {

		// Set properties for producer
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		// Instantiate producer
		producer = new KafkaProducer<>(props);

		// Connect to the echo server.
		WebSocket ws = connect();

		// The standard input via BufferedReader.
		BufferedReader in = getInput();

		// Initialize to subscribe to notifications for all new bitcoin transactions
		String text = "{\"op\":\"unconfirmed_sub\"}";

		// Subscribe to notifications for all new bitcoin transactions.
		ws.sendText(text);

		while ((text = in.readLine()) != null) {
			// If the input string is "exit".
			if (text.equals("exit")) {
				// Close producer and finish this application.
				producer.close();
				break;
			}

			// Send the text to the server.
			ws.sendText(text);
		}

		// Close the WebSocket.
		ws.disconnect();
	}

	/**
	 * Connect to the server.
	 */
	private static WebSocket connect() throws IOException, WebSocketException {

		return new WebSocketFactory().setConnectionTimeout(TIMEOUT).createSocket(SERVER)
				.addListener(new WebSocketAdapter() {

					// topicname
					String topicNameRawTransactions = "NewTransactionsRaw";

					// A text message arrived from the server.
					public void onTextMessage(WebSocket websocket, String message) {
						
						// Produce record. 1. param topicname. 2. param value
						record = new ProducerRecord<String, String>(topicNameRawTransactions, message);
						producer.send(record);
					}
				}).addExtension(WebSocketExtension.PERMESSAGE_DEFLATE).connect();
	}

	/**
	 * Wrap the standard input with BufferedReader.
	 */
	private static BufferedReader getInput() throws IOException {
		return new BufferedReader(new InputStreamReader(System.in));
	}
}
