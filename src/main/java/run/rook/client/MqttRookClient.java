package run.rook.client;

import java.io.IOException;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import run.rook.client.util.DataTypeUtil;

public class MqttRookClient implements RookClient {

	private static final long RECONNECT_TIMEOUT = 500;
	private static final String TOPIC_INPUT_PREFIX = "rook/io/i/";
	private static final String TOPIC_OUTPUT_PREFIX = "rook/io/o/";
	private static final String TOPIC_SEPARATOR = "/";
	private static final AtomicLong NEXT_INSTANCE = new AtomicLong();
	
	private final Set<InputListener> inputListeners = Collections.synchronizedSet(new LinkedHashSet<>());
	private final Set<OutputListener> outputListeners = Collections.synchronizedSet(new LinkedHashSet<>());
	
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final Pattern topicPatterm = Pattern.compile("rook/io/([io])/(.+)/(.+)");
	private final AtomicReference<MqttClient> clientRef = new AtomicReference<>(null);
	private final String mqttUrl;
	private final String mqttClientId;
	private volatile boolean run = true;
	
	public MqttRookClient(String mqttBrokerHost, int mqttBrokerPort, String mqttClientId) throws MqttException {
		this.mqttUrl = "tcp://" + mqttBrokerHost + ":" + mqttBrokerPort;
		this.mqttClientId = mqttClientId;
		new Thread(this::receiveLoop, getClass().getSimpleName()+"-"+NEXT_INSTANCE.incrementAndGet()).start();
	}
	
	@Override
	public void shutdown() {
		run = false;
		MqttClient client = clientRef.getAndSet(null);
		if(client != null) {
			try {
				client.close();
			} catch (Throwable t) {
				logger.debug("Exception during MqttClient#close()", t);
			}
		}
	}
	
	public void receiveLoop() {
		final Object reconnectNotifier = new Object();
		MqttClient client = null;
		boolean inErrorState = false;
		try {
			logger.info("Connecting...");
			while (run) {
				try {
					client = new MqttClient(mqttUrl, mqttClientId);
					client.connect();
					client.setCallback(new MqttCallback() {
						@Override
						public void messageArrived(String topic, MqttMessage message) throws Exception {
							Matcher m = topicPatterm.matcher(topic);
							if (m.matches()) {
								switch (m.group(1)) {
								case "i":
									dispatchInput(m.group(2), m.group(3), message.getPayload());
									break;
								case "o":
									dispatchOutput(m.group(2), m.group(3), message.getPayload());
									break;
								}
							}
						}

						@Override
						public void deliveryComplete(IMqttDeliveryToken token) {
							// QoS=0
						}

						@Override
						public void connectionLost(Throwable t) {
							logger.error("Lost MQTT Connection", t);
							synchronized (reconnectNotifier) {
								reconnectNotifier.notifyAll();
							}
						}
					});
					client.subscribe("rook/io/i/+/+");
					client.subscribe("rook/io/o/+/+");
					clientRef.set(client);
					
					// successfully connected (no error thrown to this point)
					logger.info("Connected");
					inErrorState = false;

					// wait for reconnect notification
					synchronized (reconnectNotifier) {
						while (client.isConnected()) {
							reconnectNotifier.wait();
						}
					}
					
					logger.info("Reconnecting...");
					clientRef.set(null);
				} catch (MqttException e) {
					// only log error once until a proper connection can be
					// established
					if (!inErrorState) {
						logger.error("Could not connect to MQTT URL '" + mqttUrl + "' - Will continuously retry every "
								+ RECONNECT_TIMEOUT + " milliseconds...", e);
						inErrorState = true;
					}
					// throttle reconnecting
					Thread.sleep(RECONNECT_TIMEOUT);
				}
			}
		} catch (InterruptedException e) {
			logger.error("Interrupted. Exiting...");
		}

		// attempt to close client when exiting thread
		if (client != null) {
			try {
				client.close();
			} catch (MqttException e) {

			}
		}
	}
	
	private void dispatchInput(String name, String dataTypeStr, byte[] payload) {
		synchronized (inputListeners) {
			DataType dataType = DataType.valueOfOrDefault(dataTypeStr, DataType.BLOB);
			DirectBuffer value = new UnsafeBuffer(payload);
			for(InputListener inputListener : inputListeners) {
				inputListener.onInput(name, value, payload.length, dataType);
			}
		}
	}
	
	private void dispatchOutput(String name, String dataTypeStr, byte[] payload) {
		synchronized (outputListeners) {
			DataType dataType = DataType.valueOfOrDefault(dataTypeStr, DataType.BLOB);
			DirectBuffer value = new UnsafeBuffer(payload);
			for(OutputListener outputListener : outputListeners) {
				outputListener.onOutput(name, value, payload.length, dataType);
			}
		}
	}

	@Override
	public void registerInputListener(InputListener inputListener) {
		inputListeners.add(inputListener);
	}

	@Override
	public void deregisterInputListener(InputListener inputListener) {
		inputListeners.remove(inputListener);
	}

	@Override
	public void registerOutputListener(OutputListener outputListener) {
		outputListeners.add(outputListener);
	}

	@Override
	public void deregisterOutputListener(OutputListener outputListener) {
		outputListeners.remove(outputListener);
	}

	@Override
	public void publishInput(String name, DirectBuffer value, int valueLength, DataType dataType) throws IOException {
		publishInput(name, DataTypeUtil.toBytes(value, valueLength), dataType);
	}

	@Override
	public void publishOutput(String name, DirectBuffer value, int valueLength, DataType dataType) throws IOException {
		publishOutput(name, DataTypeUtil.toBytes(value, valueLength), dataType);
	}

	@Override
	public void publishInput(String name, byte[] value, DataType dataType) throws IOException {
		send(TOPIC_INPUT_PREFIX + name + TOPIC_SEPARATOR + dataType, value);
	}

	@Override
	public void publishOutput(String name, byte[] value, DataType dataType) throws IOException {
		send(TOPIC_OUTPUT_PREFIX + name + TOPIC_SEPARATOR + dataType, value);
	}
	

	private void send(String topic, byte[] payload) throws IOException {
		MqttClient client = clientRef.get();
		if(client != null) {
			if(logger.isDebugEnabled()) {
				logger.debug("Sending: topic=" + topic + " payload=" + Base64.getEncoder().encodeToString(payload));
			}
			MqttMessage message = new MqttMessage();
			message.setQos(0);
			message.setPayload(payload);
			try {
				client.publish(topic, message);
			} catch (Exception e) {
				logger.error("Could not send MQTT Message", e);
			}
		} else {
			throw new IOException("Not connected to MQTT Broker");
		}
	}
}
