package run.rook.client;

import org.agrona.DirectBuffer;

import run.rook.client.util.DataTypeUtil;

public class MqttRookClientExample {

	public static void main(String[] args) throws Exception {
		RookClient client = new MqttRookClient("localhost", 1883, "MqttRookClientExample");
		client.registerInputListener(MqttRookClientExample::println);
		client.registerOutputListener(MqttRookClientExample::println);
	}
	
	private static void println(String name, DirectBuffer value, int valueLength, DataType dataType) {
		System.out.println(name + ": " + DataTypeUtil.toString(value, valueLength, dataType));
	}
}
