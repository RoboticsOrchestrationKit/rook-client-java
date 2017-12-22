package run.rook.client;

import java.io.IOException;

import org.agrona.DirectBuffer;

public interface RookClient {
	void shutdown();
	
	void registerInputListener(InputListener inputListener);
	void deregisterInputListener(InputListener inputListener);
	
	void registerOutputListener(OutputListener outputListener);
	void deregisterOutputListener(OutputListener outputListener);
	
	void publishInput(String name, DirectBuffer value, int valueLength, DataType dataType) throws IOException;
	void publishOutput(String name, DirectBuffer value, int valueLength, DataType dataType) throws IOException;
	
	void publishInput(String name, byte[] value, DataType dataType) throws IOException;
	void publishOutput(String name, byte[] value, DataType dataType) throws IOException;
	
	default void publishInput(String name, DirectBuffer value, DataType dataType) throws IOException {
		publishInput(name, value, value.capacity(), dataType);
	}
	default void publishOutput(String name, DirectBuffer value, DataType dataType) throws IOException {
		publishOutput(name, value, value.capacity(), dataType);
	}
}
