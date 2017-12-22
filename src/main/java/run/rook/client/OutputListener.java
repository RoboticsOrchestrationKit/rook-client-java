package run.rook.client;

import org.agrona.DirectBuffer;

public interface OutputListener {
	void onOutput(String name, DirectBuffer value, int valueLength, DataType dataType);
}
