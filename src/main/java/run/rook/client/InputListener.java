package run.rook.client;

import org.agrona.DirectBuffer;

public interface InputListener {
	void onInput(String name, DirectBuffer value, int valueLength, DataType dataType);
}
