package run.rook.client.util;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import run.rook.client.DataType;

public class TestDataTypeUtil {

	public static void main(String[] args) {
		byte[] buf = new byte[] { -3 };
		DirectBuffer value = new UnsafeBuffer(buf);
		System.out.println(DataTypeUtil.toBigInteger(value, buf.length, DataType.I8));
	}
}
