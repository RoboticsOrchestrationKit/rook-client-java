package run.rook.client;

public enum DataType {
	BLOB(0), UTF8(0), U8(1), U16(2), U32(4), U64(8), I8(1), I16(2), I32(4), I64(8);
	private final int size;
	private DataType(int size) {
		this.size = size;
	}
	public int getSize() {
		return size;
	}
	public static DataType valueOfOrDefault(String dataType, DataType defaultValue) {
		for(DataType d : values()) {
			if(d.toString().equals(dataType)) {
				return d;
			}
		}
		return defaultValue;
	}
}
