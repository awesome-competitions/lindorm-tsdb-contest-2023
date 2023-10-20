package net.magik6k.bitbuffer;

public class ArrayBitBuffer extends SimpleBitBuffer{
	private final byte[] bytes;
	
	public ArrayBitBuffer(long bits) {
		bytes = new byte[(int) Math.ceil(bits / 8.d)];
		limit = bits;
		capacity = bits;
	}

	@Override
	protected byte rawGet(long index) {
		return bytes[(int) index];
	}

	@Override
	protected void rawSet(long index, byte value) {
		bytes[(int) index] = value;
	}

	@Override
	protected long rawLength() {
		return bytes.length;
	}

}
