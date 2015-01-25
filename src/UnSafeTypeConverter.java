import java.lang.reflect.Field;

import sun.misc.Unsafe;


@SuppressWarnings("restriction")
class UnSafeTypeConverter {
	protected static final Unsafe unsafe;
	protected static long baseAddress; 
	protected static final int bytes = 8;
	static final long zero = 0;
	
	static {
		try {
			Field field = Unsafe.class.getDeclaredField("theUnsafe");
			field.setAccessible(true);
			unsafe = (Unsafe) field.get(null);

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public UnSafeTypeConverter() {
		baseAddress = unsafe.allocateMemory(bytes);
		unsafe.setMemory(baseAddress, bytes, (byte) 0);
	}
	
	public int IntFromLong(long value) {
		unsafe.putLong(baseAddress, value);
		return unsafe.getInt(baseAddress);
	}
	
	public long LongFromInt(int value) {
		unsafe.putInt(baseAddress, value);
		return unsafe.getLong(baseAddress);
	}

}
