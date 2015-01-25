import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;

import sun.misc.Unsafe;

@SuppressWarnings("restriction")
class UnsafeMemory {
	protected static final Unsafe unsafe;
	protected long baseAddress;
	protected long bytesBaseAddress;
	static {
		try {
			Field field = Unsafe.class.getDeclaredField("theUnsafe");
			field.setAccessible(true);
			unsafe = (Unsafe) field.get(null);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	protected static final long byteArrayOffset = unsafe
			.arrayBaseOffset(byte[].class);
	private static final long longArrayOffset = unsafe
			.arrayBaseOffset(long[].class);
	private static final long doubleArrayOffset = unsafe
			.arrayBaseOffset(double[].class);

	public static final int SIZE_OF_BOOLEAN = 1;
	public static final int SIZE_OF_INT = 4;
	public static final int SIZE_OF_LONG = 8;

	protected int pos = 0;
	protected final byte[] buffer;

	public UnsafeMemory(final byte[] buffer) {
		if (null == buffer) {
			throw new NullPointerException("buffer cannot be null");
		}

		this.buffer = buffer;
	}
	
	public UnsafeMemory(final long bytes) {
		
		if (bytes <= 0) {
			throw new NullPointerException("Bytes must be > 0");
		}
		
		baseAddress = unsafe.allocateMemory(bytes);
		unsafe.setMemory(baseAddress, bytes, (byte)0);
		this.buffer = null;
	}
	

	public long testUnsafeMemory(long bytes) {
		baseAddress = unsafe.allocateMemory(bytes);
		long reAllocBaseAddress;
		int returnValue, returnValue1, returnValue2, returnValue3, returnIntFromLong;
		unsafe.setMemory(baseAddress, bytes, (byte) 0);
		unsafe.putLong(baseAddress, 17);
		returnIntFromLong = unsafe.getInt(baseAddress);
		
		unsafe.putInt(baseAddress, 17);
		unsafe.putInt(baseAddress + SIZE_OF_INT, 117);
		returnValue = unsafe.getInt(baseAddress);
		returnValue1 = unsafe.getInt(baseAddress + SIZE_OF_INT);
		reAllocBaseAddress = unsafe.reallocateMemory(baseAddress, bytes * 2);
		// unsafe.setMemory(reAllocBaseAddress, bytes, (byte) 0);
		returnValue2 = unsafe.getInt(reAllocBaseAddress);
		returnValue3 = unsafe.getInt(baseAddress);
		returnValue3 = unsafe.getInt(baseAddress + 4);
		unsafe.freeMemory(reAllocBaseAddress);
		return baseAddress;
	}
	
	
	public static UnsafeMemory prepMemoryRWTest(int arraySize) {
		long bytes = (long)arraySize * (long)UnsafeMemory.SIZE_OF_LONG;
		
		System.out.println("Memory allocated : " + bytes / 1024 / 1024 + " MB");
		System.out.println("Memory allocated : " + bytes / 1024  + " KB");
		return new UnsafeMemory(bytes);
	}
	
	public void memoryWriteTest(long arraySize, long baseValue) {
		final long  sizeOfInt = (long) UnsafeMemory.SIZE_OF_LONG;
		long memoryAddress = baseAddress;
		for (long i = 0 ; i < arraySize; i ++) {
			unsafe.putLong(memoryAddress, baseValue + i);
			memoryAddress += sizeOfInt;
		}
	}
	
	public void memoryWriteTestRandom(long arraySize, long baseValue, int[] lookupIntValue) {
		final long  sizeOfInt = (long) UnsafeMemory.SIZE_OF_LONG;
		long memoryAddress = baseAddress;
		for (int i = 0 ; i < arraySize; i ++) {
			memoryAddress = baseAddress + sizeOfInt * lookupIntValue[i];
			unsafe.putLong(memoryAddress, baseValue + i);
		}
	}

	
	public void memoryReadTest(long arraySize) {
		long memoryAddress = baseAddress;
		final long  sizeOfInt = (long) UnsafeMemory.SIZE_OF_LONG;
		long sum = 0;
		for (long i = 0 ; i < arraySize; i ++) {
			sum += unsafe.getLong(memoryAddress);
			memoryAddress += sizeOfInt;
		}
		//System.out.println("Sum from unsafe : " + sum);
	}
	
	public void memoryReadTestRandom(long arraySize, int[] lookupIntValue) {
		long memoryAddress = baseAddress;
		final long  sizeOfInt = (long) UnsafeMemory.SIZE_OF_LONG;
		long sum = 0;
		for (int i = 0 ; i < arraySize; i ++) {
			memoryAddress = baseAddress + sizeOfInt * lookupIntValue[i];
			sum += unsafe.getLong(memoryAddress);
			//memoryAddress += sizeOfInt;
		}
		//System.out.println("Sum from unsafe : " + sum);
	}
	
	
	
	public void releaseMemory() {
		unsafe.freeMemory(baseAddress);
	}

	public void place(Object o, long address) throws Exception {
		  Class clazz = o.getClass();
		  do {
		    for (Field f : clazz.getDeclaredFields()) {
		      if (!Modifier.isStatic(f.getModifiers())) {
		        long offset = unsafe.objectFieldOffset(f);
		        if (f.getType() == long.class) {
		          long val = unsafe.getLong(o, offset);
		          unsafe.putLong(address + offset, val);
		        } else if (f.getType() == int.class) {
		          unsafe.putInt(address + offset, unsafe.getInt(o, offset));
		        } else if (f.getType() == boolean.class) {
		        	boolean currentBoolean = unsafe.getBoolean(o, offset);
		        	byte currentBytes = (currentBoolean == true) ? (byte) 1 : (byte) 0;
		        	unsafe.putByte(address + offset, currentBytes);
		        } else {
		          //throw new UnsupportedOperationException();
		        }
		      }
		    }
		  } while ((clazz = clazz.getSuperclass()) != null);
		}

		public Object read(Class clazz, long address) throws Exception {
		  Object instance = unsafe.allocateInstance(clazz);
		  do {
		    for (Field f : clazz.getDeclaredFields()) {
		      if (!Modifier.isStatic(f.getModifiers())) {
		        long offset = unsafe.objectFieldOffset(f);
		        if (f.getType() == long.class) {
		          unsafe.putLong(instance, offset, unsafe.getLong(address + offset));
		        } else if (f.getType() == int.class) {
		          unsafe.putInt(instance, offset, unsafe.getInt(address + offset));
		        } else if (f.getType() == boolean.class) {
		        	byte boolByte = unsafe.getByte(address + offset);
		        	boolean myBoolean = (boolByte!=0);
		        	unsafe.putBoolean(instance, offset, myBoolean);
		        }else {
		          //throw new UnsupportedOperationException();
		        }
		      }
		    }
		  } while ((clazz = clazz.getSuperclass()) != null);
		  return instance;
		}

	public void reset() {
		this.pos = 0;
	}

	public void putBoolean(final boolean value) {
		unsafe.putBoolean(buffer, byteArrayOffset + pos, value);
		pos += SIZE_OF_BOOLEAN;
	}

	public boolean getBoolean() {
		boolean value = unsafe.getBoolean(buffer, byteArrayOffset + pos);
		pos += SIZE_OF_BOOLEAN;

		return value;
	}

	public void putInt(final int value) {
		unsafe.putInt(buffer, byteArrayOffset + pos, value);
		pos += SIZE_OF_INT;
	}

	public int getInt() {
		int value = unsafe.getInt(buffer, byteArrayOffset + pos);
		pos += SIZE_OF_INT;

		return value;
	}

	public void putLong(final long value) {
		unsafe.putLong(buffer, byteArrayOffset + pos, value);
		pos += SIZE_OF_LONG;
	}

	public long getLong() {
		@SuppressWarnings("restriction")
		long value = unsafe.getLong(buffer, byteArrayOffset + pos);
		pos += SIZE_OF_LONG;

		return value;
	}

	public void putLongArray(final long[] values) {
		putInt(values.length);

		long bytesToCopy = values.length << 3;
		unsafe.copyMemory(values, longArrayOffset, buffer, byteArrayOffset
				+ pos, bytesToCopy);
		pos += bytesToCopy;
	}

	public void putObject(final Object value, long size) {
		putLong(size);
	}

	public HashMap<Integer, String> getObject() {
		long hmSize = getLong();
		HashMap<Integer, String> hm = new HashMap<Integer, String>();

		long bytesToCopy = hmSize;
		unsafe.copyMemory(buffer, byteArrayOffset + pos, hm, longArrayOffset,
				bytesToCopy);
		return hm;
	}

	public long[] getLongArray() {
		int arraySize = getInt();
		long[] values = new long[arraySize];

		long bytesToCopy = values.length << 3;
		unsafe.copyMemory(buffer, byteArrayOffset + pos, values,
				longArrayOffset, bytesToCopy);
		pos += bytesToCopy;

		return values;
	}

	public void putDoubleArray(final double[] values) {
		putInt(values.length);

		long bytesToCopy = values.length << 3;
		unsafe.copyMemory(values, doubleArrayOffset, buffer, byteArrayOffset
				+ pos, bytesToCopy);
		pos += bytesToCopy;
	}

	public double[] getDoubleArray() {
		int arraySize = getInt();
		double[] values = new double[arraySize];

		long bytesToCopy = values.length << 3;
		unsafe.copyMemory(buffer, byteArrayOffset + pos, values,
				doubleArrayOffset, bytesToCopy);
		pos += bytesToCopy;

		return values;
	}
}