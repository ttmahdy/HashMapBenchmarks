import java.util.Arrays;


	// Note: We use this instead of java.util.BitSet because we need access to the long[] data field
	@SuppressWarnings("restriction")
	final class BitArray extends UnsafeMemory{

		protected static final long byteArrayOffset = unsafe.arrayBaseOffset(byte[].class);
		final long[] data;
		long bitCount;

		BitArray(long bits) {
			this(new long[(int) Math.ceil(bits / 64)], bits);
		}

		// Used by serialization
		BitArray(long[] data, long bits) {
			//super(new byte[(int) Math.ceil(bits / 64)]);
			super((long) Math.ceil(bits / 64));
			assert(data.length > 0);
			this.data = data;
			long bitCount = 0;
			for (long value : data) {
				bitCount += Long.bitCount(value);
			}
			this.bitCount = bitCount;
		}
		
		@Override
		protected void finalize() {
			unsafe.freeMemory(baseAddress);
		}

		/** Returns true if the bit changed value. */
		final boolean setUnSafe(long index) {
			if (!getUnsafe(index)) {
				long byteOffset = index >>> 6;
				long curentLong = unsafe.getLong(baseAddress + byteOffset);
				long comparand = 1L << index;
				long value = curentLong | comparand;
				unsafe.putLong(baseAddress + byteOffset, value);
				bitCount++;
				return true;
			}
			return false;
		}

		
		/** Returns true if the bit changed value. */
		final boolean set(long index) {
			if (!get(index)) {
				data[(int) (index >>> 6)] |= (1L << index);
				bitCount++;
				return true;
			}
			return false;
		}

		final boolean getUnsafe(long index) {
			long byteOffset = index >>> 6;
			long currentLong = unsafe.getLong(baseAddress + byteOffset);
			long comparand = 1L << index;
			long result =currentLong & comparand;  
			return (unsafe.getLong(baseAddress + (index >>> 6)) & (1L << index)) != 0;
		}

		
		final boolean get(long index) {
			return (data[(int) (index >>> 6)] & (1L << index)) != 0;
		}

		/** Number of bits */
		long bitSize() {
			return (long) data.length * Long.SIZE;
		}

		/** Number of set bits (1s) */
		long bitCount() {
			return bitCount;
		}

		int arraySize()
		{
			return data.length;
		}

		
		/** Combines the two BitArrays using bitwise OR. */
		void putAll(BitArray array) {
			assert(data.length == array.data.length);
			bitCount = 0;
			for (int i = 0; i < data.length; i++) {
				data[i] |= array.data[i];
				bitCount += Long.bitCount(data[i]);
			}
		}

		@Override 
		public boolean equals(Object o) {
			if (o instanceof BitArray) {
				BitArray bitArray = (BitArray) o;
				return Arrays.equals(data, bitArray.data);
			}
			return false;
		}

		@Override public int hashCode() {
			return Arrays.hashCode(data);
		}
	}