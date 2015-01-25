
@SuppressWarnings("restriction")
public class UnSafeHashMap extends UnsafeMemory {

	final int payloadPerKey;
	final int keyCount;
	final long bufferSize;
	final int numberOfHashBuckets;
	final static int fudgeFactor = 2;
	final static int CACHE_LINE = 64;
	final static int L2_CACHE_SIZE = 256 * 1024;
	final long numberOfPartitions;
	BitArray hashBucketAllocated; 
	
	public static UnSafeHashMap CreateUnSafeHashMap(int keySize, int ValueSize,
			int keyCount) {
		int payloadPerKey = keySize + ValueSize;
		long bufferSize = payloadPerKey * keyCount * fudgeFactor;
		long requestSize = bufferSize;
		int numberOfHashBuckets = (int) nextPowerOf2(bufferSize / payloadPerKey);

		if (numberOfHashBuckets * payloadPerKey > bufferSize) {
			// bufferSize = (int)nextPowerOf2(numberOfHashBuckets *
			// payloadPerKey);
			bufferSize = (int) (numberOfHashBuckets * payloadPerKey);
		}

		System.out.println("Data size " + bufferSize / 1024 / 1024
				+ "MB, bucket count " + numberOfHashBuckets);
		System.out.println("Overhead size " + (bufferSize - requestSize) / 1024
				+ "KB");
		UnSafeHashMap unsafeHashMap = new UnSafeHashMap(keySize, ValueSize,
				keyCount, numberOfHashBuckets, bufferSize);
		return unsafeHashMap;
	}

	public UnSafeHashMap(int keySize, int ValueSize, int keyCount,
			int numberOfHashBuckets, long bufferSize) {
		super(new byte[(int) bufferSize]);
		//super.testUnsafeMemory(64);
		this.bufferSize = bufferSize;
		this.payloadPerKey = keySize + ValueSize;
		this.keyCount = keyCount;
		this.numberOfHashBuckets = numberOfHashBuckets;
		this.numberOfPartitions = bufferSize / L2_CACHE_SIZE;
		this.hashBucketAllocated = new BitArray(this.numberOfHashBuckets);
	}

	public final void put(long key, final long value) {
		long hashBucket = calculateHashBucket(key);
		long byteOffset = hashBucket * payloadPerKey;
		hashBucketAllocated.setUnSafe(hashBucket);
		unsafe.putLong(buffer, byteArrayOffset + byteOffset, key);
		unsafe.putLong(buffer, byteArrayOffset + byteOffset + SIZE_OF_LONG,
				value);
	}

	public final void putRowContainer(long key,
			ObjectToBeSerialised rowContainer) {
		long hashBucket = calculateHashBucket(key);
		long byteOffset = hashBucket * payloadPerKey;
		unsafe.putLong(buffer, byteArrayOffset + byteOffset, key);
		hashBucketAllocated.setUnSafe(hashBucket);
		rowContainer.write(this, byteArrayOffset + byteOffset + SIZE_OF_LONG);
	}

	public final ObjectToBeSerialised getRowContainer(long key) {
		long hashBucket = calculateHashBucket(key);
		long byteOffset = hashBucket * payloadPerKey;
		ObjectToBeSerialised otbs2 = null;
		boolean allocated = hashBucketAllocated.getUnsafe(hashBucket);
		long lookupKey = unsafe.getLong(buffer, byteArrayOffset + byteOffset);
		if (allocated
				&& key == lookupKey) {
			// if (key == unsafe.getLong(buffer, byteArrayOffset + byteOffset))
			// UnsafeMemory.read(ObjectToBeSerialised.class, byteOffset);
			return ObjectToBeSerialised.read(this, byteArrayOffset + byteOffset
					+ UnsafeMemory.SIZE_OF_LONG);
		} else
			return null;
	}

	public final void testReflection () throws Exception {
		 super.testUnsafeMemory(512);
		 ObjectToBeSerialised obj = new ObjectToBeSerialised(1, true,
					((int) 1) + 777, ((int) 1) + 99, new byte[] { 1, 2, 3, 4,
							5, 6, 7, 8, 9, 10 });
		 ObjectToBeSerialised objectToRead = null;		 
		 super.place(obj, super.baseAddress);
		 objectToRead =  (ObjectToBeSerialised)super.read(ObjectToBeSerialised.class, super.baseAddress);
		 objectToRead.toString();
	}
	
	final public boolean containsKey(long key) {
		long hashBucket = calculateHashBucket(key);
		long byteOffset = hashBucket * payloadPerKey;
		long storedKey = unsafe.getLong(buffer, byteArrayOffset + byteOffset);
		boolean keyAllocated = hashBucketAllocated.getUnsafe(hashBucket);
		return ((key == storedKey) && keyAllocated);
		// return key == unsafe.getLong(buffer, byteArrayOffset + byteOffset);
	}

	public final void putBoolean(final boolean value, long pos) {
		unsafe.putBoolean(buffer, pos, value);
	}

	public final boolean getBoolean(long pos) {
		return unsafe.getBoolean(buffer, pos);
	}

	public final void putInt(final int value, long pos) {
		unsafe.putInt(buffer, pos, value);
	}

	public final int getInt(long pos) {
		return unsafe.getInt(buffer, pos);
	}

	public void putLong(final long value, long pos) {
		unsafe.putLong(buffer, pos, value);
	}

	public long getLong(long pos) {
		return unsafe.getLong(buffer, pos);
	}

	public void putBytes(long key, final byte[] value) {
		long hashBucket = calculateHashBucket(key);
		long byteOffset = hashBucket * payloadPerKey;
		putBytes(byteArrayOffset + byteOffset, value, value.length);
	}

	public final void putBytes(long byteOffset, byte[] value, int length) {

		int i = 0;
		for (; i < length - 8; i += 8) {
			unsafe.putByte(buffer, byteOffset, value[i]);
			unsafe.putByte(buffer, byteOffset + 1, value[i + 1]);
			unsafe.putByte(buffer, byteOffset + 2, value[i + 2]);
			unsafe.putByte(buffer, byteOffset + 3, value[i + 3]);
			unsafe.putByte(buffer, byteOffset + 4, value[i + 4]);
			unsafe.putByte(buffer, byteOffset + 5, value[i + 5]);
			unsafe.putByte(buffer, byteOffset + 6, value[i + 6]);
			unsafe.putByte(buffer, byteOffset + 7, value[i + 7]);
			byteOffset += 8;
		}

		for (; i < length; i++) {
			unsafe.putByte(buffer, byteOffset + i, value[i]);
		}
	}

	public long get(long key) {
		// int hashBucket = calculateHashBucket(key);
		long byteOffset = calculateHashBucket(key) * payloadPerKey;
		return unsafe.getLong(buffer, byteArrayOffset + byteOffset);
	}

	public void getBytes(long key, byte[] value) {
		// int hashBucket = calculateHashBucket(key);
		long byteOffset = calculateHashBucket(key) * payloadPerKey;
		getBytes(byteArrayOffset + byteOffset, value, value.length);
	}

	public void getBytes(long byteOffset, byte[] value, int length) {

		int i = 0;
		for (; i < length - 8; i += 8) {
			value[i] = unsafe.getByte(buffer, byteOffset);
			value[i + 1] = unsafe.getByte(buffer, byteOffset + 1);
			value[i + 2] = unsafe.getByte(buffer, byteOffset + 2);
			value[i + 3] = unsafe.getByte(buffer, byteOffset + 3);
			value[i + 4] = unsafe.getByte(buffer, byteOffset + 4);
			value[i + 5] = unsafe.getByte(buffer, byteOffset + 5);
			value[i + 6] = unsafe.getByte(buffer, byteOffset + 6);
			value[i + 7] = unsafe.getByte(buffer, byteOffset + 7);
			byteOffset += 8;
		}

		for (; i < length; i++) {
			value[i] = unsafe.getByte(buffer, byteOffset + i);
		}
	}

	public void putExpensiveHash(long key, final long value) {

		byte[] data = longToByteArray(key);
		long hash64 = hash64(data, data.length, 4);
		int hashBucket = (int) (hash64 & (this.numberOfHashBuckets - 1));
		System.out.println(key + " " + hashBucket);
	}

	public void writeLong(final long value) {
		unsafe.putLong(buffer, byteArrayOffset + pos, value);
		pos += SIZE_OF_LONG;
	}

	public long readLong() {
		long value = unsafe.getLong(buffer, byteArrayOffset + 0);
		pos += SIZE_OF_LONG;

		return value;
	}

	public static long nextPowerOf2(long a) {
		long b = 1;
		while (b < a) {
			b = b << 1;
		}
		return b;
	}

	final long hash64shift(long key) {
		key = (~key) + (key << 21); // key = (key << 21) - key - 1;
		key = key ^ (key >>> 24);
		key = (key + (key << 3)) + (key << 8); // key * 265
		key = key ^ (key >>> 14);
		key = (key + (key << 2)) + (key << 4); // key * 21
		key = key ^ (key >>> 28);
		key = key + (key << 31);
		return key;
	}

	final long calculateHashBucket(long key) {
		key = (~key) + (key << 21); // key = (key << 21) - key - 1;
		key = key ^ (key >>> 24);
		key = (key + (key << 3)) + (key << 8); // key * 265
		key = key ^ (key >>> 14);
		key = (key + (key << 2)) + (key << 4); // key * 21
		key = key ^ (key >>> 28);
		key = key + (key << 31);
		return key & (this.numberOfHashBuckets - 1);
	}

	public static long hash64(byte[] data, int length, int seed) {
		final long m = 0xc6a4a7935bd1e995L;
		final int r = 47;

		long h = (seed & 0xffffffffl) ^ (length * m);

		int length8 = length >> 3;

		for (int i = 0; i < length8; i++) {
			final int i8 = i << 3;
			long k = ((long) data[i8 + 0] & 0xff)
					+ (((long) data[i8 + 1] & 0xff) << 8)
					+ (((long) data[i8 + 2] & 0xff) << 16)
					+ (((long) data[i8 + 3] & 0xff) << 24)
					+ (((long) data[i8 + 4] & 0xff) << 32)
					+ (((long) data[i8 + 5] & 0xff) << 40)
					+ (((long) data[i8 + 6] & 0xff) << 48)
					+ (((long) data[i8 + 7] & 0xff) << 56);

			k *= m;
			k ^= k >>> r;
					k *= m;

					h ^= k;
					h *= m;
		}

		switch (length % 8) {
		case 7:
			h ^= (long) (data[(length & ~7) + 6] & 0xff) << 48;
		case 6:
			h ^= (long) (data[(length & ~7) + 5] & 0xff) << 40;
		case 5:
			h ^= (long) (data[(length & ~7) + 4] & 0xff) << 32;
		case 4:
			h ^= (long) (data[(length & ~7) + 3] & 0xff) << 24;
		case 3:
			h ^= (long) (data[(length & ~7) + 2] & 0xff) << 16;
		case 2:
			h ^= (long) (data[(length & ~7) + 1] & 0xff) << 8;
		case 1:
			h ^= (long) (data[length & ~7] & 0xff);
			h *= m;
		}
		;

		h ^= h >>> r;
			h *= m;
			h ^= h >>> r;

			return h;
	}

	public static final byte[] longToByteArray(long value) {
		return new byte[] { (byte) (value >>> 56), (byte) (value >>> 48),
				(byte) (value >>> 40), (byte) (value >>> 32),
				(byte) (value >>> 24), (byte) (value >>> 16),
				(byte) (value >>> 8), (byte) value };
	}

	public static int LeadingZeros(int x) {
		x |= (x >> 1);
		x |= (x >> 2);
		x |= (x >> 4);
		x |= (x >> 8);
		x |= (x >> 16);
		return (UnsafeMemory.SIZE_OF_INT * 8 - Ones(x));
	}

	public static int Ones(int x) {
		x -= ((x >> 1) & 0x55555555);
		x = (((x >> 2) & 0x33333333) + (x & 0x33333333));
		x = (((x >> 4) + x) & 0x0f0f0f0f);
		x += (x >> 8);
		x += (x >> 16);
		return (x & 0x0000003f);
	}

	public static int TrailingZeros(int x) {
		return (Ones((x & -x) - 1));
	}

}
