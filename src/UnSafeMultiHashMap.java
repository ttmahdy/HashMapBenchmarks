import java.lang.reflect.Field;

import sun.misc.Unsafe;

@SuppressWarnings("restriction")
public class UnSafeMultiHashMap {
	protected final Unsafe unsafe;
	protected long baseAddress;
	public long putProbeCount = 0;
	public long getProbeCount = 0;
	public long matchCount = 0;
	private long tupleCount = 0;
	private final int keySize;
	private final int payloadSize;
	private final int keyCount;
	private final long hashBucketCount;
	private final double fillFactor;
	private final long hashBucketSize;
	private final int payloadBeginOffset;
	private long allocatedBytes;

	private final byte NEGATIVE_ONE = (byte) -1;
	private final byte POSITIVE_ONE = (byte) 1;
	private final byte ZERO = (byte) 0;
	private final int CACHE_LINE_SIZE = 64;
	private final int HASH_BUCKET_HEADER_SIZE = 4 + 1;
	private final int TUPLES_PER_HASH_BUCKET = 4;
	private long[] reusableKeyHolder;

	public static final int SIZE_OF_BYTE = 1;
	public static final int SIZE_OF_BOOLEAN = 1;
	public static final int SIZE_OF_INT = 4;
	public static final int SIZE_OF_LONG = 8;

	/**
	 * @param keySize
	 * @param payloadSize
	 * @param keyCount
	 * @param fillFactor
	 * @throws Exception
	 * @throws SecurityException
	 */
	public UnSafeMultiHashMap(int keySize, int payloadSize, int keyCount,
			double fillFactor) throws Exception, SecurityException {

		if (keyCount <= 0) {
			throw new NullPointerException("keyCount must be > 0");
		}

		long enterTime = System.nanoTime();
		this.keySize = keySize;
		this.payloadSize = payloadSize;
		this.keyCount = keyCount;
		this.fillFactor = fillFactor;
		long unOptimalHashBucketCount = (long) Math
				.ceil((keyCount / fillFactor / TUPLES_PER_HASH_BUCKET));
		this.hashBucketCount = nextPowerOf2(unOptimalHashBucketCount);

		this.hashBucketSize = HASH_BUCKET_HEADER_SIZE + TUPLES_PER_HASH_BUCKET
				* (this.keySize + this.payloadSize);

		this.allocatedBytes = this.hashBucketCount * this.hashBucketSize;
		this.payloadBeginOffset = HASH_BUCKET_HEADER_SIZE
				+ (TUPLES_PER_HASH_BUCKET * this.keySize);

		if (payloadBeginOffset > CACHE_LINE_SIZE) {
			System.out
					.println("Reduce the number of TUPLES_PER_HASH_BUCKET as keys will span more the one cacheline");
		}
		this.reusableKeyHolder = new long[TUPLES_PER_HASH_BUCKET];
		//System.out.println("Hash bucket size :" + hashBucketSize);
		
		System.out.println("Allocated memory size " + allocatedBytes / 1024 / 1024 + "MB, bucket count " + this.hashBucketCount);
		System.out.println("Overhead size "+ (this.hashBucketCount - unOptimalHashBucketCount) * hashBucketSize / 1024 + "KB");

		Field field = Unsafe.class.getDeclaredField("theUnsafe");
		field.setAccessible(true);
		unsafe = (Unsafe) field.get(null);

		long startTime = System.nanoTime();
		baseAddress = unsafe.allocateMemory(allocatedBytes);
		unsafe.setMemory(baseAddress, allocatedBytes, (byte) 0);
		//System.out.println("Allocate and set memory: "+ (int) ((allocatedBytes / 1024 / 1024) / ((System.nanoTime() - startTime) / 1000000000.0))+ " MB/sec, "+ ((System.nanoTime() - startTime) / 1000000000.0));

		startTime = System.nanoTime();
		for (long i = 0; i < hashBucketCount; i++) {
			long hashBucketAddress = baseAddress + i * hashBucketSize;
			unsafe.putInt(hashBucketAddress, -1); // Mark the next as -1
			unsafe.putByte(hashBucketAddress + SIZE_OF_INT, ZERO); // Mark the next as -1
		}

		//System.out
		//		.println("Initialized hash bucket count:"
		//				+ hashBucketCount
		//				+ ", rate: "
		//				+ (int) ((allocatedBytes / 1024 / 1024) / ((System
		//						.nanoTime() - startTime) / 1000000000.0))
		//				+ " MB/sec, "
		//				+ ((System.nanoTime() - startTime) / 1000000000.0));

		int nextHashBucket;
		startTime = System.nanoTime();
		for (long i = 0; i < hashBucketCount; i++) {
			long hashBucketAddress = baseAddress + i * hashBucketSize;
			nextHashBucket = unsafe.getInt(hashBucketAddress); // Mark the next
																// as -1
			assert (nextHashBucket == -1);
		}

		//System.out
		//		.println("Check hash bucket count:"
		//				+ hashBucketCount
		//				+ ", rate: "
		//				+ (int) ((allocatedBytes / 1024 / 1024) / ((System
		//						.nanoTime() - startTime) / 1000000000.0))
		//				+ " MB/sec, "
		//				+ ((System.nanoTime() - startTime) / 1000000000.0));

		//System.out.println("Constructor time :"
		//		+ ((System.nanoTime() - enterTime) / 1000000000.0));

	}
	
	public void releaseMemory() {
		unsafe.freeMemory(baseAddress);
	}

	// keep it simple for now
	public boolean put(long key, long value) {
		
		long hashBucketId = calculateHashBucket(key);
		long nextEmptyBucketWithSpace = -1;
		boolean tupleInserted = false;
		long rowCount = tupleCount;
		
		// To find the hash bucket address calculate the offset from the base address
		long hashBucketAddress = baseAddress + hashBucketId * hashBucketSize;
		
		//int nextHashBucketId = unsafe.getInt(hashBucketAddress);
		byte tuplesInHashBucket = unsafe.getByte(hashBucketAddress + SIZE_OF_INT);
	
		
		if (tuplesInHashBucket == TUPLES_PER_HASH_BUCKET ) {
			nextEmptyBucketWithSpace = getNextEmptyBucket(hashBucketId);
			if (nextEmptyBucketWithSpace != -1) {
				
				// Set the pointer to the next chain
				unsafe.putInt(hashBucketAddress, (int)nextEmptyBucketWithSpace);
				
				// insert in the first empty chain we found
				doPut(key, value, nextEmptyBucketWithSpace);
				
				tupleInserted = true;
			} else {
				// No Empty buckets
				System.out.println("Hash table full " + tupleCount + " " + key);
				return false;
			}
						
		} else { // There is room in this hash bucket so insert the tuple 
			
			// Write the Nth key
			doPut(key, value, hashBucketId);
			tupleInserted = true;
		}
		
		return tupleInserted;
	}
	
	private final void doPut (long key, long value, long hashBucketId) {
		// To find the hash bucket address calculate the offset from the base address
		tupleCount++;
		long hashBucketAddress = baseAddress + hashBucketId * hashBucketSize;
		byte tuplesInHashBucket = unsafe.getByte(hashBucketAddress + SIZE_OF_INT);
		long keyOffset = hashBucketAddress + HASH_BUCKET_HEADER_SIZE + tuplesInHashBucket * this.keySize;
		long valueOffset = hashBucketAddress + payloadBeginOffset + tuplesInHashBucket * this.payloadSize;
		unsafe.putLong(keyOffset, key);
		unsafe.putLong(valueOffset, value);
		
		// Update the number of tuples in this hash bucket
		unsafe.putByte(hashBucketAddress + SIZE_OF_INT, ++tuplesInHashBucket);
	}
	
	public long get(long key) {

		long hashBucketId = calculateHashBucket(key);
		byte tuplesInHashBucket = -1;

		// To find the hash bucket address calculate the offset from the base
		// address
		long hashBucketAddress = baseAddress + hashBucketId * hashBucketSize;

		int nextHashBucketId = unsafe.getInt(hashBucketAddress);
		
		do {
			nextHashBucketId = unsafe.getInt(hashBucketAddress);
			tuplesInHashBucket = unsafe.getByte(hashBucketAddress + SIZE_OF_INT);
			
			// All keys should be on the same cache line so just get them all in
			// a tight loop
			for (int i = 0; i < tuplesInHashBucket; i++) {
				reusableKeyHolder[i] = unsafe.getLong(hashBucketAddress
						+ HASH_BUCKET_HEADER_SIZE + i * this.keySize);
			}

			for (int i = 0; i < tuplesInHashBucket; i++) {
				if (reusableKeyHolder[i] == key) {
					long valueOffset = hashBucketAddress + payloadBeginOffset
							+ i * this.payloadSize;
					matchCount++;
					return unsafe.getLong(valueOffset);
				}
			}
			
			getProbeCount++;
			hashBucketAddress = baseAddress + nextHashBucketId * hashBucketSize;
		} while (nextHashBucketId != -1 && tuplesInHashBucket > 0);

		/*
			// Write the Nth key
			long foundKey = -1;
			long foundValue = -1;
			long keyOffset = hashBucketAddress + HASH_BUCKET_HEADER_SIZE
					+ (tuplesInHashBucket - 1) * this.keySize;
			long valueOffset = hashBucketAddress + payloadBeginOffset
					+ (tuplesInHashBucket - 1) * this.payloadSize;
	
			foundKey = unsafe.getLong(keyOffset);
			foundValue = unsafe.getLong(valueOffset);
		*/

		return -1;
	}
	
	private long getNextEmptyBucket(long currentBucketId) {
		long bucketId = -1;
		byte tuplesCount = 0; 
		long hashBucketAddress;
		
		for (long i = currentBucketId; i < hashBucketCount; i++) {
			putProbeCount++;
			hashBucketAddress = baseAddress + i * hashBucketSize;
			tuplesCount = unsafe.getByte(hashBucketAddress + SIZE_OF_INT);
			if (tuplesCount < TUPLES_PER_HASH_BUCKET)
				return i;
		}
		
		for (long i = 0; i < currentBucketId; i++) {
			putProbeCount++;
			hashBucketAddress = baseAddress + i * hashBucketSize;
			tuplesCount = unsafe.getByte(hashBucketAddress + SIZE_OF_INT);
			if (tuplesCount < TUPLES_PER_HASH_BUCKET)
				return i;
		}

		return bucketId;
	}

	@SuppressWarnings("unused")
	// This is not actually used in code
	// just an illustration of the hash bucket header
	private class HashBucketHeader {
		/*
		 * Instead of storing the next as a long offset into memory use an
		 * integer which is bucket ID, since hash buckets will be power of two
		 * calculating the address will be cheap as next * HASH_BUCKET_SIZE
		 */
		int next;
		byte count;
	}

	@SuppressWarnings("unused")
	// This is the in-memory layout of each hash bucker
	private class HashBucket {
		HashBucketHeader hashBucketHeader; // 5 bytes
		long[] keys; // Attempt to store header and keys on the same cache line
		Object[] payload; // Payload
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
	
	final long quickHash(long key) {
		//long key1  , key2 , key3 = key;
		//key1 = (key + (key << 3)) + (key << 8); // key * 265
		//key2 = (key + (key << 2)) + (key << 4); // key * 21
		//key3 = key1 + (key2 << 31);
		return (((key ^ 1299689) + 1299689 << 4 + 1284739 >> 1) ^ ((~key) + (key << 21))) & (this.hashBucketCount - 1);
	}
	
	final long calculateHashBucket(long key) {
		key = (~key) + (key << 21); // key = (key << 21) - key - 1;
		key = key ^ (key >>> 24);
		key = (key + (key << 3)) + (key << 8); // key * 265
		key = key ^ (key >>> 14);
		key = (key + (key << 2)) + (key << 4); // key * 21
		key = key ^ (key >>> 28);
		key = key + (key << 31);
		return key & (this.hashBucketCount - 1);
	}

}
