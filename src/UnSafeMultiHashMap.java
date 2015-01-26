import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import sun.misc.Unsafe;

@SuppressWarnings("restriction")
public class UnSafeMultiHashMap {
	protected final Unsafe unsafe;
	protected long baseAddress;
	public int putProbeCount = 0;
	public int matchCount = 0;
	public int softProbeCount = 0;
	public int hardProbeCount = 0;
	
	private long tupleCount = 0;
	private final int keySize;
	private final int payloadSize;
	private final int keyCount;
	public final long hashBucketCount;
	private final double fillFactor;
	private final long hashBucketSize;
	private final int payloadBeginOffset;
	private long allocatedBytes;
	private HashBucketInfo probeHashBucket;

	private final byte ZERO = (byte) 0;
	private final int CACHE_LINE_SIZE = 64;
	private final int HASH_BUCKET_HEADER_SIZE = 4 + 1;
	private final int TUPLES_PER_HASH_BUCKET = 2;
	// Seemed like a good idea but apparently not :((
	// Made get run 3 times slower
	// private long[] reusableKeyHolder;

	public static final int SIZE_OF_BYTE = 1;
	public static final int SIZE_OF_BOOLEAN = 1;
	public static final int SIZE_OF_INT = 4;
	public static final int SIZE_OF_LONG = 8;
	// private long[] rowsPerBucket;
	// private List<List<Long>> movedPerBucket;

	private long mA;
	private long mB;

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
		this.probeHashBucket = new HashBucketInfo();
		long unOptimalHashBucketCount = (long) Math
				.ceil((keyCount / TUPLES_PER_HASH_BUCKET));
		long temp = nextPowerOf2(unOptimalHashBucketCount);
		if (((double) unOptimalHashBucketCount / (double) temp) < fillFactor) {
			this.hashBucketCount = nextPowerOf2((long) ((double) unOptimalHashBucketCount / (double) fillFactor));
		} else {
			this.hashBucketCount = nextPowerOf2(unOptimalHashBucketCount);
		}

		this.hashBucketSize = HASH_BUCKET_HEADER_SIZE + TUPLES_PER_HASH_BUCKET
				* (this.keySize + this.payloadSize);

		this.allocatedBytes = this.hashBucketCount * this.hashBucketSize;
		this.payloadBeginOffset = HASH_BUCKET_HEADER_SIZE
				+ (TUPLES_PER_HASH_BUCKET * this.keySize);

		if (payloadBeginOffset > CACHE_LINE_SIZE) {
			System.out
			.println("Reduce the number of TUPLES_PER_HASH_BUCKET as keys will span more the one cacheline");
		}

		// this.reusableKeyHolder = new long[TUPLES_PER_HASH_BUCKET];
		// this.rowsPerBucket = new long[(int)hashBucketCount];
		// this.movedPerBucket = new
		// ArrayList<List<Long>>((int)hashBucketCount);
		// for (int i = 0; i < hashBucketCount; i++) {
		// this.rowsPerBucket[i] = 0;
		// List<Long> newList = new LinkedList<Long>();
		// movedPerBucket.add(newList);
		// }

		// System.out.println("Hash bucket size :" + hashBucketSize);

		System.out.println("Allocated memory size " + allocatedBytes / 1024
				/ 1024 + "MB, bucket count " + this.hashBucketCount
				+ ", instead of " + unOptimalHashBucketCount);
		System.out
		.println("Overhead size "
				+ (this.allocatedBytes - (keyCount * (this.keySize + this.payloadSize)))
				/ 1024 + "KB");

		Field field = Unsafe.class.getDeclaredField("theUnsafe");
		field.setAccessible(true);
		unsafe = (Unsafe) field.get(null);

		long startTime = System.nanoTime();
		baseAddress = unsafe.allocateMemory(allocatedBytes);
		unsafe.setMemory(baseAddress, allocatedBytes, (byte) 0);
		// System.out.println("Allocate and set memory: "+ (int)
		// ((allocatedBytes / 1024 / 1024) / ((System.nanoTime() - startTime) /
		// 1000000000.0))+ " MB/sec, "+ ((System.nanoTime() - startTime) /
		// 1000000000.0));

		startTime = System.nanoTime();
		for (long i = 0; i < hashBucketCount; i++) {
			long hashBucketAddress = baseAddress + i * hashBucketSize;
			unsafe.putInt(hashBucketAddress, -1); // Mark the next as -1
			unsafe.putByte(hashBucketAddress + SIZE_OF_INT, ZERO); // Mark the
			// next as
			// -1
		}

		// System.out
		// .println("Initialized hash bucket count:"
		// + hashBucketCount
		// + ", rate: "
		// + (int) ((allocatedBytes / 1024 / 1024) / ((System
		// .nanoTime() - startTime) / 1000000000.0))
		// + " MB/sec, "
		// + ((System.nanoTime() - startTime) / 1000000000.0));

		int nextHashBucket;
		startTime = System.nanoTime();
		for (long i = 0; i < hashBucketCount; i++) {
			long hashBucketAddress = baseAddress + i * hashBucketSize;
			nextHashBucket = unsafe.getInt(hashBucketAddress); // Mark the next
			// as -1
			assert (nextHashBucket == -1);
		}

		// System.out
		// .println("Check hash bucket count:"
		// + hashBucketCount
		// + ", rate: "
		// + (int) ((allocatedBytes / 1024 / 1024) / ((System
		// .nanoTime() - startTime) / 1000000000.0))
		// + " MB/sec, "
		// + ((System.nanoTime() - startTime) / 1000000000.0));

		// System.out.println("Constructor time :"
		// + ((System.nanoTime() - enterTime) / 1000000000.0));

		Random r = new Random();
		mA = r.nextLong();
		mB = r.nextLong();

	}

	public void releaseMemory() {
		unsafe.freeMemory(baseAddress);
	}

	// keep it simple for now
	public boolean put(long key, long value) {

		long hashBucketId = calculateHashBucket(key);
		long nextEmptyBucketWithSpace = -1;
		long rowCount = tupleCount;
		long hashBucketAddress;
		byte tuplesInHashBucket;

		// To find the hash bucket address calculate the offset from the base
		// address
		hashBucketAddress = baseAddress + hashBucketId * hashBucketSize;

		// int nextHashBucketId = unsafe.getInt(hashBucketAddress);
		tuplesInHashBucket = unsafe.getByte(hashBucketAddress
				+ SIZE_OF_INT);
		
		if (tuplesInHashBucket < TUPLES_PER_HASH_BUCKET) 
		{ 
			// There is room in this hash bucket so insert the tuple
			// Write the Nth key
			// doPut(key, value, hashBucketId);
			doPutDirect(key, value, tuplesInHashBucket, hashBucketAddress);
			rowCount++;
			return true;
		}
		else
		{
			//nextEmptyBucketWithSpace = getNextEmptyBucket(hashBucketId);
			nextEmptyBucketWithSpace = getNextEmptyBucketGray(hashBucketId);
			if (nextEmptyBucketWithSpace != -1) {

				// Set the pointer to the next chain
				unsafe.putInt(hashBucketAddress, (int) nextEmptyBucketWithSpace);

				// Since we already have this info from probing use it directly and avoid 
				// touching the same memory twice
				doPutDirect(key, value, probeHashBucket.tuplesCount , probeHashBucket.hashBucketAddress);
				
				// insert in the first empty chain we found
				//doPut(key, value, nextEmptyBucketWithSpace);
				rowCount++;
				return true;

			} else {
				// No Empty buckets
				//System.out.println("Hash table full " + tupleCount + " " + key);
				return false;
			}
		} 
		
		//return tupleInserted;
	}

	private final void doPut(long key, long value, long hashBucketId) {
		long hashBucketAddress = baseAddress + hashBucketId * hashBucketSize;
		byte tuplesInHashBucket = unsafe.getByte(hashBucketAddress
				+ SIZE_OF_INT);
		byte offset = tuplesInHashBucket;

		// Update the number of tuples in this hash bucket
		unsafe.putByte(hashBucketAddress + SIZE_OF_INT, ++tuplesInHashBucket);
		long keyOffset = hashBucketAddress + HASH_BUCKET_HEADER_SIZE + offset
				* this.keySize;
		unsafe.putLong(keyOffset, key);
		long valueOffset = hashBucketAddress + payloadBeginOffset + offset
				* this.payloadSize;
		unsafe.putLong(valueOffset, value);
		tupleCount++;
	}

	// Save a couple of instructions
	private final void doPutDirect(long key, long value,
			byte tuplesInHashBucket, long hashBucketAddress) {
		byte offset = tuplesInHashBucket;
		unsafe.putByte(hashBucketAddress + SIZE_OF_INT, ++tuplesInHashBucket);

		long keyOffset = hashBucketAddress + HASH_BUCKET_HEADER_SIZE + offset
				* this.keySize;
		unsafe.putLong(keyOffset, key);

		long valueOffset = hashBucketAddress + payloadBeginOffset + offset
				* this.payloadSize;
		unsafe.putLong(valueOffset, value);

		// Update the number of tuples in this hash bucket
		tupleCount++;
	}

	public long get(long key) {

		int nextHashBucketId;
		long valueOffset;

		long hashBucketId = calculateHashBucket(key);

		// To find the hash bucket address calculate the offset from the base
		// address
		long hashBucketAddress = baseAddress + hashBucketId * hashBucketSize;
		byte tuplesInHashBucket = unsafe.getByte(hashBucketAddress
				+ SIZE_OF_INT);

		do {
			// All keys should be on the same cache line so just get them all in
			// a tight loop
			// for (int i = 0; i < tuplesInHashBucket; i++) {
			// reusableKeyHolder[i] = unsafe.getLong(hashBucketAddress
			// + HASH_BUCKET_HEADER_SIZE + i * this.keySize);
			// }
			for (long i = 0; i < tuplesInHashBucket; i++) {
				if (unsafe.getLong(hashBucketAddress + HASH_BUCKET_HEADER_SIZE
						+ i * this.keySize) == key) {
					valueOffset = hashBucketAddress + payloadBeginOffset + i
							* this.payloadSize;
					//softProbeCount++;	
					//matchCount++;
					return unsafe.getLong(valueOffset);
				}
			}
			hardProbeCount++;
			nextHashBucketId = unsafe.getInt(hashBucketAddress);
			hashBucketAddress = baseAddress + nextHashBucketId * hashBucketSize;
			tuplesInHashBucket = unsafe
					.getByte(hashBucketAddress + SIZE_OF_INT);
		} while (nextHashBucketId != -1 && tuplesInHashBucket > 0);

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
			
			if (tuplesCount < TUPLES_PER_HASH_BUCKET) {
				// Save this info so don't have to look it up again.
				probeHashBucket.tuplesCount = tuplesCount;
				probeHashBucket.hashBucketAddress = hashBucketAddress;
				probeHashBucket.hashBucketId = i;

				return i;
			}
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

	// Use Gray code to avoid hash collision :)
	public long getNextEmptyBucketGray(long currentBucketId) {
		long bucketId = -1;
		byte tuplesCount = 0;
		long hashBucketAddress;
		long firstPartitionId = currentBucketId;
		long nextGrayHashBucketId = nextGrayCode(grayToBinary(currentBucketId));
		
		// Loop to  (hashBucketCount - 1) because we already checked one hash bucket and now we are probing
		for (long i = 0; i < hashBucketCount - 1; i++) {
			putProbeCount++;
			
			// If we looped back to the start go to the next
			if (firstPartitionId == nextGrayHashBucketId) {
				nextGrayHashBucketId = nextGrayCode(nextGrayHashBucketId);
			}
			
			if (nextGrayHashBucketId >= hashBucketCount) {
				nextGrayHashBucketId = 0;
			}
			
			//System.out.println(i + " " + nextGrayHashBucketId);
			hashBucketAddress = baseAddress + nextGrayHashBucketId * hashBucketSize;
			tuplesCount = unsafe.getByte(hashBucketAddress + SIZE_OF_INT);
			if (tuplesCount < TUPLES_PER_HASH_BUCKET) {
				probeHashBucket.tuplesCount = tuplesCount;
				probeHashBucket.hashBucketAddress = hashBucketAddress;
				probeHashBucket.hashBucketId = nextGrayHashBucketId;
				return nextGrayHashBucketId;
			}

			nextGrayHashBucketId = nextGrayCode(nextGrayHashBucketId);
		}

		return bucketId;
	}

	private static long nextGrayCode(long gray) {
		if (isGrayCodeodd(gray)) {
			long y = gray & -gray;
			return gray ^ (y << 1);
		} else {
			// Flip rightmost bit
			return gray ^ 1;
		}
	}

	static boolean isGrayCodeodd(long gray) {
		return (boolean) ((Long.bitCount(gray) % 2) == 1);
	}

	static long grayToBinary(long num) {
		long mask;
		for (mask = num >> 1; mask != 0; mask = mask >> 1) {
			num = num ^ mask;
		}
		return num;
	}
	
	private class HashBucketInfo {
		long hashBucketAddress;

		// Not needed just used for debugging
		long hashBucketId; 
		byte tuplesCount;
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
		// long key1 , key2 , key3 = key;
		// key1 = (key + (key << 3)) + (key << 8); // key * 265
		// key2 = (key + (key << 2)) + (key << 4); // key * 21
		// key3 = key1 + (key2 << 31);
		return (((key ^ 1299689) + 1299689 << 4 + 1284739 >> 1) ^ ((~key) + (key << 21)))
				& (this.hashBucketCount - 1);
	}

	final long calculateHashBucket(long key) {
		/*
		 * key = (~key) + (key << 21); // key = (key << 21) - key - 1; key = key
		 * ^ (key >>> 24); key = (key + (key << 3)) + (key << 8); // key * 265
		 * key = key ^ (key >>> 14); key = (key + (key << 2)) + (key << 4); //
		 * key * 21 key = key ^ (key >>> 28); key = key + (key << 31);
		 */
		return key & (this.hashBucketCount - 1);
	}

	final long calculateHashBucketMT(long key) {

		long upper = key >> 32;
		long lower = key << 32;

		/*
		 * Return the pairwise product of those bits, shifted down so that only
		 * lgSize bits remain in the output.
		 */
		return ((upper * mA + lower * mB) >>> (32 - this.hashBucketCount))
				& (this.hashBucketCount - 1);
	}

}
