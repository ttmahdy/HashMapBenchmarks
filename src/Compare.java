import gnu.trove.impl.hash.TLongLongHash;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.google.common.collect.HashMultimap;
import com.sun.tools.javac.util.Bits;
import com.sun.tools.javah.LLNI;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.sourceforge.sizeof.SizeOf;
import cern.colt.map.OpenLongObjectHashMap;


public class Compare {

	public final static byte INT_8 = 0b0001; // 1 (17 = array)
	public final static byte INT_16 = 0b0010; // 2 (18 ..)
	public final static byte INT_32 = 0b0011; // 3 (19 ..)
	public final static byte INT_64 = 0b0100; // 4 (20 ..)
	static long[] lookupValue;
	static int[] lookupIntValue;
	public static ObjectToBeSerialised[] arrayOfObjects;
	public static boolean getSize = false;

	private static ObjectToBeSerialised ITEM;
	
	public static void runMultiHashMap(int rowCount) throws SecurityException, Exception {
		@SuppressWarnings("unused")
		UnSafeMultiHashMap multiMap = new UnSafeMultiHashMap(8, 8, rowCount, 0.70d);
		
		HashMap<Long, Long> javaHashMap = new HashMap<Long, Long>(rowCount);

		long startTime = System.nanoTime();
		for (long i = 0; i < rowCount; i++) {
			multiMap.put(lookupValue[(int)i], lookupValue[(int)i] + 10000000);
		}
		System.out.println("UnSafeMultiHashMap inserted "+ rowCount + " @ \t"
				+ (int)(rowCount / 1000 / ((System.nanoTime() - startTime) / 1000000000.0)) 
				+ " KRows/sec, in " + ((System.nanoTime() - startTime) / 1000000000.0));

		startTime = System.nanoTime();
		for (long i = 0; i < rowCount; i++) {
			javaHashMap.put(lookupValue[(int)i], lookupValue[(int)i] + 10000000);
		}
		System.out.println("Java Hash Map inserted "+ rowCount + " @ \t"
				+ (int)(rowCount / 1000 / ((System.nanoTime() - startTime) / 1000000000.0)) 
				+ " KRows/sec, in " + ((System.nanoTime() - startTime) / 1000000000.0));

		if (multiMap.putProbeCount > 0) {
			System.out.println("Total probe for set : " + multiMap.putProbeCount + " average per lookup " + (double)((double)multiMap.putProbeCount / (double)rowCount) );
		}

		startTime = System.nanoTime();
		long unsafeMatch = 0;
		long javaMatch = 0;
		for (long i = 0; i < rowCount; i++) {
			if (multiMap.get(lookupValue[(int)i]) != -1)
				unsafeMatch++;
		}
		System.out.println("UnSafeMultiHashMap get "+ rowCount + " @ \t"
				+ (int)(rowCount / 1000 / ((System.nanoTime() - startTime) / 1000000000.0)) 
				+ " KRows/sec, in " + ((System.nanoTime() - startTime) / 1000000000.0) + " qualified " + unsafeMatch);

		startTime = System.nanoTime();
		for (long i = 0; i < rowCount; i++) {
			if (javaHashMap.get(lookupValue[(int)i]) != -1)
				javaMatch++;
		}
		System.out.println("Java Hash Map get "+ rowCount + " @ \t"
				+ (int)(rowCount / 1000 / ((System.nanoTime() - startTime) / 1000000000.0)) 
				+ " KRows/sec, in " + ((System.nanoTime() - startTime) / 1000000000.0) + " qualified " + javaMatch);
		
		if (multiMap.putProbeCount > 0) {
			System.out.println("Total probe for get : " + multiMap.getProbeCount + " average per lookup " + (double)((double)multiMap.getProbeCount / (double)rowCount) );
		}


		//multiMap.get(17);

		//multiMap.get(28);
		multiMap.releaseMemory();
	}
	
	public static void testMultiHashMap(int rowCount) throws SecurityException, Exception {
		@SuppressWarnings("unused")
		UnSafeMultiHashMap multiMap = new UnSafeMultiHashMap(8, 8, rowCount, 0.99d);

		for (long i = 0; i < rowCount * 2; i++) {
			multiMap.put(i, i + 10000000);
		}

		if (multiMap.putProbeCount > 0) {
			System.out.println("Total probe count : " + multiMap.putProbeCount + " average per lookup " + (double)((double)multiMap.putProbeCount / (double)rowCount) );
		}
		
		long reValue;
		for (long i = 0; i < 20; i++) {
			reValue = multiMap.get(i);
		}
		
		//multiMap.get(17);

		//multiMap.get(28);
		multiMap.releaseMemory();
	}
	
	public static void measureMemoryPerformance (int loopCount, int dataSizeMultiplier) {
		
		// Memory size
		int arraySize = Integer.MAX_VALUE / 16384 / 64 *   (1 << dataSizeMultiplier);
		
		long bytes = arraySize * UnsafeMemory.SIZE_OF_LONG;
		double mBytes = (double)((double)bytes / 1024 / 1024);
		double totalDataSize = (double) ((double)(loopCount * bytes) / 1024d / 1024d);
		
		// How much memory to read in total
		// All tests should finish in the same time
		long targetMemorySizeMB = 20 * 1024 ;
		
		//System.out.println("Allocated memory size : " + bytes + " Bytes");
		loopCount = loopCount * (int) Math.ceil(targetMemorySizeMB/totalDataSize);
		totalDataSize = loopCount * bytes / 1024 / 1024;
		//System.out.println("Final touched memory size : " + totalDataSize + " MB");
		
		UnsafeMemory unsafeMemory = UnsafeMemory.prepMemoryRWTest(arraySize);
		long baseValue = 36028796616310785l;
		
		lookupIntValue  = new int[arraySize];
		Random rand = new Random();
		for (int i = 0; i < arraySize; i++) {
			lookupIntValue[(int)i] = rand.nextInt(arraySize);
		}
		
		long startTime = System.nanoTime();
		for (int i = 0; i < loopCount; i++) {
			unsafeMemory.memoryWriteTest(arraySize, baseValue);
		}
		System.out.println(loopCount+ ","+ "Unsafe," + totalDataSize + "," + bytes + "," + "W" + ","  + "\t"
				+ (int)((totalDataSize) / ((System.nanoTime() - startTime) / 1000000000.0)) 
				+ " MB/sec," + ((System.nanoTime() - startTime) / 1000000000.0));
		
		startTime = System.nanoTime();
		for (int i = 0; i < loopCount; i++) {
			unsafeMemory.memoryWriteTestRandom(arraySize, baseValue, lookupIntValue);
		}

		System.out.println(loopCount+ ","+ "Unsafe," + totalDataSize + "," + bytes + "," + "WR" + ","  + "\t"
				+ (int)((totalDataSize) / ((System.nanoTime() - startTime) / 1000000000.0)) 
				+ " MB/sec," + ((System.nanoTime() - startTime) / 1000000000.0));

		startTime = System.nanoTime();
		for (int i = 0; i < loopCount; i++) {
			unsafeMemory.memoryReadTest(arraySize);
		}
		System.out.println(loopCount+ ","+ "Unsafe," + totalDataSize + "," + bytes + "," + "R" + "," + "\t" 
				+ (int)((totalDataSize) / ((System.nanoTime() - startTime) / 1000000000.0)) 
				+ " MB/sec," + ((System.nanoTime() - startTime) / 1000000000.0));
		
		startTime = System.nanoTime();
		for (int i = 0; i < loopCount; i++) {
			unsafeMemory.memoryReadTestRandom(arraySize, lookupIntValue);
		}
		System.out.println(loopCount+ ","+ "Unsafe," + totalDataSize + "," + bytes + "," + "RR" + ","  + "\t"
				+ (int)((totalDataSize) / ((System.nanoTime() - startTime) / 1000000000.0)) 
				+ " MB/sec," + ((System.nanoTime() - startTime) / 1000000000.0));
		
		unsafeMemory.releaseMemory();
		
		// Base java 
		
		long[] intArray = new long[arraySize];
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		startTime = System.nanoTime();
		for (int i = 0; i < loopCount; i++) {
			for (int j = 0; j < arraySize; j++) {
				intArray[j] = baseValue + j;
			}
		}
		System.out.println(loopCount+ ","+ "Java," + totalDataSize + "," + bytes + "," + "W" + ","  + "\t"
				+ (int)((totalDataSize) / ((System.nanoTime() - startTime) / 1000000000.0)) 
				+ " MB/sec," + ((System.nanoTime() - startTime) / 1000000000.0));

		startTime = System.nanoTime();
		for (int i = 0; i < loopCount; i++) {
			for (int j = 0; j < arraySize; j++) {
				intArray[lookupIntValue[j]] = baseValue + j;
			}
		}
		System.out.println(loopCount+ ","+ "Java," + totalDataSize + "," + bytes + "," + "WR" + ","  + "\t"
				+ (int)((totalDataSize) / ((System.nanoTime() - startTime) / 1000000000.0)) 
				+ " MB/sec," + ((System.nanoTime() - startTime) / 1000000000.0));
		
		startTime = System.nanoTime();
		long sum = 0;
		for (int i = 0; i < loopCount; i++) {
			for (int j = 0; j < arraySize; j++) {
				sum += intArray[j] + i;
			}
			//System.out.println("Sum from unsafe : " + sum);
		}
		System.out.println(loopCount+ ","+ "Java," + totalDataSize + "," + bytes + "," + "R" + ","  + "\t"
				+ (int)((totalDataSize) / ((System.nanoTime() - startTime) / 1000000000.0)) 
				+ " MB/sec," + ((System.nanoTime() - startTime) / 1000000000.0) + "," + sum);
		
		startTime = System.nanoTime();
		sum = 0;
		for (int i = 0; i < loopCount; i++) {
			for (int j = 0; j < arraySize; j++) {
				sum += intArray[lookupIntValue[j]] + i;
			}
			//System.out.println("Sum from unsafe : " + sum);
		}
		System.out.println(loopCount+ ","+ "Java," + totalDataSize + "," + bytes + "," + "RR" + ","  + "\t"
				+ (int)((totalDataSize) / ((System.nanoTime() - startTime) / 1000000000.0)) 
				+ " MB/sec," + ((System.nanoTime() - startTime) / 1000000000.0) + "," + sum);

	}
	
	public static void measureBitArrayBase(long count) {
		BitArray bitArray = new BitArray(count);
		bitArray.setUnSafe(10);
		for (long i = 0; i < count; i++) {
			bitArray.set(i);
		}
		
		for (long i = 0; i < count; i++) {
			bitArray.get(i);
		}
	}

	public static void measureLongToInCostAutoBoxing(int loopCount) {
		
		int maxValue = Integer.MAX_VALUE ;
		long baseValue;
		int val1 , val2 , val3 = 0;
		
		long startTime = System.nanoTime();
		for (int j = 0 ; j < loopCount ; j++) {
			for (long i = 0; i < loopCount; i++) {
				val1 = (int) i;
				val2 = (int) (i >>> 32);
				val3 = (int) (i + i);
				baseValue = val1 + val2 + val3;
			}
		}
		System.out
		.println("\n-- "
				+ maxValue
				+ " Autoboxing "
				+ ((System.nanoTime() - startTime) / 1000000000.0));
	}

	
	public static void measureIntToLongCostAutoBoxing(int loopCount) {
		
		int maxValue = Integer.MAX_VALUE ;
		long baseValue;
		Long val1 , val2 , val3 = 0l;
		
		long startTime = System.nanoTime();
		for (int j = 0 ; j < loopCount ; j++) {
			for (int i = 0; i < loopCount; i++) {
				val1 = (long) i;
				val2 = (long) (i >>> 32);
				val3 = (long) (i + i);
				baseValue = val1 + val2 + val3;
			}
		}
		System.out
		.println("\n-- "
				+ maxValue
				+ " Autoboxing "
				+ ((System.nanoTime() - startTime) / 1000000000.0));
	}
	
	public static void measureIntToLongCostUnSafe(int loopCount) {
		
		long maxValue = (long) Integer.MAX_VALUE ;
		long baseValue;
		Long val1 , val2 , val3 = 0l;
		UnSafeTypeConverter ustco = new UnSafeTypeConverter();
			
		
		long startTime = System.nanoTime();
		
		for (int j = 0 ; j < loopCount ; j++) {
			for (int i = 0; i < loopCount; i++) {
				val1 = ustco.LongFromInt(i);
				val2 = ustco.LongFromInt(i >>> 32);
				val3 = ustco.LongFromInt(i + i);
				baseValue = val1 + val2 + val3;
			}
		}
		System.out
		.println("\n-- "
				+ maxValue
				+ " UnSafe conversion "
				+ ((System.nanoTime() - startTime) / 1000000000.0));
	}

	public static void testJavaHashTable (long count) {
		//Hashtable<Long, Long>  hashTable = new Hashtable<Long, Long>((int) 2);
		TLongLongHashMap tlonglongMap = new TLongLongHashMap(2); 
		HashMultimap<Long,Long> hmm = HashMultimap.create(1, 1);
		
		for (long i = 0; i < count ; i++) {
			//	hashTable.put(i, i);
			//	hashTable.put(i, i + 900);
			hmm.put(i, i);
			hmm.put(i, i + 900);
			tlonglongMap.put(i,i);
			tlonglongMap.get(i+ 10);
			tlonglongMap.put(i+ 10,i + 900);
			
		}

		Object retValue;
		Set<Long> results;
		for (long i = 0; i < count; i++) {
			//	retValue = hashTable.get(i);
			tlonglongMap.get(i);
			results = hmm.get(i);
			System.out.println(results.toString());
		}
		
	}
	
	public static void measureBitArrayUnSafe(long count) {
		BitArray bitArray = new BitArray(count);
		bitArray.setUnSafe(10);
		
		for (long i = 0; i < count; i++) {
			bitArray.setUnSafe(i);
		}
		
		for (long i = 0; i < count; i++) {
			bitArray.getUnsafe(i);
		}
	}
	
	public static void runUnSafeHashmapTest(int keys, int lookups) throws Exception {

		UnSafeHashMap jIntIntMap = UnSafeHashMap
				.CreateUnSafeHashMap(8, 8, keys * 10);
		
		//jIntIntMap.testReflection();

		for (long i = 0; i < keys; i++) {
			jIntIntMap.put(i, i);
		}

		boolean matchFound = false;
		for (long i = 0; i < lookups; i++) {
			matchFound = jIntIntMap.containsKey(i);
			if (!matchFound) {
				System.out.println("Something went wrong" + i);
			}
		}
		
		
		UnSafeHashMap longObjHm = UnSafeHashMap.CreateUnSafeHashMap(8,
				ObjectToBeSerialised.getSize(), keys * 20);
		
		for (long i = 0; i < keys; i++) {
			// jIntIntMap.put(i, i);
			longObjHm.putRowContainer(i, arrayOfObjects[(int) i]);
		}

		ObjectToBeSerialised obj = null;
		for (int i = 0; i < 10; i++) {
			obj = longObjHm.getRowContainer((long)i);
		}
		
	}
	
	
	public static void runHopScotchHashMapTest(int keys, int lookups) {
		ConcurrentHopscotchHashMap<Long, Long> jIntIntMap =  new ConcurrentHopscotchHashMap<Long,Long>(keys, 1);

		for (long i = 0; i < keys; i++) {
			jIntIntMap.put(i, i);
		}

		boolean matchFound = false;
		for (int i = 0; i < lookups; i++) {
			matchFound = jIntIntMap.containsKey((long)i);
			if (!matchFound) {
				System.out.println("Something went wrong, check : " + i);
			}
		}
	}
	
	public static void runHopScotchHashMap(int keys, int lookups) {
		long startTime = System.nanoTime();
		long startHeapSize = Runtime.getRuntime().freeMemory();

		// BEGIN: benchmark for Java's built-in hashmap
		System.out.println("\n===== HopScotchHash HashMap =====");
		ConcurrentHopscotchHashMap<Long, ObjectToBeSerialised> jIntIntMap =  new ConcurrentHopscotchHashMap<Long,ObjectToBeSerialised>(keys, 1);

		startTime = System.nanoTime();
		for (long i = 0; i < keys; i++) {
			// jIntIntMap.put(i, ITEM);
			jIntIntMap.put(i, arrayOfObjects[(int) i]);
		}

		System.out
				.println("\n-- "
						+ keys
						+ " puts(key, value)  runtime "
						+ ((System.nanoTime() - startTime) / 1000000000.0)
						+ " Heap Size "
						+ ((startHeapSize - Runtime.getRuntime().freeMemory()) / 1048576.0));

		if (getSize)
			System.out.println("Hash Map size : "
					+ SizeOf.deepSizeOf(jIntIntMap) / 1024 + " KB" + "\n");

		startTime = System.nanoTime();
		for (int i = 0; i < lookups; i++) {
			jIntIntMap.get(lookupValue[i]);
		}
		System.out
				.println("\n-- "
						+ lookups
						+ " get(key)  runtime "
						+ ((System.nanoTime() - startTime) / 1000000000.0)
						+ " Heap Size "
						+ ((startHeapSize - Runtime.getRuntime().freeMemory()) / 1048576.0));

		startTime = System.nanoTime();
		for (int i = 0; i < lookups; i++) {
			jIntIntMap.containsKey(lookupValue[i]);
		}
		System.out
				.println("\n-- "
						+ lookups
						+ " Contains(key)  runtime "
						+ ((System.nanoTime() - startTime) / 1000000000.0)
						+ " Heap Size "
						+ ((startHeapSize - Runtime.getRuntime().freeMemory()) / 1048576.0));
	}

	public static void runUnSafeHashmap(int keys, int lookups) {
		long startTime = System.nanoTime();
		long startHeapSize = Runtime.getRuntime().freeMemory();

		// BEGIN: benchmark for Unsafe built-in hashmap
		System.out.println("\n===== UnSafeHashMap =====");
		// HashMap jIntIntMap = new HashMap(n);
		// UnSafeHashMap jIntIntMap = UnSafeHashMap.CreateUnSafeHashMap (8, 8,
		// keys);

		UnSafeHashMap jIntIntMap = UnSafeHashMap.CreateUnSafeHashMap(8,
				ObjectToBeSerialised.getSize(), keys);

		// ObjectToBeSerialised getItem;
		// jIntIntMap.putRowContainer(10, ITEM);
		// getItem = jIntIntMap.getRowContainer(10);
		// if (getItem.equals(ITEM)) {
		// System.out.print(getItem.toString());
		// }

		// byte[] byteArray = new byte[] { 0, 1, 2, 3, 4, 5,6,7,8,9,10 };
		// byte[] byteArrayResults = new byte[(byteArray.length)];
		// jIntIntMap.putBytes(10, byteArray);
		// jIntIntMap.getBytes(10, byteArrayResults);

		startTime = System.nanoTime();
		for (long i = 0; i < keys; i++) {
			// jIntIntMap.put(i, i);
			jIntIntMap.putRowContainer(i, arrayOfObjects[(int) i]);
		}

		System.out
				.println("\n-- "
						+ keys
						+ " puts(key, value)  runtime "
						+ ((System.nanoTime() - startTime) / 1000000000.0)
						+ " Heap Size "
						+ ((startHeapSize - Runtime.getRuntime().freeMemory()) / 1048576.0));

		if (getSize)
			System.out.println("Hash Map size : "
					+ SizeOf.deepSizeOf(jIntIntMap) / 1024 + " KB" + "\n");

		startTime = System.nanoTime();
		for (int i = 0; i < lookups; i++) {
			jIntIntMap.getRowContainer(lookupValue[i]);
		}
		System.out
				.println("\n-- "
						+ lookups
						+ " get(key)  runtime "
						+ ((System.nanoTime() - startTime) / 1000000000.0)
						+ " Heap Size "
						+ ((startHeapSize - Runtime.getRuntime().freeMemory()) / 1048576.0));

		startTime = System.nanoTime();
		int matchRows = 0;
		for (int i = 0; i < lookups; i++) {
			jIntIntMap.containsKey(lookupValue[i]);
		}

		System.out
				.println("\n-- "
						+ lookups
						+ " Contains(key)  runtime "
						+ ((System.nanoTime() - startTime) / 1000000000.0)
						+ " Heap Size "
						+ ((startHeapSize - Runtime.getRuntime().freeMemory()) / 1048576.0)
						+ " matches " + matchRows);
	}

	public static void runJavaHashmap(int keys, int lookups) {
		long startTime = System.nanoTime();
		long startHeapSize = Runtime.getRuntime().freeMemory();

		// BEGIN: benchmark for Java's built-in hashmap
		System.out.println("\n===== Java's built-in HashMap =====");
		HashMap<Long, ObjectToBeSerialised> jIntIntMap = new HashMap<Long, ObjectToBeSerialised>(
				keys);

		startTime = System.nanoTime();
		for (long i = 0; i < keys; i++) {
			// jIntIntMap.put(i, ITEM);
			jIntIntMap.put(i, arrayOfObjects[(int) i]);
		}

		System.out
				.println("\n-- "
						+ keys
						+ " puts(key, value)  runtime "
						+ ((System.nanoTime() - startTime) / 1000000000.0)
						+ " Heap Size "
						+ ((startHeapSize - Runtime.getRuntime().freeMemory()) / 1048576.0));

		if (getSize)
			System.out.println("Hash Map size : "
					+ SizeOf.deepSizeOf(jIntIntMap) / 1024 + " KB" + "\n");

		startTime = System.nanoTime();
		for (int i = 0; i < lookups; i++) {
			jIntIntMap.get(lookupValue[i]);
		}
		System.out
				.println("\n-- "
						+ lookups
						+ " get(key)  runtime "
						+ ((System.nanoTime() - startTime) / 1000000000.0)
						+ " Heap Size "
						+ ((startHeapSize - Runtime.getRuntime().freeMemory()) / 1048576.0));

		startTime = System.nanoTime();
		for (int i = 0; i < lookups; i++) {
			jIntIntMap.containsKey(lookupValue[i]);
		}
		System.out
				.println("\n-- "
						+ lookups
						+ " Contains(key)  runtime "
						+ ((System.nanoTime() - startTime) / 1000000000.0)
						+ " Heap Size "
						+ ((startHeapSize - Runtime.getRuntime().freeMemory()) / 1048576.0));
	}

	public static void runChronicleMapmap(int keys, int lookups) {
		long startTime = System.nanoTime();
		long startHeapSize = Runtime.getRuntime().freeMemory();

		// BEGIN: benchmark for ChronicleMap built-in hashmap
		System.out.println("\n===== ChronicleMap's built-in HashMap =====");
		// HashMap jIntIntMap = new HashMap(n);

		ChronicleMapBuilder<Long, Long> builder = ChronicleMapBuilder.of(
				Long.class, Long.class);
		ChronicleMap<Long, Long> chm = builder.create();

		startTime = System.nanoTime();
		for (long i = 0; i < keys; i++) {
			// jIntIntMap.put(i, new float[] { 0f, 1f, 2f, 3f, 4f });
			chm.put(i, i);
		}

		System.out
				.println("\n-- "
						+ keys
						+ " puts(key, value)  runtime "
						+ ((System.nanoTime() - startTime) / 1000000000.0)
						+ " Heap Size "
						+ ((startHeapSize - Runtime.getRuntime().freeMemory()) / 1048576.0));

		startTime = System.nanoTime();
		for (int i = 0; i < lookups; i++) {
			chm.get(lookupValue[i]);
		}
		System.out
				.println("\n-- "
						+ lookups
						+ " get(key)  runtime "
						+ ((System.nanoTime() - startTime) / 1000000000.0)
						+ " Heap Size "
						+ ((startHeapSize - Runtime.getRuntime().freeMemory()) / 1048576.0));

		startTime = System.nanoTime();
		for (int i = 0; i < lookups; i++) {
			chm.containsKey(lookupValue[i]);
		}
		System.out
				.println("\n-- "
						+ lookups
						+ " Contains(key)  runtime "
						+ ((System.nanoTime() - startTime) / 1000000000.0)
						+ " Heap Size "
						+ ((startHeapSize - Runtime.getRuntime().freeMemory()) / 1048576.0));
	}

	public static void runNonBlockingHashMap(int keys, int lookups) {
		long startTime = System.nanoTime();
		long startHeapSize = Runtime.getRuntime().freeMemory();

		// BEGIN: benchmark for Java's built-in hashmap
		System.out.println("\n===== NonBlockingHashMap HashMap =====");
		// HashMap jIntIntMap = new HashMap(n);
		NonBlockingHashMap<Long, Long> jIntIntMap = new NonBlockingHashMap<>(
				keys);

		startTime = System.nanoTime();
		for (long i = 0; i < keys; i++) {
			jIntIntMap.put(i, i);
		}

		System.out
				.println("\n-- "
						+ keys
						+ " puts(key, value)  runtime "
						+ ((System.nanoTime() - startTime) / 1000000000.0)
						+ " Heap Size "
						+ ((startHeapSize - Runtime.getRuntime().freeMemory()) / 1048576.0));

		startTime = System.nanoTime();
		for (int i = 0; i < lookups; i++) {
			jIntIntMap.get(lookupValue[i]);
		}
		System.out
				.println("\n-- "
						+ lookups
						+ " get(key)  runtime "
						+ ((System.nanoTime() - startTime) / 1000000000.0)
						+ " Heap Size "
						+ ((startHeapSize - Runtime.getRuntime().freeMemory()) / 1048576.0));

		startTime = System.nanoTime();
		for (int i = 0; i < lookups; i++) {
			jIntIntMap.containsKey(lookupValue[i]);
		}
		System.out
				.println("\n-- "
						+ lookups
						+ " Contains(key)  runtime "
						+ ((System.nanoTime() - startTime) / 1000000000.0)
						+ " Heap Size "
						+ ((startHeapSize - Runtime.getRuntime().freeMemory()) / 1048576.0));
	}

	public static void runTroveHashMap(int keys, int lookups) {
		long startTime = System.nanoTime();
		long startHeapSize = Runtime.getRuntime().freeMemory();

		// BEGIN: benchmark for Trove's TIntIntHashMap
		System.out.println("\n===== Trove's TIntIntHashMap =====");

		// TLongLongHashMap tIntIntMap = new TLongLongHashMap(keys);
		TLongObjectHashMap<ObjectToBeSerialised> tIntIntMap = new TLongObjectHashMap<ObjectToBeSerialised>(
				keys);

		/*
		 * tIntIntMap.put(10, 10); tIntIntMap.put(11, 10); tIntIntMap.put(12,
		 * 10); tIntIntMap.put(13, 10); tIntIntMap.put(20, 10);
		 * tIntIntMap.put(21, 10); tIntIntMap.put(22, 10);
		 */

		startTime = System.nanoTime();
		for (long i = 0; i < keys; i++) {
			// tIntIntMap.put(i, ITEM);
			tIntIntMap.put(i, arrayOfObjects[(int) i]);
		}
		System.out
				.println("\n-- "
						+ keys
						+ " puts(key, value)  runtime "
						+ ((System.nanoTime() - startTime) / 1000000000.0)
						+ " Heap Size "
						+ ((startHeapSize - Runtime.getRuntime().freeMemory()) / 1048576.0));

		if (getSize)
			System.out.println("Hash Map size : "
					+ SizeOf.deepSizeOf(tIntIntMap) / 1024 + " KB" + "\n");

		startTime = System.nanoTime();
		for (int i = 0; i < lookups; i++) {
			tIntIntMap.get(lookupValue[i]);
		}
		System.out
				.println("\n-- "
						+ lookups
						+ " get(key)  runtime "
						+ ((System.nanoTime() - startTime) / 1000000000.0)
						+ " Heap Size "
						+ ((startHeapSize - Runtime.getRuntime().freeMemory()) / 1048576.0));

		startTime = System.nanoTime();
		for (int i = 0; i < lookups; i++) {
			tIntIntMap.containsKey(lookupValue[i]);
		}
		System.out
				.println("\n-- "
						+ lookups
						+ " Contains(key)  runtime "
						+ ((System.nanoTime() - startTime) / 1000000000.0)
						+ " Heap Size "
						+ ((startHeapSize - Runtime.getRuntime().freeMemory()) / 1048576.0));
	}

	public static void runColtHashmap(int keys, int lookups) {

		long startTime = System.nanoTime();
		long startHeapSize = Runtime.getRuntime().freeMemory();

		// BEGIN: benchmark for Colt's OpenIntIntHashMap
		System.out.println("\n===== Colt's OpenIntIntHashMap =====");
		OpenLongObjectHashMap cLongLongMap = new OpenLongObjectHashMap(keys);

		startTime = System.nanoTime();
		for (long i = 0; i < keys; i++) {
			cLongLongMap.put(i, i);
		}

		System.out
				.println("\n-- "
						+ keys
						+ " puts(key, value)  runtime "
						+ ((System.nanoTime() - startTime) / 1000000000.0)
						+ " Heap Size "
						+ ((startHeapSize - Runtime.getRuntime().freeMemory()) / 1048576.0));

		startTime = System.nanoTime();
		for (int i = 0; i < lookups; i++) {
			cLongLongMap.get(lookupValue[i]);
		}
		System.out
				.println("\n-- "
						+ lookups
						+ " get(key)  runtime "
						+ ((System.nanoTime() - startTime) / 1000000000.0)
						+ " Heap Size "
						+ ((startHeapSize - Runtime.getRuntime().freeMemory()) / 1048576.0));

		startTime = System.nanoTime();
		for (int i = 0; i < lookups; i++) {
			cLongLongMap.containsKey(lookupValue[i]);
		}
		System.out
				.println("\n-- "
						+ lookups
						+ " Contains(key)  runtime "
						+ ((System.nanoTime() - startTime) / 1000000000.0)
						+ " Heap Size "
						+ ((startHeapSize - Runtime.getRuntime().freeMemory()) / 1048576.0));
	}

	public static void bytesFromStrin() {
		String foo = "0123345678abcde";
		int stringArraySize = 1024;
		int loopCount = 1024;

		String[] stringArray = new String[stringArraySize];
		for (int i = 0; i < stringArraySize; i++) {
			stringArray[i] = foo + i;
		}

		byte[] bytesFromString;
		long startTime = System.nanoTime();
		for (long i = 0; i < loopCount; i++) {
			for (int j = 0; j < stringArraySize; j++) {
				bytesFromString = stringArray[j].getBytes();
			}
		}
		long endTime = System.nanoTime();
		System.out.println("From getBytes :" + (endTime - startTime)
				/ 1000000000.0);

		byte[] bytesFromChars = null;
		int stringLength;
		startTime = System.nanoTime();
		for (long i = 0; i < loopCount; i++) {
			for (int j = 0; j < stringArraySize; j++) {
				stringLength = stringArray[j].length();
				bytesFromChars = new byte[stringLength];
				bytesFromString = stringArray[j].getBytes();

				for (int k = 0; k < stringLength; k++) {
					bytesFromChars[k] = (byte) stringArray[j].charAt(k);
				}

			}
		}
		endTime = System.nanoTime();
		System.out.println("From chars :" + (endTime - startTime)
				/ 1000000000.0);

	}

	  static boolean InterlockedDecrementNotZero(AtomicInteger refCount)
	  {
	      int comperand;
	      int exchange;
	      do {
	          comperand = refCount.get();
	          exchange = comperand-1;
	          if (comperand <= 0) {
	              return false;
	          }
	      } while (refCount.compareAndSet(comperand, exchange));
	      return true;
	  }
	
	public static void main(String args[]) throws Exception {
		
		int loopCount = 10 * 2;
		int keyCount = 8000000;
		int lookupsCount = 8000000;

		//measureLongToInCostAutoBoxing(10000);
		//measureIntToLongCostAutoBoxing(10000);
		//measureIntToLongCostUnSafe(10000);
		getSize = false;
		lookupValue = new long[lookupsCount];
		lookupIntValue  = new int[lookupsCount];
		arrayOfObjects = new ObjectToBeSerialised[keyCount];

		Random rand = new Random();
		for (long i = 0; i < lookupsCount; i++) {
			lookupValue[(int)i] = (long) rand.nextInt(keyCount);
		}

		for (long i = 0; i < loopCount; i++) {
			runMultiHashMap(keyCount);
			System.gc();
		}
		
		//testMultiHashMap(10);
		
		if (loopCount == loopCount)
			return ;
		

		
		System.gc();
		Thread.sleep(1 * 1000);
		for (int i = 1; i < 24; i++) {
			measureMemoryPerformance(1,i);
		}

		
		for (int i = 0; i < keyCount; i++) {
			arrayOfObjects[i] = new ObjectToBeSerialised(i, true,
					((int) i) + 777, ((int) i) + 99, new byte[] { 1, 2, 3, 4,
							5, 6, 7, 8, 9, 10 });
		}

		//runHopScotchHashMapTest(100, 100);
		//runUnSafeHashmapTest(100, 100);
		//testJavaHashTable(10);

		System.gc();
		Thread.sleep(5000);
		
		for (long i = 0; i < loopCount; i++) {
			runJavaHashmap(keyCount, lookupsCount);
		}

		//for (long i = 0; i < loopCount; i++) {
			//	runHopScotchHashMap(keyCount, lookupsCount);
		//}

		System.gc();

		for (long i = 0; i < loopCount; i++) {
			runTroveHashMap(keyCount, lookupsCount);
		}


		for (long i = 0; i < loopCount; i++) {
			runUnSafeHashmap(keyCount, lookupsCount);
			// runUnSafeHashmap(15000, lookupsCount);
			// System.gc();
			// runChronicleMapmap(n);
			// System.gc();
			// runJavaHashmap(n);
			// System.gc();
			// runTroveHashMap(n);
			// System.gc();
			// runColtHashmap(n);
			// System.gc();
			// runNonBlockingHashMap(n);
		}
		System.gc();
		
		// Thread.sleep(30000);

	}

}