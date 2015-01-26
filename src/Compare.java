import gnu.trove.impl.hash.TLongLongHash;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedList;
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
	static long[] keyValues;
	static int[] lookupIntValue;
	public static ObjectToBeSerialised[] arrayOfObjects;
	public static boolean getSize = false;

	private static ObjectToBeSerialised ITEM;
	
	public static void runMultiHashMap(int keyCount) throws SecurityException, Exception {
		@SuppressWarnings("unused")

		UnSafeMultiHashMap multiMap = new UnSafeMultiHashMap(8, 8, keyCount, 0.7d);
		HashMap<Long, Long> javaHashMap = new HashMap<Long, Long>(keyCount, (float)0.7);
		TLongLongHashMap troveHashMap = new TLongLongHashMap(keyCount, (float)0.7);
		HashMultimap<Long,Long> javaMultiMap = HashMultimap.create(keyCount, 1);
		
		long startTime = System.nanoTime();
		
		for (long i = 0; i < keyCount; i++) {
			multiMap.put(keyValues[(int)i], keyValues[(int)i] + 10000000);
		}
		System.out.println("UnSafeMultiHashMap inserted "+ keyCount + " @ \t\t\t"
				+ (int)(keyCount / 1000 / ((System.nanoTime() - startTime) / 1000000000.0)) 
				+ " KRows/sec, in " + ((System.nanoTime() - startTime) / 1000000000.0));

		startTime = System.nanoTime();
		for (long i = 0; i < keyCount; i++) {
			javaHashMap.put(keyValues[(int)i], keyValues[(int)i] + 10000000);
		}
		System.out.println("Java Hash Map inserted "+ keyCount + " @ \t\t\t"
				+ (int)(keyCount / 1000 / ((System.nanoTime() - startTime) / 1000000000.0)) 
				+ " KRows/sec, in " + ((System.nanoTime() - startTime) / 1000000000.0));
		
		startTime = System.nanoTime();
		for (long i = 0; i < keyCount; i++) {
			troveHashMap.put(keyValues[(int)i], keyValues[(int)i] + 10000000);
		}
		System.out.println("Trove Hash Map inserted "+ keyCount + " @ \t\t\t"
				+ (int)(keyCount / 1000 / ((System.nanoTime() - startTime) / 1000000000.0)) 
				+ " KRows/sec, in " + ((System.nanoTime() - startTime) / 1000000000.0));
		
		startTime = System.nanoTime();
		for (long i = 0; i < keyCount; i++) {
			javaMultiMap.put(keyValues[(int)i], keyValues[(int)i] + 10000000);
		}
		System.out.println("Java Multi Map inserted "+ keyCount + " @ \t\t\t"
				+ (int)(keyCount / 1000 / ((System.nanoTime() - startTime) / 1000000000.0)) 
				+ " KRows/sec, in " + ((System.nanoTime() - startTime) / 1000000000.0));

		if (multiMap.putProbeCount > 0) {
			System.out.println("Total probe for set : " + multiMap.putProbeCount + " average per lookup " + (double)((double)multiMap.putProbeCount / (double)keyCount) );
		}

		long unsafeMatch = 0;
		long javaMatch = 0;
		long troveMatch = 0;
		long javaMultiMatch = 0;

		startTime = System.nanoTime();
		for (long i = 0; i < keyCount; i++) {
			if (multiMap.get(keyValues[(int)i]) != -1)
				unsafeMatch++;
		}
		System.out.println("UnSafeMultiHashMap get "+ keyCount + " @ \t\t\t"
				+ (int)(keyCount / 1000 / ((System.nanoTime() - startTime) / 1000000000.0)) 
				+ " KRows/sec, in " + ((System.nanoTime() - startTime) / 1000000000.0) + " qualified " + unsafeMatch);

		startTime = System.nanoTime();
		for (long i = 0; i < keyCount; i++) {
			if (javaHashMap.get(keyValues[(int)i]) != -1)
				javaMatch++;
		}
		System.out.println("Java Hash Map get "+ keyCount + " @ \t\t\t"
				+ (int)(keyCount / 1000 / ((System.nanoTime() - startTime) / 1000000000.0)) 
				+ " KRows/sec, in " + ((System.nanoTime() - startTime) / 1000000000.0) + " qualified " + javaMatch);
		
		startTime = System.nanoTime();
		for (long i = 0; i < keyCount; i++) {
			if (troveHashMap.get(keyValues[(int)i]) != -1)
				troveMatch++;
		}
		System.out.println("Trove Hash Map get "+ keyCount + " @ \t\t\t"
				+ (int)(keyCount / 1000 / ((System.nanoTime() - startTime) / 1000000000.0)) 
				+ " KRows/sec, in " + ((System.nanoTime() - startTime) / 1000000000.0) + " qualified " + troveMatch);

		startTime = System.nanoTime();
		for (long i = 0; i < keyCount; i++) {
			if (javaMultiMap.get(keyValues[(int)i]) != null)
				javaMultiMatch++;
		}
		System.out.println("MultiMap Hash Map get "+ keyCount + " @ \t\t\t"
				+ (int)(keyCount / 1000 / ((System.nanoTime() - startTime) / 1000000000.0)) 
				+ " KRows/sec, in " + ((System.nanoTime() - startTime) / 1000000000.0) + " qualified " + javaMultiMatch);
		
		
		if (multiMap.softProbeCount > 0 || multiMap.hardProbeCount > 0) {
			long totalProbes = multiMap.softProbeCount + multiMap.hardProbeCount;
			System.out.println("Total probe for get : " + totalProbes + " average per lookup " + (double)((double)totalProbes / (double)keyCount) );
			System.out.println("\t Soft probes for get : " + multiMap.softProbeCount + " average per lookup " + (double)((double)multiMap.softProbeCount / (double)keyCount) );
			System.out.println("\t Hard probes for get : " + multiMap.hardProbeCount + " average per lookup " + (double)((double)multiMap.hardProbeCount / (double)keyCount) );
		}

		if (getSize)
			System.out.println("Hash Map size : "
					+ SizeOf.deepSizeOf(javaHashMap) / 1024 + " KB" + "\n");
		
		if (getSize)
			System.out.println("Trove Map size : "
					+ SizeOf.deepSizeOf(troveHashMap) / 1024 + " KB" + "\n");

		if (getSize)
			System.out.println("Java Multi Map size : "
					+ SizeOf.deepSizeOf(javaMultiMap) / 1024 + " KB" + "\n");

		javaHashMap.size();
		troveHashMap.size();
		javaMultiMap.size();
		//multiMap.get(17);

		//multiMap.get(28);
		multiMap.releaseMemory();
	}
	
	public static void testMultiHashMap(int keyCount) throws SecurityException, Exception {
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();

		
		@SuppressWarnings("unused")
		UnSafeMultiHashMap multiMap = new UnSafeMultiHashMap(8, 8, keyCount, 0.9d);

		System.out.println("Start load :" + dateFormat.format(new Date())); //2014/08/06 15:59:48
		long startTime = System.nanoTime();
		for (long i = 0; i < keyCount; i++) {
			multiMap.put(keyValues[(int)i], keyValues[(int)i] + 10000000);
		}
		System.out.println("End load :" + dateFormat.format(new Date())); //2014/08/06 15:59:48
		System.out.println("UnSafeMultiHashMap inserted "+ keyCount + " @ \t\t\t"
				+ (int)(keyCount / 1000 / ((System.nanoTime() - startTime) / 1000000000.0)) 
				+ " KRows/sec, in " + ((System.nanoTime() - startTime) / 1000000000.0));

		if (multiMap.putProbeCount > 0) {
			System.out.println("Total probe for set : " + multiMap.putProbeCount + " average per lookup " + (double)((double)multiMap.putProbeCount / (double)keyCount) );
		}
		
		//Thread.sleep(5000);
		
		startTime = System.nanoTime();
		System.out.println("Start get :" + dateFormat.format(new Date())); //2014/08/06 15:59:48
		int innerLoopCount = 5;
		for (int j = 0; j < innerLoopCount; j ++) {
			for (long i = 0; i < keyCount; i++) {
				multiMap.get(keyValues[(int)i]);
			}
		}
		System.out.println("End get :" + dateFormat.format(new Date())); //2014/08/06 15:59:48
		System.out.println("UnSafeMultiHashMap get "+ keyCount * innerLoopCount + " @ \t\t\t"
				+ (int)(keyCount * innerLoopCount / 1000 / ((System.nanoTime() - startTime) / 1000000000.0)) 
				+ " KRows/sec, in " + ((System.nanoTime() - startTime) / 1000000000.0) + " qualified " );
		
		if (multiMap.softProbeCount > 0 || multiMap.hardProbeCount > 0) {
			long totalProbes = multiMap.softProbeCount + multiMap.hardProbeCount;
			System.out.println("Total probe for get : " + totalProbes + " average per lookup " + (double)((double)totalProbes / (double)keyCount) );
			System.out.println("\t Soft probes for get : " + multiMap.softProbeCount + " average per lookup " + (double)((double)multiMap.softProbeCount / (double)keyCount) );
			System.out.println("\t Hard probes for get : " + multiMap.hardProbeCount + " average per lookup " + (double)((double)multiMap.hardProbeCount / (double)keyCount) );
		}
		
		//long reValue;
		//for (long i = 0; i < 30; i++) {
		//	reValue = multiMap.get(i);
		//}
		
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
			jIntIntMap.get(keyValues[i]);
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
			jIntIntMap.containsKey(keyValues[i]);
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
			jIntIntMap.getRowContainer(keyValues[i]);
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
			jIntIntMap.containsKey(keyValues[i]);
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
			jIntIntMap.get(keyValues[i]);
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
			jIntIntMap.containsKey(keyValues[i]);
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
			chm.get(keyValues[i]);
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
			chm.containsKey(keyValues[i]);
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
			jIntIntMap.get(keyValues[i]);
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
			jIntIntMap.containsKey(keyValues[i]);
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
			tIntIntMap.get(keyValues[i]);
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
			tIntIntMap.containsKey(keyValues[i]);
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
			cLongLongMap.get(keyValues[i]);
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
			cLongLongMap.containsKey(keyValues[i]);
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
	
	  public static  void shuffle(long[] lookupIntValue2) {
          for (int i = lookupIntValue2.length; i > 1; i--) {
                  long temp = lookupIntValue2[i - 1];
                  int randIx = (int) (Math.random() * i);
                  lookupIntValue2[i - 1] = lookupIntValue2[randIx];
                  lookupIntValue2[randIx] = (int)temp;
          }
	  }  
	  
	  public static List<Integer> grayCode(int start, int n) {

		  List<Integer> result = new ArrayList<Integer>();

		  if (n == 0) {
		      result.add(0);
		      return result;
		  }

		  for (int i = start; i < Math.pow(2, n); i++) {
		      result.add(i ^ (i / 2));
		  }

		  return result;
		  }
	  
	  public static ArrayList<Integer> grayCode2(int n) {
		    ArrayList<Integer> arr = new ArrayList<Integer>();
		    arr.add(0);
		    for(int i=0;i<n;i++){
		        int inc = 1<<i;
		        for(int j=arr.size()-1;j>=0;j--){
		            arr.add(arr.get(j)+inc);
		        }
		    }
		    return arr;
		}
	  
	  public static int Ones(int x) {
			x -= ((x >> 1) & 0x55555555);
			x = (((x >> 2) & 0x33333333) + (x & 0x33333333));
			x = (((x >> 4) + x) & 0x0f0f0f0f);
			x += (x >> 8);
			x += (x >> 16);
			return (x & 0x0000003f);
		}
	  
	  static long next_gray(long gray)
	  {
	      if (is_gray_odd(gray))
	      {
	          long y = gray & -gray;
	          return gray ^ (y << 1);
	      }
	      else
	      {
	          // Flip rightmost bit
	          return gray ^ 1;
	      }
	  }
	  
	  static boolean is_gray_odd(long gray)
	  {
		  return (boolean)((Long.bitCount(gray) % 2) == 1);
	  }
	  
	  static long  binaryToGray( long num)
	  {
	          return (num >> 1) ^ num;
	  }
	  
	  static long grayToBinary(long num)
	  {
	      long mask;
	      for (mask = num >> 1; mask != 0; mask = mask >> 1)
	      {
	          num = num ^ mask;
	      }
	      return num;
	  }
	  
	public static void main(String args[]) throws Exception {
		
		int loopCount = 100;
		int keyCount = 16000000;
		int lookupsCount = 1000000;
		
		getSize = false;
		keyValues = new long[keyCount];
		arrayOfObjects = new ObjectToBeSerialised[keyCount];

		Random rand = new Random();
		for (long i = 0; i < keyCount; i++) {
			keyValues[(int)i] = i;
			//lookupValue[(int)i] = ((long) rand.nextInt(keyCount));
		}
		shuffle(keyValues);
		
		//testMultiHashMap(10);
		
		for (long i = 0; i < loopCount; i++) {
			//runMultiHashMap(keyCount);
			testMultiHashMap(keyCount);
			Thread.sleep(2 * 1000);
		}

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