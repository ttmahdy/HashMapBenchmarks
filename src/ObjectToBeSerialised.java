import java.io.Serializable;
import java.util.Arrays;

class ObjectToBeSerialised implements Serializable 
{
    public ObjectToBeSerialised() {
		super();
		this.sourceId = 0;
        this.special = false;
        this.orderCode = 0;
        this.priority = 0;
        this.randomStrings = new byte[] {};
	}

	private static final long serialVersionUID = 10275539472837495L;
 
    private final long sourceId;
    private final boolean special;
    private final int orderCode;
    private final int priority;
    private final byte[] randomStrings;
    
    public static int getSize() {
    	int sizeInBytes = 0;
    	sizeInBytes += UnsafeMemory.SIZE_OF_LONG;
    	sizeInBytes += UnsafeMemory.SIZE_OF_BOOLEAN;
    	sizeInBytes += UnsafeMemory.SIZE_OF_INT;
    	sizeInBytes += UnsafeMemory.SIZE_OF_INT;
    	sizeInBytes += UnsafeMemory.SIZE_OF_INT;
    	sizeInBytes += 10; // Assume fixed sized byte Array for now.
    	
    	return sizeInBytes;
    }
 
    public ObjectToBeSerialised(final long sourceId, final boolean special,
                                final int orderCode, final int priority,
                                final byte[] randomString)
    {
        this.sourceId = sourceId;
        this.special = special;
        this.orderCode = orderCode;
        this.priority = priority;
        this.randomStrings = randomString;
    }
    
    
    public void write(final UnSafeHashMap buffer, long pos)
    {
        buffer.putLong(sourceId, pos);
        pos += UnsafeMemory.SIZE_OF_LONG;
        buffer.putBoolean(special, pos);
        pos += UnsafeMemory.SIZE_OF_BOOLEAN;
        buffer.putInt(orderCode, pos );
        pos += UnsafeMemory.SIZE_OF_INT;
        buffer.putInt(priority, pos );
        pos += UnsafeMemory.SIZE_OF_INT;
        // Write out the array length
        buffer.putInt(randomStrings.length, pos );
        pos += UnsafeMemory.SIZE_OF_INT;
        buffer.putBytes(pos, randomStrings, randomStrings.length);
    }
 
    final public static ObjectToBeSerialised read(final UnSafeHashMap buffer, long pos)
    {
        final long sourceId = buffer.getLong(pos);
        pos += UnsafeMemory.SIZE_OF_LONG;
        final boolean special = buffer.getBoolean(pos);
        pos += UnsafeMemory.SIZE_OF_BOOLEAN;
        final int orderCode = buffer.getInt(pos);
        pos += UnsafeMemory.SIZE_OF_INT;
        final int priority = buffer.getInt(pos);
        pos += UnsafeMemory.SIZE_OF_INT;
        int arrayLength = buffer.getInt(pos);
        pos += UnsafeMemory.SIZE_OF_INT;
        final byte[] randomStrings = new byte[arrayLength];
        buffer.getBytes(pos, randomStrings, arrayLength);
 
        return new ObjectToBeSerialised(sourceId, special, orderCode, 
                                        priority, randomStrings);
    }
 
    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
 
        final ObjectToBeSerialised that = (ObjectToBeSerialised)o;
 
        if (orderCode != that.orderCode)
        {
            return false;
        }
        if (priority != that.priority)
        {
            return false;
        }
        if (sourceId != that.sourceId)
        {
            return false;
        }
        if (special != that.special)
        {
            return false;
        }
        if (!Arrays.equals(randomStrings, that.randomStrings))
        {
            return false;
        }
      
        return true;
    }
}
 