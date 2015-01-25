
public interface SerializableObject {
	
	public SerializableObject read(final UnSafeHashMap buffer, long pos);
	public void write(final UnSafeHashMap buffer, long pos);

}
