package nodeParser;

/**
 * Created by Pete Meltzer on 24/07/17.
 * <p>Represents a key - value mapping for properties in a Node.</p>
 */
public class PropertyPair<T> {

    private String key;
    private T value;

    /**
     * Returns the key.
     * @return key
     */
    public String getKey() {
        return key;
    }

    /**
     * Returns the value object.
     * @return value
     */
    public T getValue() {
        return value;
    }

    /**
     * Creates a new PropertyPair from a String key and object value.
     * @param key property key
     * @param value property value
     */
    public PropertyPair(String key, T value) {

        this.key = key;
        this.value = value;

    }

    @Override
    public String toString() {
        return
                (this.key != null ? this.key : "null")
                        + ": " +
                        (this.value != null ? this.value : "null");
    }
}
