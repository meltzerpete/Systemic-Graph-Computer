package queryCompiler;

/**
 * Created by Pete Meltzer on 24/07/17.
 */
public class PropertyPair<T> {

    private String key;
    private T value;

    public String getKey() {
        return key;
    }

    public T getValue() {
        return value;
    }

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
