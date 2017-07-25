package queryCompiler;

/**
 * Created by Pete Meltzer on 24/07/17.
 */
class PropertyPair<T> {

    public String name;
    public T value;

    public PropertyPair(String name, T value) {

        this.name = name;
        this.value = value;

    }

    @Override
    public String toString() {
        return
                (this.name != null ? this.name : "null")
                        + ": " +
                        (this.value != null ? this.value : "null");
    }
}
