package parallel;

/**
 * Created by Pete Meltzer on 05/08/17.
 */
public class Triplet {

    ContextEntry contextEntry;
    long s1;
    long s2;

    public Triplet(ContextEntry contextEntry, long s1, long s2) {
        this.contextEntry = contextEntry;
        this.s1 = s1;
        this.s2 = s2;
    }

    public void set(ContextEntry contextEntry, long s1, long s2) {
        this.contextEntry = contextEntry;
        this.s1 = s1;
        this.s2 = s2;
    }

    @Override
    public String toString() {
        return contextEntry.context + ", " + s1 + ", " + s2;
    }
}
