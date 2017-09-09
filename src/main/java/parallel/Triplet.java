package parallel;

/**
 * Created by Pete Meltzer on 05/08/17.
 * <p>A composition of a contextEntry and a pair of matching s1 and
 * s2 system IDs.</p>
 */
public class Triplet {

    ContextEntry contextEntry;
    long s1;
    long s2;

    /**
     * @param contextEntry contextEntry object
     * @param s1 s1 matching Node ID
     * @param s2 s2 matching Node ID
     */
    public Triplet(ContextEntry contextEntry, long s1, long s2) {
        this.contextEntry = contextEntry;
        this.s1 = s1;
        this.s2 = s2;
    }

    /**
     * Used in debugging.
     * @param contextEntry contextEntry object
     * @param s1 s1 matching NodeID
     * @param s2 s2 matching NodeID
     */
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
