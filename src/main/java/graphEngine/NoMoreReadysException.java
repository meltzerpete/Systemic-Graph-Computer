package graphEngine;

import java.util.function.Supplier;

/**
 * Created by Pete Meltzer on 11/07/17.
 */
public class NoMoreReadysException extends Exception implements Supplier<NoMoreReadysException> {
    public NoMoreReadysException(String message) {
        super(message);
    }

    @Override
    public NoMoreReadysException get() {
        return this;
    }
}
