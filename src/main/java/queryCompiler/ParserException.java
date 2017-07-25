package queryCompiler;

/**
 * Created by Pete Meltzer on 24/07/17.
 * http://cogitolearning.co.uk/?p=525
 */
public class ParserException extends RuntimeException {
    public ParserException(String msg) {
        super(msg);
    }

    public ParserException(String message, Object... args) {
        super(String.format(message, args));
    }
}
