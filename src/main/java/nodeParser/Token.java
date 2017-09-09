package nodeParser;

/**
 * Created by Pete Meltzer on 25/07/17.
 * <p>Used by the Tokenizer/Parser for labelling of tokens.</p>
 */
public class Token {
    public final Tokens token;
    public final String sequence;

    public Token(Tokens token, String sequence) {
        super();
        this.token = token;
        this.sequence = sequence;
    }

    @Override
    public String toString() {
        return this.sequence;
    }

}
