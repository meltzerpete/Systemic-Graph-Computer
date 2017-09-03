package queryCompiler;

/**
 * Created by Pete Meltzer on 25/07/17.
 */
class Token {
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
