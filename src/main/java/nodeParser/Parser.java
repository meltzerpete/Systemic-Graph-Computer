package nodeParser;

import org.neo4j.graphdb.Label;

import java.util.ArrayList;
import java.util.LinkedList;

import static nodeParser.Tokens.*;


/**
 * Created by Pete Meltzer on 25/07/17.
 * http://cogitolearning.co.uk/?p=573
 */
public class Parser {

    private Tokenizer tokenizer;
    private LinkedList<Token> tokens;
    private Token lookahead;

    private ArrayList<Label> labels = new ArrayList<>();
    private ArrayList<PropertyPair> properties = new ArrayList<>();

    public NodeMatch parse(String queryString) {

        tokenizer = new Tokenizer();
        setTokens();

        tokenizer.tokenize(queryString);
        tokens = tokenizer.getTokens();
        lookahead = tokens.getFirst();

        if (lookahead.token != LBRKT) throw new ParserException("Expected ( found %s", lookahead);
        nextToken();

        labelSet();

        propertySet();

        if (lookahead.token != RBRKT) throw new ParserException("Expected ) found %s", lookahead);
        nextToken();

        if (lookahead.token != EPSILON) throw new ParserException("Expected $ found %s", lookahead);

        return new NodeMatch(labels, properties);
    }

    private void labelSet() {
        // labelSet :: LABEL labelSet | LABEL | EPSILON

        while (lookahead.token == COLON) {
            labels.add(label());
        }
    }

    private Label label() {
        // LABEL :: : STRING

        if (lookahead.token != COLON) throw new ParserException("Expected : found %s", lookahead);
        nextToken();    // consume :

        if (lookahead.token != STRING) throw new ParserException("Expected string found %s", lookahead);
        Label label = Label.label(lookahead.sequence);
        nextToken();    // consume string

        return label;
    }

    private void propertySet() {
        // propertySet :: { properties }

        if (lookahead.token == LCBRKT) {
            nextToken();

            while (lookahead.token == STRING) {
                properties.add(property());
                if (lookahead.token == COMMA) nextToken();
                else break;
            }

            if (lookahead.token != RCBKT) throw new ParserException("Expected } found %s", lookahead);
        }
    }

    private PropertyPair property() {
        // PROPERTY :: STRING : VALUE

        String key = lookahead.sequence;
        nextToken();    // consume string

        if (lookahead.token != COLON) throw new ParserException("Expected : found %s", lookahead);
        nextToken();    // consume :

        Object value = value();

        return new PropertyPair<>(key, value);
    }

    private Object value() {

        // VALUE :: NUMBER | STRING

        Object value;

        switch (lookahead.token) {
            case NUMBER:
                value = Long.valueOf(lookahead.sequence);
                nextToken();    // consume number
                break;
            case STRING:
                value = lookahead.sequence;
                nextToken();    // consume string
                break;
            default:
                throw new ParserException("Expected number or string found %s", lookahead);
        }

        return value;
    }

    private void nextToken() {

        tokens.pop();

        // at the end of input we return an epsilon token
        if (tokens.isEmpty())
            lookahead = new Token(EPSILON, "");
        else
            lookahead = tokens.getFirst();
    }

    private void setTokens() {
        tokenizer.add("\\(", LBRKT);
        tokenizer.add("\\)", RBRKT);
        tokenizer.add("\\[", LSQBRKT);
        tokenizer.add("\\]", RSQBRKT);
        tokenizer.add("\\{", LCBRKT);
        tokenizer.add("\\}", RCBKT);
        tokenizer.add(",", COMMA);
        tokenizer.add(":", COLON);
        tokenizer.add("(([1-9][0-9]*)|0)(\\.[0-9]*[1-9])?", NUMBER);
        tokenizer.add("[a-zA-Z][a-zA-Z0-9_-]*", STRING);
        tokenizer.add("'", SINGLE_QUOTE);
        tokenizer.add("\"", DOUBLE_QUOTE);
    }
}
