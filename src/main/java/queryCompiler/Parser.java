package queryCompiler;


import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Objects;

import static queryCompiler.Tokens.*;

/**
 * Created by Pete Meltzer on 25/07/17.
 * http://cogitolearning.co.uk/?p=573
 */
public class Parser {

    private LinkedList<Token> tokens;
    Token lookahead;

    private HashMap<String, Vertex> vertices = new HashMap<>();

    @SuppressWarnings("unchecked cast")
    public Parser(LinkedList<Token> tokens) {
        this.tokens = (LinkedList<Token>) tokens.clone();
    }

    public Vertex parse() {

        lookahead = tokens.getFirst();

        query();

        if (lookahead.token != EPSILON)
            throw new ParserException("Unexpected symbol %s found.", lookahead);

//        System.out.println("Success");

//        System.out.println(vertices.size() + " vertices created.");
        return vertices.get("n");

    }

    private void query() {

        // QUERY :: STATEMENTS
        statements();
    }

    private void statements() {

        // STATEMENTS :: STATEMENT
        //             | STATEMENT , STATEMENTS

        statement();

        if (lookahead.token == COMMA) {

            nextToken();
            statements();

        } else if (lookahead.token != EPSILON) {
            throw new ParserException("Expected , or $ found %s", lookahead);
        }
    }

    private void statement() {

        // STATEMENT :: ( NODE )
        //            | ( NODE ) DIRECTION [ RELATIONSHIP ] DIRECTION ( NODE )


        if (lookahead.token != LBRKT) throw new ParserException("Expected ( found %s", lookahead);
        nextToken();    // consume (

        Vertex root = node();

        if (lookahead.token != RBRKT) throw new ParserException("Expected ) found %s", lookahead);
        nextToken();    // consume )

        if (lookahead.token == EPSILON || lookahead.token == COMMA)
            return;

        Direction d1 = direction();

        if (lookahead.token != LSQBRKT) throw new ParserException("Expected [ found %s", lookahead);
        nextToken();    // consume [

        Edge edge = relationship();

        if (lookahead.token != RSQBRKT) throw new ParserException("Expected ] found %s", lookahead);
        nextToken();    // consume ]

        Direction d2 = direction();

        if (lookahead.token != LBRKT) throw new ParserException("Expected ( found %s", lookahead);
        nextToken();    // consume (

        edge.next = node();

        if (lookahead.token != RBRKT) throw new ParserException("Expected ) found %s", lookahead);
        nextToken();    // consume )

        if (d1 == Direction.BOTH) {
            if (Objects.equals(d2, Direction.BOTH)) {
                edge.direction = Direction.BOTH;
            } else if (Objects.equals(d2, Direction.OUTGOING)) {
                edge.direction = Direction.OUTGOING;
            } else {
                throw new ParserException("Direction mismatch in edges: ...-[]<-()");
            }
        } else if (d1 == Direction.INCOMING) {
            if (d2 == Direction.BOTH) {
                edge.direction = Direction.INCOMING;
            } else if (d2 == Direction.OUTGOING) {
                edge.direction = Direction.BOTH;
            } else {
                throw new ParserException("Direction mismatch in edges: ...-[]<-()");
            }
        } else {
            throw new ParserException("Direction mismatch in edges: ()->[]-...");
        }

        root.edges.add(edge);
    }

    private Edge relationship() {

        // RELATIONSHIP :: LABELS PLIST

        Edge edge = new Edge();

        edge.labels.addAll(labels());

        edge.properties.addAll(plist());

        return edge;

    }

    private Direction direction() {

        // DIRECTION :: BIDIRECTIONAL | INCOMING | OUTGOING

        Direction direction;
        switch (lookahead.token) {
            case BIDIRECTIONAL:
                direction = Direction.BOTH;
                nextToken();    // consume bidirectional
                break;
            case INCOMING:
                direction = Direction.INCOMING;
                nextToken();    // consume incoming
                break;
            case OUTGOING:
                direction = Direction.OUTGOING;
                nextToken();    // consume outgoing
                break;
            default:
                throw new ParserException("Expected - <- or -> found %s", lookahead);
        }
        return direction;
    }

    private Vertex node() {

        // NODE :: STRING LABELS PLIST


        if (lookahead.token != STRING) throw new ParserException("Expected string found %s", lookahead);

        String name = lookahead.sequence;

        Vertex vertex;

        if (vertices.containsKey(name))
            vertex = vertices.get(name);
        else {
            vertex = new Vertex();
            vertices.put(name, vertex);
        }

        vertex.name = name;
        nextToken();    // consume string

        vertex.labels.addAll(labels());

        vertex.properties.addAll(plist());

        return vertex;
    }

    private LinkedList<Label> labels() {

        // LABELS :: LABEL
        //         | LABEL LABELS
        //         | EPSILON

        LinkedList<Label> labels = new LinkedList<>();

        if (lookahead.token == COLON) {
            labels.add(label());
            labels.addAll(labels());
        }
        return labels;

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

    private LinkedList<PropertyPair<Object>> plist() {

        // PLIST :: { PROPERTIES }
        //        | EPSILON

        LinkedList<PropertyPair<Object>> properties = new LinkedList<>();

        if (lookahead.token == LCBRKT) {
            nextToken();    // consume {

            properties = properties();

            nextToken();    // consume }

        }
        return properties;
    }

    private LinkedList<PropertyPair<Object>> properties() {

        // PROPERTIES :: PROPERTY
        //             | PROPERTY , PROPERTIES
        //             | EPSILON

        LinkedList<PropertyPair<Object>> properties = new LinkedList<>();

        if (lookahead.token == STRING) {
            properties.add(property());

            if (lookahead.token == COMMA) {
                nextToken();    // consume ,
                properties.addAll(properties());
            }
            return properties;
        }

        return null;

    }

    private PropertyPair<Object> property() {

        // PROPERTY :: STRING : VALUE

        if (lookahead.token != STRING) throw new ParserException("Expected string found %s", lookahead);
        String name = lookahead.sequence;
        nextToken();    // consume string

        if (lookahead.token != COLON) throw new ParserException("Expected : found %s", lookahead);
        nextToken();    // consume :

        Object value = value();

        return new PropertyPair<>(name, value);
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
}
