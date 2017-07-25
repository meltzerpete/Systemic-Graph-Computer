package queryCompiler;

import java.util.HashSet;
import java.util.LinkedList;

import static queryCompiler.Tokens.*;


/**
 * Created by Pete Meltzer on 24/07/17.
 */
public class Compiler {

    private Tokenizer tokenizer = new Tokenizer();

    private int debugLevel = 2;

    public Compiler() {
        setTokens();
    }

    public Vertex compile(String queryString) {

        if (debugLevel < 3)
            System.out.println("Compiling: " + queryString);

        tokenizer.tokenize(queryString);

        if (debugLevel < 1)
            tokenizer.getTokens()
                    .forEach(token -> System.out.println(token.token + ": " + token.sequence));

        LinkedList<Token> tokens = tokenizer.getTokens();

        Parser parser = new Parser(tokens);

        Vertex v = parser.parse();

        if (debugLevel < 2)
            printGraph(v);

        return v;
    }

    private HashSet<Vertex> visited = new HashSet<>();

    private void printGraph(Vertex v) {

        if (visited.contains(v)) {
            System.out.println("SEEN: " + v.name);
        } else {
            visited.add(v);
            System.out.print(v.name + ", labels: ");
            v.labels.forEach(label -> System.out.print(label + ", "));
            System.out.print("properties: ");
            v.properties.forEach(objectPropertyPair -> System.out.print(objectPropertyPair + ", "));
            System.out.print("edges: ");
            v.edges.forEach(edge -> System.out.print(edge.direction + " - " + edge.next.name + ", "));
            System.out.println();
        }

        v.edges.forEach(edge -> printGraph(edge.next));
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
        tokenizer.add("<-", INCOMING);
        tokenizer.add("->", OUTGOING);
        tokenizer.add("-", BIDIRECTIONAL);
        tokenizer.add("(([1-9][0-9]*)|0)(\\.[0-9]*[1-9])?", NUMBER);
        tokenizer.add("[a-zA-Z][a-zA-Z0-9_-]*", STRING);
        tokenizer.add("'", SINGLE_QUOTE);
        tokenizer.add("\"", DOUBLE_QUOTE);
    }
}