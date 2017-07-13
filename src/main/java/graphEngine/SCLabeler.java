package graphEngine;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Created by Pete Meltzer on 11/07/17.
 */
abstract class SCLabeler {

    private Computer comp;
    private GraphDatabaseService db;

    SCLabeler(Computer comp, GraphDatabaseService db) {
        this.comp = comp;
        this.db = db;
    }

    void labelFitsInScope(Node scope) {
        //TODO can add extra matching conditions here - OR

        SCSystemHandler scHandler = comp.getHandler();
        Stream<Node> containedContexts = scHandler.getContextsInScope(scope);
        containedContexts.forEach(context -> {
            Stream<Node>  otherSystems = scHandler.getAllSystemsInScope(scope);
            otherSystems
                    .filter(other -> !other.equals(context))
                    .forEach(other -> {
                        if (fitsLabels(context, other, Components.s1Labels))
                            context.createRelationshipTo(other, Components.FITS1);
                    });
        });
    }

    void labelAllFits() {

        db.findNodes(Components.SCOPE).stream()
                .forEach(scope -> labelFitsInScope(scope));
    }

    void labelAllReady() {
        //TODO labelAllReady()
    }

    void labelReadyInScope() {
        //TODO labelReadyInScope()
    }

    private boolean fitsLabels(Node context, Node node, String propertyName) {

        if (context.hasProperty(propertyName))
            return Arrays.stream((String[]) context.getProperty(propertyName))
                    .map(labelString -> node.hasLabel(Label.label(labelString)))
                    .reduce(true, (aBoolean, aBoolean2) -> aBoolean && aBoolean2);
        else
            return false;
    }
}
