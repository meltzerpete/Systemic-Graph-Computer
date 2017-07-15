package graphEngine;

import org.neo4j.graphdb.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.Collectors;
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
        //TODO can add extra matching conditions here - OR/AND?

        SCSystemHandler scHandler = comp.getHandler();
        Stream<Node> containedContexts = scHandler.getContextsInScope(scope);
        containedContexts.forEach(context -> {
            Stream<Node>  otherSystems = scHandler.getAllSystemsInScope(scope);
            otherSystems
                    .filter(other -> !other.equals(context))
                    .forEach(other -> {
                        if (fitsLabels(context, other, Components.s1Labels)) {
                            Relationship rel = context.createRelationshipTo(other, Components.FITS1);
                            rel.setProperty("scope", scope.getId());
                        }
                        if (fitsLabels(context, other, Components.s2Labels)) {
                            Relationship rel = context.createRelationshipTo(other, Components.FITS2);
                            rel.setProperty("scope", scope.getId());
                        }
                    });
        });
    }

    void labelAllFits() {

        db.findNodes(Components.SCOPE).stream().forEach(this::labelFitsInScope);
    }

    void labelAllReady() {

        db.findNodes(Components.SCOPE).stream().forEach(this::labelReadyInScope);
    }

    void labelReadyInScope(Node scope) {

        SCSystemHandler scHandler = comp.getHandler();
        Stream<Node> contexts = scHandler.getContextsInScope(scope);
        Stream<Node> nodes = scHandler.getAllSystemsInScope(scope);

        HashSet<Node> nodesInScope  = nodes.collect(Collectors.toCollection(HashSet::new));

        Stream<Node> readyContexts = contexts.filter(context -> {
            ResourceIterator<Relationship> rels1 = (ResourceIterator<Relationship>) context.getRelationships(Components.FITS1).iterator();
            ResourceIterator<Relationship> rels2 = (ResourceIterator<Relationship>) context.getRelationships(Components.FITS2).iterator();

            return rels1.stream().anyMatch(rel1 -> nodesInScope.contains(rel1.getEndNode()))
                    && rels2.stream().anyMatch(rel2 -> nodesInScope.contains(rel2.getEndNode()));

        });

        readyContexts.forEach(context -> {
            context.addLabel(Components.READY);
        });

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
