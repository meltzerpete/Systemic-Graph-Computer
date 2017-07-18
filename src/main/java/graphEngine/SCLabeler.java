package graphEngine;

import org.apache.commons.lang3.ArrayUtils;
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

    private boolean relationshipExists(Node from, Node to, RelationshipType relationshipType) {
        ResourceIterator<Relationship> rels =
                (ResourceIterator<Relationship>) from.getRelationships(relationshipType, Direction.OUTGOING).iterator();
        return rels.stream()
                .anyMatch(rel -> rel.getEndNode().equals(to));
    }

    void labelFitsInScope(Node scope) {
        //TODO can add extra matching conditions here - OR/AND?

        SCSystemHandler scHandler = comp.getHandler();
        Stream<Node> containedContexts = scHandler.getContextsInScope(scope);
        containedContexts.forEach(context -> {
            Stream<Node>  otherSystems = scHandler.getAllSystemsInScope(scope);
            otherSystems
                    .filter(other -> !other.equals(context))
                    .filter(other -> !relationshipExists(context, other, Components.FITS1))
                    .filter(other -> !relationshipExists(context, other, Components.FITS2))
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

            HashSet<Node> set1 = new HashSet<>();
            HashSet<Node> set2 = new HashSet<>();

            rels1.forEachRemaining(rel1 -> set1.add(rel1.getEndNode()));
            rels2.forEachRemaining(rel2 -> set2.add(rel2.getEndNode()));

            set1.addAll(set2);

            // only concerned about same nodes in both sets if there is only one node in both sets
            if (set1.size() == 1)
                return false;

            rels1 = (ResourceIterator<Relationship>) context.getRelationships(Components.FITS1).iterator();
            rels2 = (ResourceIterator<Relationship>) context.getRelationships(Components.FITS2).iterator();

            return rels1.stream().anyMatch(rel1 -> nodesInScope.contains(rel1.getEndNode()))
                    && rels2.stream().anyMatch(rel2 -> nodesInScope.contains(rel2.getEndNode()));
        });

//        System.out.println("Labelling READY in " + scope.getProperty("name"));
        readyContexts.forEach(context -> {
            context.addLabel(Components.READY);
//            System.out.println(context.getProperty("name"));
            if (context.hasProperty(Components.readyContextScopeID)) {
                long[] ids = (long[]) context.getProperty(Components.readyContextScopeID);
                if (!ArrayUtils.contains(ids, scope.getId())) {
                    long[] newIds = Arrays.copyOf(ids, ids.length + 1);
                    newIds[ids.length] = scope.getId();
                    context.setProperty(Components.readyContextScopeID, newIds);
                }
            } else {
                context.setProperty(Components.readyContextScopeID, new long[]{scope.getId()});
            }
        });

    }

    boolean fitsLabels(Node context, Node node, String propertyName) {

        if (context.hasProperty(propertyName))
            return Arrays.stream((String[]) context.getProperty(propertyName))
                    .map(labelString -> node.hasLabel(Label.label(labelString)))
                    .reduce(true, (aBoolean, aBoolean2) -> aBoolean && aBoolean2);
        else
            return false;
    }
}
