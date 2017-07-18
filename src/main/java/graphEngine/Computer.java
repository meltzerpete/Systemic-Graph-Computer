package graphEngine;

import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Locale;

/**
 * Created by Pete Meltzer on 11/07/17.
 */
class Computer {

    final GraphDatabaseService db;
    private List<Result> output;

    private final SCLabeler labeler;
    private final SCSystemHandler handler;

    private StopWatch timer = new StopWatch();

    Computer(GraphDatabaseService db) {
        this.db = db;
        this.output = new LinkedList<>();

        this.handler = new SCSystemHandler();
        this.labeler = new SCLabeler();
    }

    void preProcess() {

        timer.start();
        Transaction tx = db.beginTx();
        try {
            labeler.labelAllFits();
            labeler.labelAllReady();

            tx.success();
        } catch (TransactionFailureException ex) {
            ex.printStackTrace();
        } finally {
            tx.close();
            System.out.println(String.format(Locale.UK, "Pre- processing: %,d x10e-9 s", timer.getNanoTime()));
        }

    }

    void compute(int maxInteractions) {

        int count = 0;
        while (count++ < maxInteractions
                && db.getAllLabels().stream().anyMatch(label -> label.equals(Components.READY))) {

            // get random trio
            Node readyContext = handler.getRandomReady();
//            System.out.println("Selected " + readyContext.getProperty("name") + " for context.");
            Pair readyPair = handler.getRandomPair(readyContext);
//            System.out.println("Selected " + readyPair.s1.getProperty("name") + " and " + readyPair.s2.getProperty("name")
//                    + " for interaction.");

            Transaction functionTx = db.beginTx();
            Function selectedFunction = Function.valueOf((String) readyContext.getProperty("function"));

            try {
            /* -- acquire locks -- */

                functionTx.acquireWriteLock(readyContext);
                functionTx.acquireWriteLock(readyPair.s1);
                functionTx.acquireWriteLock(readyPair.s2);

                // lock parent scopes if necessary
                if (selectedFunction.affectsS1parentScopes())
                    handler.getParentScopes(readyPair.s1).forEach(functionTx::acquireWriteLock);
                if (selectedFunction.affectsS2parentScopes())
                    handler.getParentScopes(readyPair.s2).forEach(functionTx::acquireWriteLock);

            /* -- perform transformation function -- */

                selectedFunction.perform(readyContext, readyPair.s1, readyPair.s2);

            /* -- amend READY / FITS properties -- */

                ResourceIterator<Relationship> fits1Relationships =
                        (ResourceIterator<Relationship>) readyPair.s1
                                .getRelationships(Components.FITS1, Direction.INCOMING).iterator();

                ResourceIterator<Relationship> fits2Relationships =
                        (ResourceIterator<Relationship>) readyPair.s2
                                .getRelationships(Components.FITS2, Direction.INCOMING).iterator();

                // remove existing fits/READY labels dependent on the transformed nodes
                fits1Relationships.stream().forEach(fitsRel -> {
                    if (fitsRel.getStartNode().hasLabel(Components.READY)) {
                        fitsRel.getStartNode().removeLabel(Components.READY);
                        fitsRel.getStartNode().removeProperty(Components.readyContextScopeID);
                    }
                    fitsRel.delete();
                });

                fits2Relationships.stream().forEach(fitsRel -> {
                    if (fitsRel.getStartNode().hasLabel(Components.READY))
                        fitsRel.getStartNode().removeLabel(Components.READY);
                    fitsRel.delete();
                });

                // check for new fits/ready
                readyPair.getAll().forEach(sNode ->
                        handler.getParentScopes(sNode).forEach(scope -> {
                            labeler.labelFitsInScope(scope);
                            labeler.labelReadyInScope(scope);
                        }));

                // check original context if it is no longer in a parent scope of the transformed sNodes
                if (selectedFunction.affectsS1parentScopes() || selectedFunction.affectsS2parentScopes()) {
                    handler.getParentScopes(readyContext).forEach(scope -> {
                        labeler.labelFitsInScope(scope);
                        labeler.labelReadyInScope(scope);
                    });
                }

            /* -- releases the locks -- */
                functionTx.success();

                //TODO amend exception types
            } catch (IllegalArgumentException ex) {
                System.out.println("Failed to parse: " + readyContext.getProperty("function"));
                ex.printStackTrace();
            } catch (Exception ex) {
                System.out.println("Failed to acquire locks");
                ex.printStackTrace();
                functionTx.failure();
            } finally {
//                System.out.println("Success!");
//                String state = db.execute(TestGraphQueries.viewGraph).resultAsString();
//                System.out.println(state);
                functionTx.close();
            }

        }

        if (--count == maxInteractions)
            System.out.println(String.format(Locale.UK, "** Max interactions (%,d) reached **", maxInteractions));
        else
            System.out.println(String.format(Locale.UK, "** No more active systems **\nExecution completed in %,d interactions.", maxInteractions));

        System.out.println(String.format(Locale.UK, "Total execution time: %,d x10e-9 s", timer.getNanoTime()));
    }

    SCLabeler getLabeler() {
        return labeler;
    }

    SCSystemHandler getHandler() {
        return handler;
    }

    private class SCSystemHandler extends graphEngine.SCSystemHandler {
        SCSystemHandler() {
            super(Computer.this, db);
        }
    }

    private class SCLabeler extends graphEngine.SCLabeler {

        SCLabeler() {
            super(Computer.this, db);
        }
    }
}
