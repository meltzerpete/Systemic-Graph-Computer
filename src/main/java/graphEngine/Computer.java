package graphEngine;

import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.*;
import queryCompiler.Compiler;
import queryCompiler.Vertex;

import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;

/**
 * Created by Pete Meltzer on 11/07/17.
 */
class Computer implements Runnable {

    final GraphDatabaseService db;
    private List<Result> output;

    private final SCLabeler labeler;
    private final SCSystemHandler handler;

    private Hashtable<String, Vertex> matchingGraphs = new Hashtable<>();

    private Compiler compiler = new Compiler();

    Computer(GraphDatabaseService db) {
        this.db = db;
        this.output = new LinkedList<>();

        this.handler = new SCSystemHandler();
        this.labeler = new SCLabeler();
    }


    /**
     * Returns the compiled graph for the given query.
     * Checks first for a version in memory else compiles it and adds it
     * to the collection.
     * @param query Cypher style query for matching (root node <strong>must</strong>
     *              have key 'n').
     * @return {@link Vertex} for root of graph
     */
    Vertex getMatchingGraph(String query) {
        if (matchingGraphs.containsKey(query))
            return matchingGraphs.get(query);
        else {
            Vertex vertex = compiler.compile(query);
            matchingGraphs.put(query, vertex);
            return vertex;
        }
    }

    void preProcess() {

        /* -- Compile matchingGraphs -- */

        StopWatch compileTimer = new StopWatch();
        compileTimer.start();

        db.findNodes(Components.CONTEXT).stream().forEach(node -> {
            if (node.hasProperty(Components.s1Query)) {
                String s1Query = (String) node.getProperty(Components.s1Query);
                if (!matchingGraphs.containsKey(s1Query)) {
                    matchingGraphs.put(s1Query, compiler.compile(s1Query));
                }
            }
            if (node.hasProperty(Components.s2Query)) {
                String s2Query = (String) node.getProperty(Components.s2Query);
                if (!matchingGraphs.containsKey(s2Query)) {
                    matchingGraphs.put(s2Query, compiler.compile(s2Query));
                }
            }
        });

        compileTimer.stop();
        System.out.println(String.format("Compilation took %,d x10e-3 s", compileTimer.getTime()));

        /* -- Create initial FITS relationships / label initial READY nodes -- */

        StopWatch timer = new StopWatch();
        timer.start();
        labeler.labelAllFits();
        labeler.labelAllReady();
        timer.stop();
        System.out.println(String.format(Locale.UK, "Pre- processing: %,d x10e-9 s", timer.getNanoTime()));
        timer.reset();
    }

    private boolean check() {
        Transaction checkTx = db.beginTx();
        try {
            boolean check = db.getAllLabels().stream().anyMatch(label -> label.equals(Components.READY));
            checkTx.success();
            checkTx.close();
            return check;
        } catch (Exception ex) {
            ex.printStackTrace();
            checkTx.failure();
            checkTx.close();
            return false;
        }
    }

    void compute(int maxInteractions) {

        StopWatch timer = new StopWatch();
        timer.start();

        int count = 0;
        while (count < maxInteractions && check()) {


            Node readyContext = null;
            Transaction functionTx = db.beginTx();

            try {

            /* -- get random trio -- */

                readyContext = handler.getRandomReady();
            /* -- acquire locks -- */
                Function selectedFunction = Function.valueOf((String) readyContext.getProperty("function"));
                Pair readyPair = handler.getRandomPair(readyContext);

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
                count++;

                //TODO amend exception types
            } catch (NoMoreReadysException e) {
                // no ready systems - terminate program by exiting loop
                functionTx.success();
                break;
            } catch (IllegalArgumentException ex) {
                System.out.println("Failed to parse: " + (readyContext != null ? readyContext.getProperty("function") : "null"));
                ex.printStackTrace();
            } catch (Exception ex) {
                System.out.println("Failed to acquire locks");
//                ex.printStackTrace();
                functionTx.failure();
            } finally {
//                System.out.println("Success!");
//                String state = db.execute(TestGraphQueries.viewGraph).resultAsString();
//                System.out.println(state);
                functionTx.close();
            }

        }

        if (count == maxInteractions)
        System.out.println(String.format(Locale.UK, "** Max interactions (%,d) reached **", maxInteractions));
        else
        System.out.println(String.format(Locale.UK, "** No more active systems **\nExecution completed in %,d interactions.", count));

        System.out.println(String.format(Locale.UK, "Thread: %s\nTotal execution time: %,d x10e-9 s", Thread.currentThread().getId(), timer.getNanoTime()));
    }

    SCLabeler getLabeler() {
        return labeler;
    }

    SCSystemHandler getHandler() {
        return handler;
    }

    @Override
    public void run() {
//        Transaction t = db.beginTx();
//        System.out.println(db.execute(TestGraphQueries.viewGraph).resultAsString());
        compute(5000);
        //        t.success();
//        t.close();
    }

    private class SCSystemHandler extends graphEngine.SCSystemHandler {
        SCSystemHandler() {
            super(db);
        }
    }

    private class SCLabeler extends graphEngine.SCLabeler {

        SCLabeler() {
            super(Computer.this, db);
        }
    }
}
