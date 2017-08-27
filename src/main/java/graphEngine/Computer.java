package graphEngine;

import nodeParser.NodeMatch;
import nodeParser.Parser;
import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.*;
import queryCompiler.Compiler;
import queryCompiler.Vertex;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;

/**
 * Created by Pete Meltzer on 11/07/17.
 */
class Computer implements Runnable {

    public int maxInteractions = 10000;

    private boolean fine = true;

    final GraphDatabaseService db;
    private List<Result> output;

    private final SCLabeler labeler;
    private final SCSystemHandler handler;

    private Hashtable<String, Vertex> matchingGraphs = new Hashtable<>();
    private Hashtable<String, NodeMatch> nodeMatches = new Hashtable<>();

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

    NodeMatch getNodeMatch(String query) {
        if (nodeMatches.containsKey(query))
            return nodeMatches.get(query);
        else {
            Parser parser = new Parser();
            NodeMatch nodeMatch = parser.parse(query);
            nodeMatches.put(query, nodeMatch);
            return nodeMatch;
        }
    }

    void preProcess() {

        /* -- Compile matchingGraphs -- */

        StopWatch compileTimer = new StopWatch();
        compileTimer.start();

        try (Transaction tx = db.beginTx()) {

            db.findNodes(Components.CONTEXT).stream()
                    .forEach(node -> {
                        if (node.hasProperty(Components.s1Query)) {
                            String queryString = (String) node.getProperty(Components.s1Query);
                            if (queryString.startsWith("(:"))
                                getNodeMatch(queryString);
                        }
                        if (node.hasProperty(Components.s2Query)) {
                            String queryString = (String) node.getProperty(Components.s2Query);
                            if (queryString.startsWith("(:"))
                                getNodeMatch(queryString);
                        }
                    });


            compileTimer.stop();
            System.out.println(String.format("Compilation took %,d x10e-3 s", compileTimer.getTime()));

            /* -- Create initial FITS relationships / label initial READY nodes -- */

            StopWatch timer = new StopWatch();
            timer.start();


            labeler.createAllFits();

            labeler.labelAllReady();

            timer.stop();
            System.out.println(String.format(Locale.UK, "Pre- processing: %,d x10e-9 s", timer.getNanoTime()));
            timer.reset();
            tx.success();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    private boolean check() {
//        Transaction checkTx = db.beginTx();
//        try {
//            boolean check = db.getAllLabels().stream().anyMatch(label -> label.equals(Components.READY));
//            checkTx.success();
//            checkTx.close();
//            return check;
//        } catch (Exception ex) {
//            ex.printStackTrace();
//            checkTx.failure();
//            checkTx.close();
//            return false;
//        }
//    }

    void compute(int maxInteractions) {
        fine = false;
        try {
            compute(maxInteractions, null);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    void compute(int maxInteractions, BufferedWriter writer) throws IOException {


        StopWatch timer = new StopWatch();
        timer.start();

        StopWatch fineTimer = new StopWatch();

        int count = 0;
        while (count < maxInteractions) {

            Node readyContext = null;

            if (fine) fineTimer.reset();
            if (fine) fineTimer.start();
            Transaction functionTx = db.beginTx();
            if (fine) fineTimer.stop();
            if (fine) writer.write(fineTimer.getNanoTime() + ", ");

            if (fine) fineTimer.reset();
            if (fine) fineTimer.start();

            if (db.getAllLabels().stream().noneMatch(label -> label.equals(Components.READY)))
                    break;

            if (fine) fineTimer.stop();
            if (fine) writer.write(fineTimer.getNanoTime() + ", ");

            try {

            /* -- get random trio -- */

                if (fine) fineTimer.reset();
                if (fine) fineTimer.start();
                readyContext = handler.getRandomReady();
                if (fine) fineTimer.stop();
                if (fine) writer.write(fineTimer.getNanoTime() + ", ");

            /* -- acquire locks -- */
                if (fine) fineTimer.reset();
                if (fine) fineTimer.start();
                Function selectedFunction = Function.valueOf((String) readyContext.getProperty(Components.function));
                if (fine) fineTimer.stop();
                if (fine) writer.write(fineTimer.getNanoTime() + ", ");

                if (fine) fineTimer.reset();
                if (fine) fineTimer.start();
                Pair readyPair = handler.getRandomPair(readyContext);
                if (fine) fineTimer.stop();
                if (fine) writer.write(fineTimer.getNanoTime() + ", ");

                if (fine) fineTimer.reset();
                if (fine) fineTimer.start();
                functionTx.acquireWriteLock(readyContext);
                functionTx.acquireWriteLock(readyPair.s1);
                functionTx.acquireWriteLock(readyPair.s2);

                // lock parent scopes if necessary
                if (selectedFunction.affectsS1parentScopes()) {
                    handler.getParentScopes(readyPair.s1).forEach(functionTx::acquireWriteLock);
                }
                if (selectedFunction.affectsS2parentScopes()) {
                    handler.getParentScopes(readyPair.s2).forEach(functionTx::acquireWriteLock);
                }
                if (fine) fineTimer.stop();
                if (fine) writer.write(fineTimer.getNanoTime() + ", ");

            /* -- perform transformation function -- */

                if (fine) fineTimer.reset();
                if (fine) fineTimer.start();
                selectedFunction.perform(readyContext, readyPair.s1, readyPair.s2);
                if (fine) fineTimer.stop();
                if (fine) writer.write(fineTimer.getNanoTime() + ", ");


            /* -- amend READY / FITS properties -- */

                if (fine) fineTimer.reset();
                if (fine) fineTimer.start();
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
                if (fine) fineTimer.stop();
                if (fine) writer.write(fineTimer.getNanoTime() + ", ");

                if (fine) fineTimer.reset();
                if (fine) fineTimer.start();
                // check for new fits/ready
                //TODO
                readyPair.getAll().forEach(sNode -> {
                    labeler.createFitsForTarget(sNode);
                    handler.getParentScopes(sNode).forEach(scope -> {
                        labeler.labelReadyInScope(scope);
                    });
                });

                // check original context if it is no longer in a parent scope of the transformed sNodes
                if (selectedFunction.affectsS1parentScopes() || selectedFunction.affectsS2parentScopes()) {
                    handler.getParentScopes(readyContext).forEach(scope -> {
                        labeler.createFitsInScope(scope);
                        labeler.labelReadyInScope(scope);
                    });
                }
                if (fine) fineTimer.stop();
                if (fine) writer.write(fineTimer.getNanoTime() + ", ");
                if (fine) writer.flush();


            /* -- releases the locks -- */

                if (fine) fineTimer.reset();
                if (fine) fineTimer.start();
                functionTx.success();
                if (fine) fineTimer.stop();
                if (fine) writer.write(fineTimer.getNanoTime() + "\n");

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

            if (count % 100 == 0) System.out.println(String.format("%,d interactions...", count));
        }

        if (count == maxInteractions)
        System.out.println(String.format("** Max interactions (%,d) reached **", maxInteractions));
        else
        System.out.println(String.format("** No more active systems **\nExecution completed in %,d interactions.", count));

        System.out.println(String.format("Execution time: %,d x10e-9 s", timer.getNanoTime()));
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
//        compute(this.maxInteractions);
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
