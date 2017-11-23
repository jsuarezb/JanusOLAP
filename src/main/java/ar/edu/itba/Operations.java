package ar.edu.itba;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;

public class Operations {

    JanusGraph graph;
    JanusGraphTransaction tx;

    public Operations(JanusGraph graph) {
        this.graph = graph;
        this.tx = graph.newTransaction();
    }

    /**
     * Replaces all nodes with "bottom" label to the corresponding node with "top" label.
     * This means that if there is a path from V to W where V has label "bottom" and W has label "top",
     * all edges that go from a vertex R to V will now go to W. Also, all vertices in the path from V to 
     * W that have no edge that gets in them will be removed.
     */
    public void climb(String bottom, String top) throws Exception {
    	// Find all vertices with "bottom" label.
        GraphTraversal<Vertex, Vertex> bottomTraversal = graph.traversal().V().hasLabel(bottom);
        GraphTraversal<Vertex, Vertex> auxTraversal = bottomTraversal.asAdmin().clone();

        // Find the ancestor vertex with label "top" for each "bottom" vertex.
        while (!auxTraversal.hasLabel(top).hasNext()) {
        	bottomTraversal = bottomTraversal.out("extendsFrom");
            auxTraversal.close();
            auxTraversal = bottomTraversal.asAdmin().clone();
            if (bottomTraversal.count().next() == 0) {
            	throw new IllegalArgumentException(
            			top + " label  vertices not found starting from vertices with label " + bottom);
            }
            bottomTraversal = auxTraversal.asAdmin().clone();
        }

        // For each path, (v:bottom) -*-> (u:top), remove e = (r, v) and v, and add e = (r, u). 
        bottomTraversal.hasLabel(top).path().toStream().forEach(path -> {
        	Vertex start = path.get(0);
        	Vertex end = path.get(path.size() - 1);

        	Iterator<Edge> it = start.edges(Direction.IN);
        	// Replace (r, v) to (r, u).
        	while (it.hasNext()) {
        		Edge edge = it.next();
        		edge.outVertex().addEdge(edge.label(), end);
        		edge.remove();
        	}
        
        	// Remove all vertices in the path that have no in-edges.
        	for (int i = 0; i < path.size() - 1; i++) {
        		Vertex curr = path.get(i);
        		if (!curr.edges(Direction.IN).hasNext()) {
        			curr.remove();
        		}
        	}
        });
    }
    
    /**
     * Minimizes the graph over the call vertices. If two vertices have the same time, the same caller and the 
     * same participants (including their cardinality), then they will be merged into a single call vertex with
     * an array of durations containing the durations of both calls (This allows more flexibility for later aggregations).  
     */
    public void minimize() {
    	Stream<Vertex> calls = graph.traversal().V().hasLabel("call").toStream();
    	// Iterate over all the calls to try to find those that have all same participants, caller and time.
    	calls.forEach(call -> {
    		// This condition prevents from finding matches for a call that is a match of a previously visited call.
    		if ((boolean) call.property("visited").value()) {
    			call.remove();
    			return;
    		}
    		call.property("visited", true);
    		// Find all calls that occurred at the same time as "call".
    		Stream<Vertex> otherCalls = graph.traversal().V(call.id()).out("atTime").in().hasLabel("call").toStream();
    		otherCalls.forEach(otherCall -> {
    			if ((boolean) otherCall.property("visited").value()) {
        			return;
        		}
    			// Iterate over all the edges of "call" and make sure that there is a "unique"
    			// edge that is exactly the same that leaves from "otherCall". Mark that edge as used
    			// to enforce that the cardinality of edges is the same.
    			Iterator<Edge> edges = call.edges(Direction.OUT);
    			Set<Edge> usedEdges = new HashSet<Edge>();
    			boolean hasMatch = true;
    			while (edges.hasNext() && hasMatch) {
    				hasMatch = false;
    				Edge edge = edges.next();
    				// Make sure both edges have the same label.
    				Iterator<Edge> neighEdges = graph.traversal().V(otherCall.id()).outE(edge.label());
    				while (neighEdges.hasNext() && !hasMatch) {
    					Edge neighEdge = neighEdges.next();
    					// If both edges in-vertex has the same value, then it's a match.
    					if (!usedEdges.contains(neighEdge)) {
    						hasMatch = neighEdge.inVertex().value("value")
    								.equals(edge.inVertex().value("value"));
    						if (hasMatch) {
    							usedEdges.add(neighEdge);
    						}
    					}
    				}
    			}
    			// Make sure that there are no spare edges in "otherCall" that haven't been matched.
    			Long count = graph.traversal().V(otherCall.id()).out().count().next();
    			if (hasMatch && count == usedEdges.size()) {
    				// Mark "otherCall" as visited so that we don't find matches for this one, and add
    				// the duration to "call".
    				otherCall.property("visited", true);
    				call.property("duration", otherCall.value("duration"));
    			}
    		});
    	});
    }
}
