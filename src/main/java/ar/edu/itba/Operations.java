package ar.edu.itba;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Spliterators;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.JanusGraph;
import org.javatuples.Pair;

public class Operations {

    private final JanusGraph graph;

    public Operations(JanusGraph graph) {
        this.graph = graph;
    }

    /**
     * Replaces all nodes with "bottom" label to the corresponding node with "top" label.
     * This means that if there is a path from V to W where V has label "bottom" and W has label "top",
     * all edges that go from a vertex R to V will now go to W. Also, all vertices in the path from V to 
     * W that have no edge that gets in them will be removed.
     */
    public void climb(String bottom, String top) {
    	System.out.println(String.format("Climbing from %s to %s", bottom, top));
    	// Find all vertices with "bottom" label.
        GraphTraversal<Vertex, Vertex> bottomTraversal = graph.traversal().V().has("type", bottom);
        GraphTraversal<Vertex, Vertex> auxTraversal = bottomTraversal.asAdmin().clone();

        // Find the ancestor vertex with label "top" for each "bottom" vertex.
        while (!auxTraversal.has("type", top).hasNext()) {
        	bottomTraversal = bottomTraversal.out("extendsFrom");
            try {
				auxTraversal.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
            auxTraversal = bottomTraversal.asAdmin().clone();
            if (bottomTraversal.count().next() == 0) {
            	throw new IllegalArgumentException(
            			top + " label  vertices not found starting from vertices with label " + bottom);
            }
            bottomTraversal = auxTraversal.asAdmin().clone();
        }

        // For each path, (v:bottom) -*-> (u:top), remove e = (r, v) and v, and add e = (r, u). 
        bottomTraversal.has("type", top).path().toStream().forEach(path -> {
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
    	System.out.println("Minimizing...");
    	Stream<Vertex> calls = graph.traversal().V().has("type", "call").toStream();
    	// Iterate over all the calls to try to find those that have all same participants, caller and time.
    	calls.forEach(call -> {
    		// This condition prevents from finding matches for a call that is a match of a previously visited call.
    		if ((boolean) call.property("visited").value()) {
    			call.remove();
    			return;
    		}
    		call.property("visited", true);
    		// Find all calls that occurred at the same time as "call".
    		Stream<Vertex> otherCalls = graph.traversal().V(call.id()).out("atTime").in().has("type", "call").toStream();
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
    	
    	// Remove the visited mark.
    	calls = graph.traversal().V().has("type", "call").toStream();
    	calls.forEach(call -> call.property("visited", false));
    }
    
    /**
     * Aggregates over the array of durations for each call. 
     */
    public void aggregate(Aggregation agg) {
    	System.out.println(String.format("Aggregating: %s", agg.name()));
    	Stream<Vertex> calls = graph.traversal().V().has("type", "call").toStream();
    	
    	// Iterate over each call and aggregate over the array of durations.
    	calls.forEach(call -> {
    		Iterable<Double> durations = () -> call.values("duration");
    		DoubleStream stream = StreamSupport.stream(durations.spliterator(), true).mapToDouble(x -> x);
    		
    		// Remove the existing values of the duration array property.
    		Iterator<VertexProperty<Double>> vpIt = call.properties("duration");
    		while(vpIt.hasNext()) {
    			vpIt.next().remove();
    		}
    		
    		// Get the new duration value based on the corresponding aggregation.
    		double value = agg(stream, agg);
    		// Replace the duration value with the one after the aggregation.
    		call.property("duration", value);
    	});
    }
    
    /**
     * Rolls up from "bottom" to "top" using "agg" as aggregation.
     */
    public void rollUp(String bottom, String top, Aggregation agg) {
    	System.out.println(String.format("Roll up from %s to %s with %s", bottom, top, agg));
    	climb(bottom, top);
    	minimize();
    	if (agg.equals(Aggregation.ARRAY)) {
    		return;
    	}
    	aggregate(agg);
    }
    
    /**
     * Rolls up from all "bottom"-"top" pairs, using "agg" as aggregation.
     */
    public void rollUp(List<Pair<String, String>> bottomTops, Aggregation agg) {
    	System.out.println(String.format("Multiple roll up with %s", agg));
    	for (Pair<String, String> bottomTop : bottomTops) {
    		climb(bottomTop.getValue0(), bottomTop.getValue1());
    	}
    	minimize();
    	if (agg.equals(Aggregation.ARRAY)) {
    		return;
    	}
    	aggregate(agg);
    }
    
    /**
     * Keeps only facts that are related indirectly to vertices with label "label" only with value "value".
     */
    public void diceEquals(String label, String value) {
    	System.out.println(String.format("Dice %s = '%s'", label, value));
    	// Find all vertices with the label and not the value.
    	Stream<Vertex> verticesToRemove = graph.traversal().V().has("type", label).toStream()
    		.filter(v -> !value.equals(v.value("value")));
    	
    	removeIns(verticesToRemove);
    }
    
    /**
     * Keeps only facts that are not related indirectly to vertices with label "label" only with value "value".
     */
    public void diceNotEquals(String label, String value) {
    	System.out.println(String.format("Dice %s <> '%s'", label, value));
    	// Find all vertices with the label and the value.
    	Stream<Vertex> verticesToRemove = graph.traversal().V().has("type", label)
    			.has("value", value).toStream();
    	
    	removeIns(verticesToRemove);
    }
    
    /**
     * Removes recursively all the in-vertices of the stream of vertices received. 
     */
    private void removeIns(Stream<Vertex> vertices) {
    	Iterator<Vertex> it = vertices.iterator();
    	if (!it.hasNext()) {
    		return;
    	}
    	vertices = StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, 0), false);
    	vertices.forEach(v -> {
    		try {
	    		Iterable<Edge> edges = () -> v.edges(Direction.IN);
	    		Stream<Vertex> outVertices = StreamSupport.stream(edges.spliterator(), true)
	    				.map(edge -> edge.outVertex());
    			v.remove();
    			removeIns(outVertices);
    		} catch (IllegalStateException e) {
    			// Vertex was already removed.
    		}
    	});
    }
    
    public static double agg(DoubleStream stream, Aggregation agg) {
    	double value;
    	switch (agg) {
		case AVG:
			value = stream.average().getAsDouble();
			break;
		case COUNT:
			value = stream.count();
			break;
		case MAX:
			value = stream.max().getAsDouble();
			break;
		case MIN:
			value = stream.min().getAsDouble();
			break;
		case SUM:
			value = stream.sum();
			break;
		default:
			throw new IllegalArgumentException("Unknown aggregation: " + agg);
		}
    	return value;
    }
    
    public static enum Aggregation {
    	
    	ARRAY, SUM, AVG, MAX, MIN, COUNT;
    }
}
