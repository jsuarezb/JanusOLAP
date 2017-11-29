package ar.edu.itba;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import ar.edu.itba.Operations.Aggregation;

public class QueriesOlap {

	private static boolean syso = true;
	
	public static void query1_1(JanusGraph graph) {
		query(graph, "phone", Aggregation.AVG, false);
	}
	
	public static void query1_2(JanusGraph graph) {
		query(graph, "phone", Aggregation.AVG, true);
	}
	
	public static void query1_3(JanusGraph graph) {
		query(graph, "user", Aggregation.MAX, false);
	}
	
	public static void query1_4(JanusGraph graph) {
		query(graph, "user", Aggregation.COUNT, false);
	}
	
	public static void query1_5(JanusGraph graph) {
		Operations operations = new Operations(graph);
		
		operations.rollUp("timestamp", "month", Aggregation.ARRAY);
		operations.rollUp("phone", "user", Aggregation.ARRAY);
		
		GraphTraversal<Vertex, Path> paths = graph.traversal().V().has("type", "user")
				.in("integratedBy", "calledBy").out("integratedBy", "calledBy").path();
		
		Map<Triplet<String, String, String>, Set<Vertex>> tripletsMap = new HashMap<>();

		paths.toStream().forEach(path -> {
			String user1 = ((Vertex) path.get(0)).value("value");
			String user2 = ((Vertex) path.get(2)).value("value");
			String month = ((Vertex) path.get(1)).edges(Direction.OUT, "atTime").next().inVertex().value("value");
			Triplet<String, String, String> triplet = new Triplet<>(user1, user2, month);
			Vertex call = (Vertex) path.get(1);
			
			tripletsMap.putIfAbsent(triplet, new HashSet<Vertex>());
			tripletsMap.get(triplet).add(call);
		});
		
		tripletsMap.entrySet().stream().forEach(entry -> {
			Triplet<String, String, String> triplet = entry.getKey();
			if (triplet.getValue1().compareTo(triplet.getValue0()) >= 0) {
				return;
			}
			Set<Vertex> callSet = entry.getValue();
			double result = Operations.agg(callSet.stream().flatMapToDouble(call -> {
				Iterable<Double> durations = () -> call.values("duration");
	    		return StreamSupport.stream(durations.spliterator(), true).mapToDouble(x -> x);
			}), Aggregation.COUNT);
			if (syso) {
				System.out.println(triplet.toString() + ": " + result);
				syso = false;
			}
		});
	}
	
	public static void query1_6(JanusGraph graph) {
		Operations operations = new Operations(graph);
		operations.diceEquals("month", "4-2017");
		
		query(graph, "user", Aggregation.COUNT, false);
	}
	
	public static void query2_1(JanusGraph graph) {
		Operations operations = new Operations(graph);
		
		operations.diceEquals("month", "4-2017");
		operations.rollUp("phone", "user", Aggregation.ARRAY);
		
		GraphTraversal<Vertex, Path> paths = graph.traversal().V().has("type", "user")
				.in("integratedBy", "calledBy").out("integratedBy", "calledBy").path();
		
		Map<Triplet<String, String, String>, Set<Vertex>> tripletsMap = new HashMap<>();

		paths.toStream().forEach(path -> {
			String user1 = ((Vertex) path.get(0)).value("value");
			String user2 = ((Vertex) path.get(2)).value("value");
			if (user1.equals(user2)) {
				return;
			}
			Vertex call = (Vertex) path.get(1);
			Iterator<Edge> edgesIt = ((Vertex) path.get(1)).edges(Direction.OUT, "integratedBy", "calledBy");
			while (edgesIt.hasNext()) {
				Edge edge = edgesIt.next();
				String user3 = edge.inVertex().value("value");
				if (user3.equals(user1) || user3.equals(user2)) {
					continue;
				}
				Triplet<String, String, String> triplet = new Triplet<>(user1, user2, user3);
				tripletsMap.putIfAbsent(triplet, new HashSet<Vertex>());
				tripletsMap.get(triplet).add(call);
			}
		});
		
		tripletsMap.entrySet().stream().forEach(entry -> {
			Triplet<String, String, String> triplet = entry.getKey();
			if (triplet.getValue1().compareTo(triplet.getValue0()) <= 0 
					|| triplet.getValue2().compareTo(triplet.getValue1()) <= 0) {
				return;
			}
			Set<Vertex> callSet = entry.getValue();
			double result = Operations.agg(callSet.stream().flatMapToDouble(call -> {
				Iterable<Double> durations = () -> call.values("duration");
	    		return StreamSupport.stream(durations.spliterator(), true).mapToDouble(x -> x);
			}), Aggregation.AVG);
			if (syso) {
				System.out.println(triplet.toString() + ": " + result);
				syso = false;
			}
		});
	} 
	
	private static void query(JanusGraph graph, String top, Aggregation agg, boolean diffCaller) {
		Operations operations = new Operations(graph);
		
		operations.rollUp("timestamp", "allTimes", Aggregation.ARRAY);
		if (!top.equals("phone")) {
			operations.rollUp("phone", top, Aggregation.ARRAY);
		}
		
		GraphTraversal<Vertex, Path> paths = getPaths(graph, top, diffCaller);
		
		forEachPair(paths.toStream(), agg);
	}
	
	private static GraphTraversal<Vertex, Path> getPaths(JanusGraph graph, String bottom, boolean diffCaller) {
		if (diffCaller) {
			return graph.traversal().V().has("type", bottom)
					.in("calledBy").out("integratedBy").path();
		} else {
			return graph.traversal().V().has("type", bottom)
					.in("integratedBy", "calledBy").out("integratedBy", "calledBy").path();
		}
	}
	
	private static void forEachPair(Stream<Path> paths, Aggregation agg) {
		Map<Pair<String, String>, Set<Vertex>> valuesPairMap = new HashMap<>();

		paths.forEach(path -> {
			String value1 = ((Vertex) path.get(0)).value("value");
			String value2 = ((Vertex) path.get(2)).value("value");
			Pair<String, String> phonePair = new Pair<>(value1, value2);
			Vertex call = (Vertex) path.get(1);
			
			valuesPairMap.putIfAbsent(phonePair, new HashSet<Vertex>());
			valuesPairMap.get(phonePair).add(call);
		});
		
		valuesPairMap.entrySet().stream().forEach(entry -> {
			Pair<String, String> valuePair = entry.getKey();
			if (valuePair.getValue1().compareTo(valuePair.getValue0()) <= 0) {
				return;
			}
			Set<Vertex> callSet = entry.getValue();
			double result = Operations.agg(callSet.stream().flatMapToDouble(call -> {
				Iterable<Double> durations = () -> call.values("duration");
	    		return StreamSupport.stream(durations.spliterator(), true).mapToDouble(x -> x);
			}), agg);
			if (syso) {
				System.out.println(valuePair.toString() + ": " + result);
				syso = false;
			}
		});
	}
}
