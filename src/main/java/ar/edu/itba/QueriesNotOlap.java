package ar.edu.itba;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.javatuples.Pair;
import org.javatuples.Triplet;

public class QueriesNotOlap {

	public static boolean printTuples;
	
	public static void query1_1_1(JanusGraph graph) {
		
		GraphTraversal<Vertex, Vertex> calls = graph.traversal().V().has("type", "call");
		
		Map<Pair<String, String>, Set<Vertex>> pairsMap = new HashMap<>();

		calls.toStream().forEach(call -> {
			Iterator<Edge> edgesIt = call.edges(Direction.OUT, "integratedBy", "calledBy");
			Set<String> phones = new HashSet<String>();
			while (edgesIt.hasNext()) {
				String phone = edgesIt.next().inVertex().value("value");
				phones.add(phone);
			}
			if (phones.size() < 2) {
				return;
			}
			for (String phone1 : phones) {
				for (String phone2 : phones) {
					if (phone1.equals(phone2)) {
						continue;
					}
					Pair<String, String> phonePair = new Pair<>(phone1, phone2);
					pairsMap.putIfAbsent(phonePair, new HashSet<Vertex>());
					pairsMap.get(phonePair).add(call);
				}
			}
		});
		
		aggPairs(pairsMap, Aggregation.AVG);
	} 
	
	public static void query1_1_2(JanusGraph graph) {
		
		GraphTraversal<Vertex, Path> paths = graph.traversal().V().has("type", "phone")
				.in("integratedBy", "calledBy").out("integratedBy", "calledBy").path();
		
		Map<Pair<String, String>, Set<Vertex>> pairsMap = new HashMap<>();
		
		paths.toStream().forEach(path -> {
			String value1 = ((Vertex) path.get(0)).value("value");
			String value2 = ((Vertex) path.get(2)).value("value");
			Pair<String, String> phonePair = new Pair<>(value1, value2);
			Vertex call = (Vertex) path.get(1);
			
			pairsMap.putIfAbsent(phonePair, new HashSet<Vertex>());
			pairsMap.get(phonePair).add(call);
		});
		
		aggPairs(pairsMap, Aggregation.AVG);
	} 
	
	public static void query1_2(JanusGraph graph) {
		
		GraphTraversal<Vertex, Vertex> calls = graph.traversal().V().has("type", "call");
		
		Map<Pair<String, String>, Set<Vertex>> pairsMap = new HashMap<>();

		calls.toStream().forEach(call -> {
			Iterator<Edge> callereEdgesIt = call.edges(Direction.OUT, "calledBy");
			Set<String> callers = new HashSet<String>();
			while (callereEdgesIt.hasNext()) {
				String phone = callereEdgesIt.next().inVertex().value("value");
				callers.add(phone);
			}
			Iterator<Edge> participantsEdgesIt = call.edges(Direction.OUT, "integratedBy");
			Set<String> participants = new HashSet<String>();
			while (participantsEdgesIt.hasNext()) {
				String phone = participantsEdgesIt.next().inVertex().value("value");
				participants.add(phone);
			}
			for (String phone1 : callers) {
				for (String phone2 : participants) {
					if (phone1.equals(phone2)) {
						continue;
					}
					Pair<String, String> phonePair = new Pair<>(phone1, phone2);
					pairsMap.putIfAbsent(phonePair, new HashSet<Vertex>());
					pairsMap.get(phonePair).add(call);
				}
			}
		});
		
		aggPairs(pairsMap, Aggregation.AVG);
	} 
	
	public static void query1_3(JanusGraph graph) {
		Map<Pair<String, String>, Set<Vertex>> pairsMap = getUserPairs(graph);
		
		aggPairs(pairsMap, Aggregation.MAX);
	} 
	
	public static void query1_4(JanusGraph graph) {
		Map<Pair<String, String>, Set<Vertex>> pairsMap = getUserPairs(graph);
		
		aggPairs(pairsMap, Aggregation.COUNT);
	} 
	
	public static void query1_5(JanusGraph graph) {
		GraphTraversal<Vertex, Vertex> calls = graph.traversal().V().has("type", "call");
		
		Map<Triplet<String, String, String>, Set<Vertex>> tripletsMap = new HashMap<>();

		calls.toStream().forEach(call -> {
			Iterator<Edge> edgesIt = call.edges(Direction.OUT, "integratedBy", "calledBy");
			Set<String> users = new HashSet<String>();
			while (edgesIt.hasNext()) {
				Iterator<Edge> phoneEdgesIt = edgesIt.next().inVertex().edges(Direction.OUT);
				while (phoneEdgesIt.hasNext()) {
					Vertex v = phoneEdgesIt.next().inVertex();
					if (v.label().equals("user")) {
						users.add(v.value("value"));
					}
				}
			}
			String month = call.edges(Direction.OUT, "atTime").next().inVertex() // timestamp
					.edges(Direction.OUT).next().inVertex() // day
					.edges(Direction.OUT).next().inVertex().value("value");
			if (users.size() < 2) {
				return;
			}
			for (String user1 : users) {
				for (String user2 : users) {
					if (user1.equals(user2)) {
						continue;
					}
					Triplet<String, String, String> userTriplet = new Triplet<>(user1, user2, month);
					tripletsMap.putIfAbsent(userTriplet, new HashSet<Vertex>());
					tripletsMap.get(userTriplet).add(call);
				}
			}
		});
		
		tripletsMap.entrySet().stream().forEach(entry -> {
			Triplet<String, String, String> triplet = entry.getKey();
			if (triplet.getValue1().compareTo(triplet.getValue0()) <= 0) {
				return;
			}
			Set<Vertex> callSet = entry.getValue();
			double result = Operations.agg(callSet.stream().flatMapToDouble(call -> {
				Iterable<Double> durations = () -> call.values("duration");
	    		return StreamSupport.stream(durations.spliterator(), true).mapToDouble(x -> x);
			}), Aggregation.COUNT);
			if (printTuples) {
				System.out.println(triplet.toString() + ": " + result);
			}
		});
	}
	
	public static void query1_6(JanusGraph graph) {
		
		GraphTraversal<Vertex, Vertex> calls = graph.traversal().V().has("type", "month")
				.has("value", "4-2017")
				.in() // day
				.in() // timestamp
				.in(); // call
		
		Map<Pair<String, String>, Set<Vertex>> pairsMap = new HashMap<>();

		calls.toStream().forEach(call -> {
			Iterator<Edge> edgesIt = call.edges(Direction.OUT, "integratedBy", "calledBy");
			Set<String> users = new HashSet<String>();
			while (edgesIt.hasNext()) {
				Edge edge = edgesIt.next();
				Iterator<Edge> inEdgesIt = edge.inVertex().edges(Direction.OUT);
				String user = null;
				while (inEdgesIt.hasNext()) {
					Vertex v = inEdgesIt.next().inVertex();
					if (v.label().equals("user")) {
						user = v.value("value");
					}
				}
				users.add(user);
			}
			if (users.size() < 2) {
				return;
			}
			for (String user1 : users) {
				for (String user2 : users) {
					if (user1.equals(user2)) {
						continue;
					}
					Pair<String, String> pair = new Pair<>(user1, user2);
					pairsMap.putIfAbsent(pair, new HashSet<Vertex>());
					pairsMap.get(pair).add(call);
				}
			}
		});
		
		aggPairs(pairsMap, Aggregation.COUNT);
	} 
	
	public static void query2_1(JanusGraph graph) {
		
		GraphTraversal<Vertex, Vertex> calls = graph.traversal().V().has("type", "month")
				.has("value", "4-2017")
				.in() // day
				.in() // timestamp
				.in(); // call
		
		Map<Triplet<String, String, String>, Set<Vertex>> tripletsMap = new HashMap<>();

		calls.toStream().forEach(call -> {
			Iterator<Edge> edgesIt = call.edges(Direction.OUT, "integratedBy", "calledBy");
			Set<String> users = new HashSet<String>();
			while (edgesIt.hasNext()) {
				Edge edge = edgesIt.next();
				Iterator<Edge> inEdgesIt = edge.inVertex().edges(Direction.OUT);
				String user = null;
				while (inEdgesIt.hasNext()) {
					Vertex v = inEdgesIt.next().inVertex();
					if (v.label().equals("user")) {
						user = v.value("value");
					}
				}
				users.add(user);
			}
			if (users.size() < 3) {
				return;
			}
			for (String user1 : users) {
				for (String user2 : users) {
					for (String user3 : users) {
						if (user1.equals(user2) || user3.equals(user1) || user3.equals(user2)) {
							continue;
						}
						Triplet<String, String, String> triplet = new Triplet<>(user1, user2, user3);
						tripletsMap.putIfAbsent(triplet, new HashSet<Vertex>());
						tripletsMap.get(triplet).add(call);
					}
				}
			}
		});
		
		aggTriplets(tripletsMap, Aggregation.AVG);
	} 
	
	private static Map<Pair<String, String>, Set<Vertex>> getUserPairs(JanusGraph graph) {
		GraphTraversal<Vertex, Vertex> calls = graph.traversal().V().has("type", "call");
		
		Map<Pair<String, String>, Set<Vertex>> pairsMap = new HashMap<>();

		calls.toStream().forEach(call -> {
			Iterator<Edge> edgesIt = call.edges(Direction.OUT, "integratedBy", "calledBy");
			Set<String> users = new HashSet<String>();
			while (edgesIt.hasNext()) {
				Iterator<Edge> phoneEdgesIt = edgesIt.next().inVertex().edges(Direction.OUT);
				while (phoneEdgesIt.hasNext()) {
					Vertex v = phoneEdgesIt.next().inVertex();
					if (v.label().equals("user")) {
						users.add(v.value("value"));
					}
				}
			}
			if (users.size() < 2) {
				return;
			}
			for (String user1 : users) {
				for (String user2 : users) {
					if (user1.equals(user2)) {
						continue;
					}
					Pair<String, String> userPair = new Pair<>(user1, user2);
					pairsMap.putIfAbsent(userPair, new HashSet<Vertex>());
					pairsMap.get(userPair).add(call);
				}
			}
		});
		
		return pairsMap;
	}
	
	private static void aggPairs(Map<Pair<String, String>, Set<Vertex>> pairsMap, 
			Aggregation agg) {
		pairsMap.entrySet().stream().forEach(entry -> {
			Pair<String, String> pair = entry.getKey();
			if (pair.getValue1().compareTo(pair.getValue0()) <= 0) {
				return;
			}
			Set<Vertex> callSet = entry.getValue();
			double result = Operations.agg(callSet.stream().flatMapToDouble(call -> {
				Iterable<Double> durations = () -> call.values("duration");
	    		return StreamSupport.stream(durations.spliterator(), true).mapToDouble(x -> x);
			}), agg);
			if (printTuples) {
				System.out.println(pair.toString() + ": " + result);
			}
		});
	}
	
	private static void aggTriplets(Map<Triplet<String, String, String>, Set<Vertex>> tripletsMap, 
			Aggregation agg) {
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
			}), agg);
			if (printTuples) {
				System.out.println(triplet.toString() + ": " + result);
			}
		});
	}
}
