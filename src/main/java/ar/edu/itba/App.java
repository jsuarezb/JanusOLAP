package ar.edu.itba;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.util.JanusGraphCleanup;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.LoggerFactory;

import ar.edu.itba.Operations.Aggregation;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;

public class App {
	
	private static boolean syso = true;
	
	private JanusGraph graph;
	private JanusGraphManagement mgmt;
	
	public static void main(String[] args) throws Exception {
		String path = args[0];
		String query = args[1];
		new App(path, query);
	}
	
	public App(String path, String query) throws Exception {
		// Sets the package level to INFO
		LoggerContext loggerContext = (LoggerContext)LoggerFactory.getILoggerFactory();
		Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
		rootLogger.setLevel(Level.INFO);
		
		boolean isOpen = false;
		boolean wasCleaned = false;
		boolean cleanUp = true;
		while (!isOpen) {
			graph = JanusGraphFactory.build().set("storage.backend", "cassandra")
					.set("storage.hostname", "10.16.6.21,10.16.6.22,10.16.6.23,10.16.6.24")
					.set("storage.cassandra.replication-factor", 2).set("storage.cassandra.keyspace", "tcolloca")
					.set("schema.default", "none").set("storage.username", "tcolloca").set("storage.password", "tcolloca")
					.open();
			isOpen = true;
			
			if (cleanUp && !wasCleaned) {
				graph.close();
				isOpen = false;
				wasCleaned = true;
				JanusGraphCleanup.clear(graph);
				System.out.println("Closed and cleared graph");
			}
		}


		buildSchema();

		System.out.println("Schema built");

		DataReader reader = new DataReader(graph, path);
		reader.buildGraph();

		long start = System.currentTimeMillis();
		switch (query) {
		case "1_1":
			QueriesOlap.query1_1(graph);
			break;
		case "1_2":
			QueriesOlap.query1_2(graph);
			break;
		case "1_3":
			QueriesOlap.query1_3(graph);
			break;
		case "1_4":
			QueriesOlap.query1_4(graph);
			break;
		case "1_5":
			QueriesOlap.query1_5(graph);
			break;
		case "1_6":
			QueriesOlap.query1_6(graph);
			break;
		case "2_1":
			QueriesOlap.query2_1(graph);
			break;
		case "1_1_1_not_olap":
			query1_1_1(graph);
			break;
		case "1_1_2_not_olap":
			query1_1_2(graph);
			break;
		case "2_1_not_olap":
			query2_1(graph);
			break;
		}
		long end = System.currentTimeMillis();
		System.out.println("TOTAL TIME: " + (end - start));
		
		graph.close();
	}
	
	public static String toString(Edge edge) {
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append(toString(edge.outVertex(), "value", "duration"));
		strBuilder.append("- " + edge.label() + " -> ");
		strBuilder.append(toString(edge.inVertex(), "value", "duration"));
		return strBuilder.toString();
	}
	
	public static String toString(Vertex vertex, String ... keys) {
		List<String> keyList = keys != null ? Arrays.asList(keys) : new ArrayList<>();
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append("(v[" + vertex.id() + "] ");
		strBuilder.append(vertex.label());
		if (vertex.properties().hasNext()) {
			strBuilder.append(": {");
			for (Iterator<VertexProperty<Object>> it = vertex.properties(); it.hasNext();) {
				VertexProperty<Object> p = it.next();
				if (keyList.isEmpty() || keyList.contains(p.key())) {
					strBuilder.append(p.value());
					strBuilder.append(", ");
				}
			}
			strBuilder.append("}");
		}
		strBuilder.append(")");
		return strBuilder.toString();
	}

	private void buildSchema() throws InterruptedException {
		mgmt = graph.openManagement();

		PropertyKey valueProp = addPropertyKey("value", String.class, Cardinality.SINGLE, true);
		addPropertyKey("type", String.class, Cardinality.SINGLE, true);

		addPropertyKey("duration", Double.class, Cardinality.LIST, false);
		addPropertyKey("visited", Boolean.class, Cardinality.SINGLE, true);

		addVertexLabel("phone", valueProp);
		addVertexLabel("user", valueProp);
		addVertexLabel("city");
		addVertexLabel("country");
		addVertexLabel("allLocations");

		addVertexLabel("operator");
		addVertexLabel("allOperators");

		addVertexLabel("timestamp", valueProp);
		addVertexLabel("day");
		addVertexLabel("month", valueProp);
		addVertexLabel("year");
		addVertexLabel("allTimes", valueProp);

		addVertexLabel("call", valueProp);

		addEdgeLabel("calledBy");
		addEdgeLabel("integratedBy");
		addEdgeLabel("atTime");

		addEdgeLabel("extendsFrom");


		mgmt.commit();
	}

	private void addVertexLabel(String label, PropertyKey prop) throws InterruptedException {
		if (mgmt.getVertexLabel(label) != null)
			return;

		mgmt.makeVertexLabel(label).make();
	}
	
	private void addVertexLabel(String label) {
		if (mgmt.getVertexLabel(label) != null)
			return;

		mgmt.makeVertexLabel(label).make();
	}

	private void addEdgeLabel(String label) {
		if (mgmt.getEdgeLabel(label) != null)
			return;

		mgmt.makeEdgeLabel(label).make();
	}

	private PropertyKey addPropertyKey(String label, Class<?> dataType,
			Cardinality cardinality, boolean index) throws InterruptedException {
		if (mgmt.getPropertyKey(label) != null)
			return mgmt.getPropertyKey(label);

		PropertyKey prop = mgmt.makePropertyKey(label).dataType(dataType).cardinality(cardinality).make();
		if (index) {
			mgmt.buildIndex(label, Vertex.class).addKey(prop).buildCompositeIndex();
		}
		return prop;
	}
	
	public static void query1_1_1(JanusGraph graph) {
		
		GraphTraversal<Vertex, Vertex> calls = graph.traversal().V().has("type", "call");
		
		Map<Pair<String, String>, Set<Vertex>> tripletsMap = new HashMap<>();

		calls.toStream().forEach(call -> {
			Iterator<Edge> edgesIt = call.edges(Direction.OUT, "integratedBy", "calledBy");
			Set<String> users = new HashSet<String>();
			while (edgesIt.hasNext()) {
				String user = edgesIt.next().inVertex().value("value");
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
					Pair<String, String> triplet = new Pair<>(user1, user2);
					tripletsMap.putIfAbsent(triplet, new HashSet<Vertex>());
					tripletsMap.get(triplet).add(call);
				}
			}
		});
		
		tripletsMap.entrySet().stream().forEach(entry -> {
			Pair<String, String> triplet = entry.getKey();
			if (triplet.getValue1().compareTo(triplet.getValue0()) <= 0) {
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
	
	public static void query1_1_2(JanusGraph graph) {
		
		GraphTraversal<Vertex, Path> paths = graph.traversal().V().has("type", "phone")
				.in("integratedBy", "calledBy").out("integratedBy", "calledBy").path();
		
		Map<Pair<String, String>, Set<Vertex>> tripletsMap = new HashMap<>();

		paths.toStream().forEach(path -> {
			String value1 = ((Vertex) path.get(0)).value("value");
			String value2 = ((Vertex) path.get(2)).value("value");
			Pair<String, String> phonePair = new Pair<>(value1, value2);
			Vertex call = (Vertex) path.get(1);
			
			tripletsMap.putIfAbsent(phonePair, new HashSet<Vertex>());
			tripletsMap.get(phonePair).add(call);
		});
		
		tripletsMap.entrySet().stream().forEach(entry -> {
			Pair<String, String> triplet = entry.getKey();
			if (triplet.getValue1().compareTo(triplet.getValue0()) <= 0) {
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
}
