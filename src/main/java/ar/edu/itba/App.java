package ar.edu.itba;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.util.JanusGraphCleanup;
import org.javatuples.Pair;

import ar.edu.itba.Operations.Aggregation;

public class App {
	
	public static void main(String[] args) throws Exception {
		String path = args[0];

		JanusGraph graph = null;
		boolean isOpen = false;
		boolean wasCleaned = false;
		boolean cleanUp = false;
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


		buildSchema(graph);

		System.out.println("Schema built");

		DataReader reader = new DataReader(graph, path);
		reader.buildGraph();

		Operations operations = new Operations(graph);
//		operations.slice("city", "Baluk");
		operations.rollUp(Arrays.asList(new Pair<>("phone", "allLocations"), 
				new Pair<>("timestamp", "year")), Aggregation.COUNT);
		
		graph.traversal().V().toStream().forEach(vertex -> {
			System.out.println(toString(vertex));
		});

		graph.traversal().E().toStream().forEach(edge -> {
			System.out.println(toString(edge));
		});
		
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

	private static void buildSchema(JanusGraph graph) {
		JanusGraphManagement mgmt = graph.openManagement();

		addVertexLabel(mgmt, "phone");
		addVertexLabel(mgmt, "user");
		addVertexLabel(mgmt, "city");
		addVertexLabel(mgmt, "country");
		addVertexLabel(mgmt, "allLocations");

		addVertexLabel(mgmt, "operator");
		addVertexLabel(mgmt, "allOperators");

		addVertexLabel(mgmt, "timestamp");
		addVertexLabel(mgmt, "day");
		addVertexLabel(mgmt, "month");
		addVertexLabel(mgmt, "year");
		addVertexLabel(mgmt, "allTimes");

		addVertexLabel(mgmt, "call");

		addEdgeLabel(mgmt, "calledBy");
		addEdgeLabel(mgmt, "integratedBy");
		addEdgeLabel(mgmt, "atTime");

		addEdgeLabel(mgmt, "extendsFrom");

		addPropertyKey(mgmt, "value", String.class, Cardinality.SINGLE);
		addPropertyKey(mgmt, "duration", Double.class, Cardinality.LIST);
		addPropertyKey(mgmt, "visited", Boolean.class, Cardinality.SINGLE);

		mgmt.commit();
	}

	private static void addVertexLabel(JanusGraphManagement mgmt, String label) {
		if (mgmt.getVertexLabel(label) != null)
			return;

		mgmt.makeVertexLabel(label).make();
	}

	private static void addEdgeLabel(JanusGraphManagement mgmt, String label) {
		if (mgmt.getEdgeLabel(label) != null)
			return;

		mgmt.makeEdgeLabel(label).make();
	}

	private static void addPropertyKey(JanusGraphManagement mgmt, String label, Class<?> dataType,
			Cardinality cardinality) {
		if (mgmt.getPropertyKey(label) != null)
			return;

		mgmt.makePropertyKey(label).dataType(dataType).cardinality(cardinality).make();
	}
}
