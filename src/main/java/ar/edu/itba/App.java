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
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.util.JanusGraphCleanup;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;

public class App {
	
	private JanusGraph graph;
	private JanusGraphManagement mgmt;
	
	public static void main(String[] args) throws Exception {
		String path = args[0];
		String query = args[1];
		boolean sysoTuples = args.length >= 3 ? Boolean.valueOf(args[2]) : false;
		new App(path, query, sysoTuples);
	}
	
	public App(String path, String query, boolean sysoTuples) throws Exception {
		// Sets the package level to INFO
		LoggerContext loggerContext = (LoggerContext)LoggerFactory.getILoggerFactory();
		Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
		rootLogger.setLevel(Level.INFO);
		
		QueriesOlap.sysoTuples = sysoTuples;
		QueriesNotOlap.sysoTuples = sysoTuples;
		
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
		case "1_1_not_olap":
		case "1_1_1_not_olap":
			QueriesNotOlap.query1_1_1(graph);
			break;
		case "1_1_2_not_olap":
			QueriesNotOlap.query1_1_2(graph);
			break;
		case "1_2_not_olap":
			QueriesNotOlap.query1_2(graph);
			break;
		case "1_3_not_olap":
			QueriesNotOlap.query1_3(graph);
			break;
		case "1_4_not_olap":
			QueriesNotOlap.query1_4(graph);
			break;
		case "1_5_not_olap":
			QueriesNotOlap.query1_5(graph);
			break;
		case "1_6_not_olap":
			QueriesNotOlap.query1_6(graph);
			break;
		case "2_1_not_olap":
			QueriesNotOlap.query2_1(graph);
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

}
