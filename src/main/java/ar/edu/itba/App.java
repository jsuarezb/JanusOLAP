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
    if (args.length < 2) {
      throw new IllegalArgumentException("Expecting path and query as arguments.");
    }
    // TODO: Add arguments to be able to clean graph from cli.
    String path = args[0];
    String query = args[1];
    boolean printTuples = args.length >= 3 ? Boolean.valueOf(args[2]) : false;
    String user = null;
    int threshold = -1;
    if (query.startsWith("final_2_d")) {
      if (args.length < 4) {
        throw new IllegalArgumentException("Expecting user and threshold as arguments");
      }
      user = args[2];
      threshold = Integer.valueOf(args[3]);
      printTuples = args.length >= 5 ? Boolean.valueOf(args[4]) : false;
    }
    new App(path, query, printTuples, user, threshold);
  }

  public App(String path, String query, boolean printTuples, String user, int threshold)
      throws Exception {
    // Sets the package level to INFO
    LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
    Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
    rootLogger.setLevel(Level.OFF);

    QueriesOlap.printTuples = printTuples;
    QueriesNotOlap.printTuples = printTuples;
    FinalQueriesNotOlap.printTuples = printTuples;
    FinalQueriesKnownAlgorithms.printTuples = printTuples;

    boolean isOpen = false;
    boolean wasCleaned = false;
    boolean cleanUp = false; // TODO: Enable?
    while (!isOpen) {
      graph = JanusGraphFactory.build().set("storage.backend", "cassandra")
          .set("storage.hostname", "node2,node3,node4")
          .set("storage.cassandra.replication-factor", 2)
          .set("storage.cassandra.keyspace", "tcolloca")
          .set("schema.default", "none").set("storage.username", "tcolloca")
          .set("storage.password", "tcolloca")
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
      case "final_2_a":
        FinalQueriesNotOlap.query2_a(graph);
        break;
      case "final_2_b":
        FinalQueriesNotOlap.query2_b(graph);
        break;
      case "final_2_c":
        FinalQueriesNotOlap.query2_c(graph);
        break;
      case "final_2_d":
        FinalQueriesNotOlap.query2_d(graph, user, threshold);
        break;
      case "final_2_a_other":
        FinalQueriesKnownAlgorithms.query2_a(graph);
        break;
      case "final_2_b_other":
        FinalQueriesKnownAlgorithms.query2_b(graph);
        break;
      case "final_2_c_other":
        FinalQueriesKnownAlgorithms.query2_c(graph);
        break;
      case "final_2_d_other":
        FinalQueriesKnownAlgorithms.query2_d(graph, user, threshold);
        break;
    }
    long end = System.currentTimeMillis();
    System.out.println("Total time: " + (end - start) + " ms");

    graph.close();
  }

  public static String toString(Edge edge) {
    StringBuilder strBuilder = new StringBuilder();
    strBuilder.append(toString(edge.outVertex(), "value", "duration"));
    strBuilder.append("- " + edge.label() + " -> ");
    strBuilder.append(toString(edge.inVertex(), "value", "duration"));
    return strBuilder.toString();
  }

  public static String toString(Vertex vertex, String... keys) {
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

    addPropertyKey("value", String.class, Cardinality.SINGLE, true);
    addPropertyKey("type", String.class, Cardinality.SINGLE, true);

    addPropertyKey("duration", Double.class, Cardinality.LIST, false);
    addPropertyKey("visited", Boolean.class, Cardinality.SINGLE, true);

    addVertexLabel("phone");
    addVertexLabel("user");
    addVertexLabel("city");
    addVertexLabel("country");
    addVertexLabel("allLocations");

    addVertexLabel("operator");
    addVertexLabel("allOperators");

    addVertexLabel("timestamp");
    addVertexLabel("day");
    addVertexLabel("month");
    addVertexLabel("year");
    addVertexLabel("allTimes");

    addVertexLabel("call");

    addEdgeLabel("calledBy");
    addEdgeLabel("integratedBy");
    addEdgeLabel("atTime");

    addEdgeLabel("extendsFrom");


    mgmt.commit();
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

    PropertyKey prop =
        mgmt.makePropertyKey(label).dataType(dataType).cardinality(cardinality).make();
    if (index) {
      mgmt.buildIndex(label, Vertex.class).addKey(prop).buildCompositeIndex();
    }
    return prop;
  }

}
