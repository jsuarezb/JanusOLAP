package ar.edu.itba;

import static ar.edu.itba.GraphUtils.getNeighborPhones;
import static ar.edu.itba.GraphUtils.getUser;
import static ar.edu.itba.GraphUtils.getUserName;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;

public class FinalQueriesKnownAlgorithms {

  public static boolean printTuples;

  public static void query2_a(JanusGraph graph) {
    Map<String, Map<String, Integer>> distancesMap = shortestDistanceBetweenUsers(graph, null);
    PrintUtils.printDistances(distancesMap, printTuples);
  }

  public static void query2_b(JanusGraph graph) {
    Map<String, Map<String, Integer>> distancesMap =
        shortestDistanceBetweenUsers(graph, QueryFilters.query2_b_filter);
    PrintUtils.printDistances(distancesMap, printTuples);
  }

  public static void query2_c(JanusGraph graph) {
    Map<String, Map<String, Integer>> distancesMap =
        shortestDistanceBetweenUsers(graph, QueryFilters.query2_c_filter);
    PrintUtils.printDistances(distancesMap, printTuples);
  }

  public static void query2_d(JanusGraph graph, String user, int threshold) {
    Map<String, Map<String, Integer>> distancesMap =
        shortestDistanceBetweenUsers(graph, user, threshold);
    PrintUtils.printDistances(distancesMap, printTuples);
  }

  /**
   * Prints the shortest distance between users applying the corresponding filter, using
   * Floyd-Warshall algorithm.
   * 
   * @param graph Graph to apply the algorithm.
   * @param filter Filter to be applied, that receives two phone vertices. (i.e: User phones not
   *        from the same city, or calls between phones with different operator). If null, a dummy
   *        always-true filter is applied.
   */
  public static Map<String, Map<String, Integer>> shortestDistanceBetweenUsers(JanusGraph graph,
      BiFunction<Vertex, Vertex, Boolean> filter) {

    Data data = new Data(graph);

    for (int k = 0; k < data.vSize; k++) {
      for (int i = 0; i < data.vSize; i++) {
        for (int j = 0; j < data.vSize; j++) {
          int distIJ = data.phoneDistancesMap.get(i).getOrDefault(j, Integer.MAX_VALUE);
          int distIK = data.phoneDistancesMap.get(i).getOrDefault(k, Integer.MAX_VALUE);
          int distKJ = data.phoneDistancesMap.get(k).getOrDefault(j, Integer.MAX_VALUE);

          if (distIK < Integer.MAX_VALUE && distKJ < Integer.MAX_VALUE &&
              distIJ > distIK + distKJ) {
            data.phoneDistancesMap.get(i).put(j, distIK + distKJ);
          }
        }
      }
    }

    return data.getDistancesMap(filter, null);
  }

  /**
   * Prints the shortest distance between users applying the corresponding filter, using
   * Dijkstra/BFS algorithm.
   * 
   * @param graph Graph to apply the algorithm.
   * @param user Source user to apply the algorithm to. If null the algorithm is applied for every
   *        pair of users.
   * @param threshold Distance between users will be lower than threshold. If is negative, then it
   *        is considered as Integer.MAX_VALUE.
   */
  public static Map<String, Map<String, Integer>> shortestDistanceBetweenUsers(JanusGraph graph,
      String user, int threshold) {
    // Base case for threshold 0 and 1.
    if (threshold == 0) {
      return new HashMap<>();
    } else if (threshold == 1) {
      Map<String, Map<String, Integer>> map = new HashMap<>();
      map.computeIfAbsent(user, x -> new HashMap<>()).put(user, 0);
      return map;
    }

    Data data = new Data(graph);

    Set<Integer> phoneIds = data.phoneDataIdsOf(user);

    // Apply Dijkstra's for each user's phone.
    for (Integer phoneId : phoneIds) {
      // Initialize Dijkstra's priority queue.
      PriorityQueue<PqNode> pq = new PriorityQueue<>();
      pq.offer(new PqNode(phoneId, 0));
      Set<Integer> reached = new HashSet<>();
      while (!pq.isEmpty() && (threshold < 0 || pq.peek().depth < threshold)) {
        PqNode curr = pq.poll();
        // If the vertex was already reached, ignore it.
        if (reached.contains(curr.id)) {
          continue;
        }
        reached.add(curr.id);
        data.phoneDistancesMap.get(phoneId).putIfAbsent(curr.id, curr.depth);
        // For every neighbor not yet reached of the current one, add to the priority queue a node
        // with the sum of the distances to it.
        for (Map.Entry<Integer, Integer> edge : data.phoneDistancesMap.get(curr.id).entrySet()) {
          int neigh = edge.getKey();
          int dist = edge.getValue();
          if (!reached.contains(neigh)) {
            pq.offer(new PqNode(neigh, curr.depth + dist));
          }
        }
      }
    }

    return data.getDistancesMap(null, user);
  }


  private static class PqNode implements Comparable<PqNode> {
    int id;
    int depth;

    PqNode(int id, int depth) {
      this.id = id;
      this.depth = depth;
    }

    @Override
    public int compareTo(PqNode o) {
      if (depth == o.depth) {
        return id - o.id;
      }
      return depth - o.depth;
    }
  }


  /**
   * Extracts data from graph. Maps graph id to data id in order to apply Floyd-Warshall algorithm
   * more easily.
   */
  private static class Data {

    private final JanusGraph graph;

    private final Map<Object, Integer> graphIdToDataIdMap = new HashMap<>();
    private final Map<Integer, Object> dataIdToGraphIdMap = new HashMap<>();
    private final Map<Integer, Map<Integer, Integer>> phoneDistancesMap = new HashMap<>();
    private final Map<String, Map<String, Integer>> userDistancesMap = new TreeMap<>();

    private final int vSize;

    Data(JanusGraph graph) {
      this.graph = graph;
      GraphTraversal<Vertex, Vertex> phones = graph.traversal().V().has("type", "phone");

      final AtomicInteger id = new AtomicInteger(0);

      phones.toStream().forEach(phone1 -> {
        // Add phone1 to maps.
        Object graphId1 = phone1.id();
        if (graphIdToDataIdMap.putIfAbsent(graphId1, id.get()) == null) {
          dataIdToGraphIdMap.put(id.getAndIncrement(), graphId1);
        }
        int dataId1 = graphIdToDataIdMap.get(graphId1);
        // Add distance between phone1 and phone1 as 0.
        phoneDistancesMap.computeIfAbsent(dataId1, x -> new HashMap<>()).put(dataId1, 0);

        getNeighborPhones(graph, phone1)
            .toStream().forEach(phone2 -> {

              // Add phone2 to maps.
              Object graphId2 = phone2.id();
              if (graphIdToDataIdMap.putIfAbsent(graphId2, id.get()) == null) {
                dataIdToGraphIdMap.put(id.getAndIncrement(), graphId2);
              }
              int dataId2 = graphIdToDataIdMap.get(graphId2);
              if (dataId1 == dataId2) {
                return;
              }
              // Add distance between phone1 and phone2 as 1.
              phoneDistancesMap.get(dataId1).put(dataId2, 1);
            });
      });

      vSize = id.get();
    }

    public Set<Integer> phoneDataIdsOf(String user) {
      return GraphUtils.phoneIdsOf(graph, user).stream()
          .map(graphId -> graphIdToDataIdMap.get(graphId))
          .collect(Collectors.toSet());
    }

    public Map<String, Map<String, Integer>> getDistancesMap(
        BiFunction<Vertex, Vertex, Boolean> filter, String user) {
      if (!userDistancesMap.isEmpty()) {
        return userDistancesMap;
      }
      for (Map.Entry<Integer, Map<Integer, Integer>> e1 : phoneDistancesMap.entrySet()) {
        // Get phone1 and user 1
        int dataId1 = e1.getKey();
        Object graphId1 = dataIdToGraphIdMap.get(dataId1);
        Vertex phone1 = graph.vertices(graphId1).next();
        String userName1 = getUserName(getUser(phone1));

        if (user != null && !userName1.equals(user)) {
          continue;
        }

        // Add user1 to map.
        userDistancesMap.putIfAbsent(userName1, new TreeMap<>());
        for (Map.Entry<Integer, Integer> e2 : e1.getValue().entrySet()) {
          // Get phone2 and user2.
          int dataId2 = e2.getKey();
          int distance = e2.getValue();
          Object graphId2 = dataIdToGraphIdMap.get(dataId2);
          Vertex phone2 = graph.vertices(graphId2).next();

          // Make sure filter applies if not null.
          if (filter == null || filter.apply(phone1, phone2)) {
            String userName2 = getUserName(getUser(phone2));

            // Update distance between user1 and user2.
            userDistancesMap.get(userName1).compute(userName2,
                (x, currDist) -> currDist == null ? distance : (int) Math.min(currDist, distance));
          }
        }
      }

      // Add username - username with distance 0 for every user o just `user` if provided.
      if (user == null) {
        graph.traversal().V().has("type", "user").toSet()
            .stream()
            .forEach(userV -> {
              String userName = getUserName(userV);
              userDistancesMap.computeIfAbsent(userName, x -> new TreeMap<>()).put(userName, 0);
            });
      } else {
        userDistancesMap.computeIfAbsent(user, x -> new TreeMap<>()).put(user, 0);
      }

      return userDistancesMap;
    }
  }
}
