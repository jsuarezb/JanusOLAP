package ar.edu.itba;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataReader {

    private HashMap<String, JanusGraphVertex> usersMap = new HashMap<>();
    private HashMap<String, JanusGraphVertex> operatorsMap = new HashMap<>();
    private HashMap<String, JanusGraphVertex> citiesMap = new HashMap<>();
    private HashMap<String, JanusGraphVertex> countriesMap = new HashMap<>();
    private HashMap<String, JanusGraphVertex> yearsMap = new HashMap<>();
    private HashMap<String, JanusGraphVertex> monthYearsMap = new HashMap<>();
    private HashMap<String, JanusGraphVertex> datesMap = new HashMap<>();
    private HashMap<String, JanusGraphVertex> timestampMap = new HashMap<>();
    private HashMap<String, JanusGraphVertex> allsMap = new HashMap<>();
    private HashMap<String, JanusGraphVertex> phonesMap = new HashMap<>();

    private HashMap<Long, JanusGraphVertex> phoneIdsMap = new HashMap<>();
    private HashMap<Long, JanusGraphVertex> dateTimesIdsMap = new HashMap<>();
    private HashMap<Long, JanusGraphVertex> callIdsMap = new HashMap<>();

    private JanusGraph graph;
    private String path;

    public DataReader(final JanusGraph graph, final String path) {
        this.path = path;
        this.graph = graph;
    }

    public void buildGraph() throws IOException {
        newAll("Locations");
        newAll("Operators");
        newAll("Times");


        parseUsers();
        parseDateTimes();
        parseCalls();
    }

    private void parseUsers() throws IOException {
        JanusGraphTransaction tx = graph.newTransaction();
        List<String> lines = Files.readAllLines(Paths.get(path + "/emisorreceptor.csv"));

        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            String[] vars = line.split(",");
            long id = Long.parseLong(vars[0]);
            String phone = vars[1];
            String operator = vars[2];
            String user = vars[3];
            String city = vars[4];
            String country = vars[5];

            addVertexAndEdge(allsMap.get("Locations"), countriesMap, "country", country);
            addVertexAndEdge(countriesMap.get(country), citiesMap, "city", city);
            addVertexAndEdge(citiesMap.get(city), usersMap, "user", user);
            addVertexAndEdge(allsMap.get("Operators"), operatorsMap, "operator", operator);

            addPhoneVertexAndEdge(operatorsMap.get(operator), usersMap.get(user), phonesMap, "phone", phone);

            phoneIdsMap.put(id, phonesMap.get(phone));

            if (i % 1000 == 0) {
                System.out.println(String.format("User progress: %.2f %%", i * 100 / (double) lines.size()));
                tx.commit();
                tx = graph.newTransaction();
            }
        }

        tx.commit();
    }

    private void parseDateTimes() throws IOException {
        JanusGraphTransaction tx = graph.newTransaction();
        List<String> lines = Files.readAllLines(Paths.get(path + "/datetime.csv"));

        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            String[] vars = line.split(",");
            long id = Long.parseLong(vars[0]);
            String time = vars[1].split(" ")[1];
            String day = vars[2];
            String month = vars[3];
            String year = vars[4];
            String monthYear = month + "-" + year;
            String date = day + "-" + monthYear;
            String timestamp = time + " " + date;

            addVertexAndEdge(allsMap.get("Times"), yearsMap, "year", year);
            addVertexAndEdge(yearsMap.get(year), monthYearsMap, "month", monthYear);
            addVertexAndEdge(monthYearsMap.get(monthYear), datesMap, "day", date);
            addVertexAndEdge(datesMap.get(date), timestampMap, "timestamp", timestamp);

            dateTimesIdsMap.put(id, timestampMap.get(timestamp));

            if (i % 1000 == 0) {
                System.out.println(String.format("Date time progress: %.2f %%", i * 100 / (double) lines.size()));
                tx.commit();
                tx = graph.newTransaction();
            }
        }

        tx.commit();
    }

    private void parseCalls() throws IOException {
        JanusGraphTransaction tx = graph.newTransaction();
        List<String> lines = Files.readAllLines(Paths.get(path + "/call.csv"));

        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            String[] vars = line.split(",");
            long id = Long.parseLong(vars[0]);
            long dateTimeId = Long.parseLong(vars[1]);
            long callerId = Long.parseLong(vars[2]);
            long memberId = Long.parseLong(vars[3]);
            int duration = Integer.parseInt(vars[4]);

            newCall(id, phoneIdsMap.get(callerId), phoneIdsMap.get(memberId), dateTimesIdsMap.get(dateTimeId), duration);

            if (i % 1000 == 0) {
                System.out.println(String.format("Call progress: %.2f %%", i * 100 / (double) lines.size()));
                tx.commit();
                tx = graph.newTransaction();
            }
        }

        tx.commit();
    }

    private void newAll(String dimension) {
        if (allsMap.containsKey(dimension))
            return;

        JanusGraphVertex vertex = graph.addVertex("all" + dimension);
        vertex.property("value", "all" + dimension);

        allsMap.put(dimension, vertex);
    }

    private void addVertexAndEdge(JanusGraphVertex parent, Map<String, JanusGraphVertex> levelMap, String label, String value) {
        if (levelMap.containsKey(value))
            return;

        JanusGraphVertex vertex = graph.addVertex(label);
        vertex.property("value", value);
        vertex.addEdge("extendsFrom", parent);

        levelMap.put(value, vertex);
    }

    private void addPhoneVertexAndEdge(JanusGraphVertex parentOperator, JanusGraphVertex parentUser,
                                       Map<String, JanusGraphVertex> levelMap, String label, String value) {
        if (levelMap.containsKey(value))
            return;

        JanusGraphVertex vertex = graph.addVertex(label);
        vertex.property("value", value);
        vertex.addEdge("extendsFrom", parentOperator);
        vertex.addEdge("extendsFrom", parentUser);

        levelMap.put(value, vertex);
    }

    private void newCall(long callId, JanusGraphVertex callerVertex, JanusGraphVertex memberVertex,
                         JanusGraphVertex timeVertex, int duration) {
        if (!callIdsMap.containsKey(callId)) {
            JanusGraphVertex vertex = graph.addVertex("call");
            vertex.property("duration", duration);

            vertex.addEdge("calledBy", callerVertex);
            vertex.addEdge("atTime", timeVertex);

            callIdsMap.put(callId, vertex);
        }

        callIdsMap.get(callId).addEdge("integratedBy", memberVertex);
    }
}
