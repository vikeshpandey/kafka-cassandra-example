package io.github.examples;

import com.datastax.driver.core.*;

import java.util.UUID;



public class CassandraConsumer {
    private Cluster cluster;
    private Session session;

    public void connect(String node, Integer port) {
        Cluster.Builder b = Cluster.builder().addContactPoint(node);
        if (port != null) {
            b.withPort(port);
        }
        cluster = b.build();

        session = cluster.connect();
    }

    public void getSession() {
        session = cluster.connect();
    }

    public void closeSession() {
        session.close();
    }

    public void close() {
        cluster.close();
    }


    public void insertData(String data) {
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append("demo_101.cricket_lovers").append("(id, body) ")
                .append("VALUES (").append(UUID.randomUUID())
                .append(", '").append(data).append("');");

        String query = sb.toString();
        session.execute(query);
        System.out.println("record saved successfully");
    }

    public void queryData() {
        StringBuilder sb =
                new StringBuilder("SELECT * FROM ").append("demo_101.cricket_lovers");

        String query = sb.toString();
        ResultSet rs = session.execute(query);

        rs.forEach(r -> {
            System.out.println("id is: " + r.getUUID("id")+" and data is : "+r.getString("body"));
        });
    }
}

