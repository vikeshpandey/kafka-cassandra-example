package io.github.examples;

public class ReadData {

    public static void main(String[] args) {
        CassandraConsumer cassandraConsumer = new CassandraConsumer();
        cassandraConsumer.connect("localhost",9042);
        cassandraConsumer.queryData();
        cassandraConsumer.close();
    }
}
