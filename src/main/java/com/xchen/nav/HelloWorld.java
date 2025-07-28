package com.xchen.nav;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.definitions.ColumnDefinition;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;

public class HelloWorld {

    public static void main(String[] args) {
        try (IgniteClient client = IgniteClient.builder().addresses("127.0.0.1:10800").build()) {
            System.out.println("--- Querying Person table --");
            client.sql().execute(null, "SELECT * FROM Person")
                    .forEachRemaining(row -> System.out.println("Persion: " + row.stringValue("name")));

            System.out.println("--- Creating Person2 table ---");
            Table table = client.catalog().createTable(
                    TableDefinition.builder("Person2")
                            .ifNotExists()
                            .columns(
                                    ColumnDefinition.column("ID", ColumnType.INT32),
                                    ColumnDefinition.column("NAME", ColumnType.VARCHAR)
                            )
                            .primaryKey("ID")
                            .build()
            );


            System.out.println("--- Populating Person2 table using different views ---");
            // 1. Using RecordView with Tuples
            RecordView<Tuple> recordView = table.recordView();
            recordView.upsert(null, Tuple.create().set("id", 2).set("name", "Jane"));
            System.out.println("Added record using RecordView with Tuple");
            // 2. Using RecordView With POJOS
            RecordView<Person> pojoView = table.recordView(Person.class);
            pojoView.upsert(null, new Person(3, "Jack"));
            System.out.println("Added record using KeyValueView with POJO");

            // 3. Using KeyValueView with Tuples
            KeyValueView<Tuple, Tuple> keyValueView = table.keyValueView();
            keyValueView.put(null, Tuple.create().set("id", 4), Tuple.create().set("name", "Jill"));
            System.out.println("Added record using KeyValueView with Tuples");

            // 4. Using KeyValueView with Native Types
            KeyValueView<Integer, String> keyValuePojoView = table.keyValueView(Integer.class, String.class);
            keyValuePojoView.put(null, 5, "Joe");
            System.out.println("Added record using KeyValuePojoView with Types");

            // Queries the newly created Person2 table using SQL
            System.out.println("--- Querying Person2 table ---");
            client.sql().execute(null, "SELECT * FROM Person2")
                    .forEachRemaining(row -> System.out.println("Person2: "+ row.stringValue("name")));
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class Person {
        private Integer id;
        private String name;
    }
}
