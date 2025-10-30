package com.example;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import org.bson.Document;

import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.*;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.lang.String;

import com.mongodb.ConnectionString;
import org.bson.conversions.Bson;

//import oracle.ucp.jdbc.PoolDataSource;
//import oracle.ucp.jdbc.PoolDataSourceFactory;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.pool.OracleDataSource;
//import java.sql.Connection;
//import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MongoDBExampleTest {
    public static void main(String[] args) {
	// Paswword: dkd134##BB--13, special character, %23 = #
        ConnectionString connectionString = new ConnectionString("mongodb://mongotest:dkd134%23%23BB--13@o4kkqjgi.adb.ap-seoul-1.oraclecloudapps.com:27017/mongotest?authMechanism=PLAIN&authSource=$external&ssl=true&retryWrites=false&loadBalanced=true");
        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            // Access the database
            MongoDatabase database = mongoClient.getDatabase("mongotest");

            // Access the collection
            MongoCollection<Document> collection = database.getCollection("mycollection");

            // Create a new document
            Document document = new Document("color", "green").append("qty", 2).append("type",1);

            // Insert the document into the collection
            collection.insertOne(document);
            System.out.println("insertOne 1 Document successfully!");

            //print document
            System.out.println("Get All Documents List:");
            FindIterable<Document> iterable1 = collection.find();
            iterable1.forEach(doc -> System.out.println(doc.toJson()));

            //InsertMany
            List<Document> documents = new ArrayList<>();
            Document doc1 = new Document("color", "red").append("qty", 10).append("type",1);
            Document doc2 = new Document("color", "purple").append("qty", 10).append("type",1);
            Document doc3 = new Document("color", "blue").append("qty", 20).append("type",1);
            Document doc4 = new Document("color", "black").append("qty", 30).append("type",1);
            Document doc5 = new Document("color", "white").append("qty", 5).append("type",2);
            Document doc6 = new Document("color", "gray").append("qty", 3).append("type",2);
            documents.add(doc1);
            documents.add(doc2);
            documents.add(doc3);
            documents.add(doc4);
            documents.add(doc5);
            documents.add(doc6);
            collection.insertMany(documents);
            System.out.println("insertMany 6 Documents successfully!");

            //print document
            System.out.println("Get All Documents List:");
            FindIterable<Document> iterable2= collection.find();
            iterable2.forEach(doc -> System.out.println(doc.toJson()));

            Bson filter = Filters.eq("color", "purple");
            collection.deleteOne(filter);
            System.out.println("Delete Documents: color:purple, successfully");
            //print document
            System.out.println("Get All Documents List:");
            FindIterable<Document> iterable3= collection.find();
            iterable3.forEach(doc -> System.out.println(doc.toJson()));

            Bson filter2 = Filters.eq("color", "red");
            Document document2 = new Document("color", "orange").append("qty", 25).append("type",1);
            collection.replaceOne(filter2, document2);
            System.out.println("replace Documents: color:red -> color:orange successfully");
            //print document
            System.out.println("Get All Documents List:");
            FindIterable<Document> iterable4= collection.find();
            iterable4.forEach(doc -> System.out.println(doc.toJson()));

            // Query for documents
            Document query = new Document("color", "blue");
            Document foundDocument = collection.find(query).first();
            if (foundDocument != null) {
                System.out.println("Found Document: " + foundDocument.toJson());
            } else {
                System.out.println("Document not found.");
            }

            //Aggregation操作，相当于进行MongoDB如下查询
            // https://docs.oracle.com/en/database/oracle/mongodb-api/mgapi/develop-applications-oracle-database-api-mongodb.html#GUID-1482207B-A594-44EF-957D-89A24311A2B5
            //db.mycollection.aggregate(
            //  [{"$group" : {"_id"    : "$type",
            //                "avgRev" : {"$avg" : "$qty"}}},
            //   {"$sort"  : {"avgRev" : -1}}])
            System.out.println("Execute Aggregation:");
            String outlines =
            "AggregateIterable<Document> result = collection.aggregate(Arrays.asList(\n" +
               "group(\"$type\", avg(\"avgRev\", \"$qty\")),\n"+
               "project(new Document(\"type\", \"$_id\").append(\"avgRev\", 1).append(\"_id\", 0)),\n"+
               "sort(descending(\"avgRev\"))\n"+
            "));";

            System.out.println(outlines);
            /* Method 1
             AggregateIterable<Document> result = collection.aggregate(Arrays.asList(
               new Document("$group", new Document("_id", "$type")
                .append("avgRev", new Document("$avg", "$qty"))),
                new Document("$project", new Document("type", "$_id").append("avgRev", 1).append("_id", 0)),
                new Document("$sort", new Document("avgRev", -1))
            ));
            */
            AggregateIterable<Document> result = collection.aggregate(Arrays.asList(
               group("$type", avg("avgRev", "$qty")),
               project(new Document("type", "$_id").append("avgRev", 1).append("_id", 0)),
               sort(descending("avgRev"))
            ));

            System.out.println("Aggregation Result:");
            // Print the results
            for (Document doc : result) {
                System.out.println(doc.toJson());
            }

            //执行完后，删除collection
            collection.drop();

            mongoClient.close();

        }
    }
}

/***************************************************
运行结果输出

Sep 24, 2024 9:17:37 AM com.mongodb.internal.diagnostics.logging.Loggers shouldUseSLF4J
WARNING: SLF4J not found on the classpath.  Logging is disabled for the 'org.mongodb.driver' component
insertOne 1 Document successfully!
Get All Documents List:
{"_id": {"$oid": "66f283b131bf3535d1d276c1"}, "color": "green", "qty": 2, "type": 1}
insertMany 6 Documents successfully!
Get All Documents List:
{"_id": {"$oid": "66f283b131bf3535d1d276c1"}, "color": "green", "qty": 2, "type": 1}
{"_id": {"$oid": "66f283b231bf3535d1d276c2"}, "color": "red", "qty": 10, "type": 1}
{"_id": {"$oid": "66f283b231bf3535d1d276c3"}, "color": "purple", "qty": 10, "type": 1}
{"_id": {"$oid": "66f283b231bf3535d1d276c4"}, "color": "blue", "qty": 20, "type": 1}
{"_id": {"$oid": "66f283b231bf3535d1d276c5"}, "color": "black", "qty": 30, "type": 1}
{"_id": {"$oid": "66f283b231bf3535d1d276c6"}, "color": "white", "qty": 5, "type": 2}
{"_id": {"$oid": "66f283b231bf3535d1d276c7"}, "color": "gray", "qty": 3, "type": 2}
Delete Documents: color:purple, successfully
Get All Documents List:
{"_id": {"$oid": "66f283b131bf3535d1d276c1"}, "color": "green", "qty": 2, "type": 1}
{"_id": {"$oid": "66f283b231bf3535d1d276c2"}, "color": "red", "qty": 10, "type": 1}
{"_id": {"$oid": "66f283b231bf3535d1d276c4"}, "color": "blue", "qty": 20, "type": 1}
{"_id": {"$oid": "66f283b231bf3535d1d276c5"}, "color": "black", "qty": 30, "type": 1}
{"_id": {"$oid": "66f283b231bf3535d1d276c6"}, "color": "white", "qty": 5, "type": 2}
{"_id": {"$oid": "66f283b231bf3535d1d276c7"}, "color": "gray", "qty": 3, "type": 2}
replace Documents: color:red -> color:orange successfully
Get All Documents List:
{"_id": {"$oid": "66f283b131bf3535d1d276c1"}, "color": "green", "qty": 2, "type": 1}
{"_id": {"$oid": "66f283b231bf3535d1d276c2"}, "color": "orange", "qty": 25, "type": 1}
{"_id": {"$oid": "66f283b231bf3535d1d276c4"}, "color": "blue", "qty": 20, "type": 1}
{"_id": {"$oid": "66f283b231bf3535d1d276c5"}, "color": "black", "qty": 30, "type": 1}
{"_id": {"$oid": "66f283b231bf3535d1d276c6"}, "color": "white", "qty": 5, "type": 2}
{"_id": {"$oid": "66f283b231bf3535d1d276c7"}, "color": "gray", "qty": 3, "type": 2}
Found Document: {"_id": {"$oid": "66f283b231bf3535d1d276c4"}, "color": "blue", "qty": 20, "type": 1}
Execute Aggregation:
AggregateIterable<Document> result = collection.aggregate(Arrays.asList(
group("$type", avg("avgRev", "$qty")),
project(new Document("type", "$_id").append("avgRev", 1).append("_id", 0)),
sort(descending("avgRev"))
));
Aggregation Result:
{"avgRev": 19.25, "type": 1}
{"avgRev": 4.0, "type": 2}
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  3.286 s
[INFO] Finished at: 2024-09-24T09:17:39Z
[INFO] ------------------------------------------------------------------------
**************************************************/
