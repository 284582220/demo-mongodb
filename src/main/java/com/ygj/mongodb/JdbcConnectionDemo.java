package com.ygj.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.*;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Position;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.or;

public class JdbcConnectionDemo {
    public static void main(String[] args) {
        //连接test数据库
        MongoClient mongoClient = new MongoClient("172.19.100.130", 27017);
        MongoDatabase database = mongoClient.getDatabase("test");
        MongoCollection<Document> collection = database.getCollection("user");
        findTest(collection);
//        updateTest(collection);
        mapReduceTest(collection);

//        findTest(collection);
    }

    private static void findTest(MongoCollection<Document> collection){
        //与查询并输出结果
        System.out.println("查询:");
                FindIterable<Document> result2 = collection.find();
//        FindIterable<Document> result=collection.find(new Document("age", new Document("$gte", 20).append("$lt", 21)).append("likeNum", 9));
//
//        FindIterable<Document> result2 = collection.find(or(eq("age", 18), eq("age", 19)));

        MongoCursor<Document> ll =  result2.iterator();
        while (ll.hasNext()){
            Document docc = ll.next();
            System.out.println(docc.toJson());
        }



//
//        String user:
//        String database;
//        char[] password;
//        MongoCredential credential = MongoCredential.createCredential(user, database, password);
//        MongoClient mongoClient1 = new MongoClient(new ServerAddress("host1", 27017), Arrays.asList(credential));

        // 不同加密机制的连接方式参见官网

//        Pattern pattern = Pattern.compile("user");
//        BasicDBObject query = new BasicDBObject("name", pattern);
//        FindIterable<Document> result3 = collection.find(query);
//        MongoCursor<Document> l =  result3.iterator();
//        while (l.hasNext()){
//            Document docc = l.next();
//            System.out.println(docc);
//        }

//        // 地理位置查询
//        collection.createIndex(Indexes.geo2dsphere("contact.location"));
//        Point refPoint = new Point(new Position(-73.92505, 40.8279556));
//        FindIterable<Document> result2 = collection.find(Filters.near("contact.location", refPoint, 5000000000.0, 2.0));
//        MongoCursor<Document> ll =  result2.iterator();
//        while (ll.hasNext()){
//            Document docc = ll.next();
//            System.out.println(docc.toJson());
//        }
    }

    private static void insertTest(MongoCollection<Document> collection){
        //插入数据
        List<Document> datas = new ArrayList<Document>();
        for (int i=1; i < 10; i++) {
            String userName="user"+i;
            Document document = new Document("name",userName)
                    .append("contact", new Document("phone", "228-555-0149"+i)
                            .append("email", userName+"@example.com")
                            .append("location", Arrays.asList(-73.92502+i, 40.8279556)))
                    .append("age",18+i)
                    .append("likeNum", Arrays.asList(i, 8, 9));
            datas.add(document);
        }
        collection.insertMany(datas);
    }

    private static void updateTest(MongoCollection<Document> collection){
        // 更新操作
        System.out.print("更新操作");
//        UpdateResult result = collection.updateOne(new Document("age", 19), new Document("$set",new Document("contact.phone", "000-2231-1289")));
//        System.out.print(result.getMatchedCount());

        UpdateResult result = collection.updateMany(new Document("age", new Document("$gte", 20)), new Document("$push", new Document("likeNum", 0)));
        System.out.print(result.getModifiedCount());


    }


    private static void deleteOne(MongoCollection<Document> collection){
        DeleteResult deleteResult = collection.deleteOne(new Document("age", new Document("$gte", 20)));
        System.out.print(deleteResult.getDeletedCount());

    }

    private static void deleteMany(MongoCollection<Document> collection){
        DeleteResult deleteResult = collection.deleteMany(new Document("age", new Document("$gte", 10)));
        System.out.print(deleteResult.getDeletedCount());

    }

    private static void countTest(MongoCollection<Document> collection){
        //
        System.out.println("聚合运算");
        long count = collection.count(new BasicDBObject("age", 19));
        System.out.println(count);
        DistinctIterable<Integer> result = collection.distinct("age", new Document("likeNum", 9), Integer.class);
//        System.out.println(result.first());

        MongoCursor<Integer> ll =  result.iterator();
        while (ll.hasNext()){
            Integer docc = ll.next();
            System.out.println(docc);
        }
    }

    private static void mapReduceTest(MongoCollection<Document> collection){
        String map = "function Map(){if(this.name=='user3'){emit('result',this);}}";
        String reduce = "function Reduce(key, values) {return values[0];}";
        MapReduceIterable<Document> out = collection.mapReduce(map, reduce);
        for(Document u : out){
            System.out.println(u);
        }
    }

    private static void aggregateTest(MongoCollection<Document> collection){
        AggregateIterable<Document> result = collection.aggregate(
                Arrays.asList(
                        Aggregates.match(Filters.eq("likeNum", 9)),
                        Aggregates.group("$age", Accumulators.sum("count", 1))
                )
        );

        collection.aggregate(Arrays.asList(Aggregates.match(Filters.eq("likeNum", 9)), Aggregates.group("$age", Accumulators.sum("count", 1))));

    }

}
