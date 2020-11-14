import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestConn {
    @Test
    public void test1(){
        //实例化一个mongo客户端
        MongoClient  mongoClient=new MongoClient("192.168.2.131");
        //实例化一个mongo数据库
        MongoDatabase mongoDatabase = mongoClient.getDatabase("test");
        //获取数据库中某个集合
        MongoCollection<Document> collection = mongoDatabase.getCollection("student");
        //实例化一个文档,内嵌一个子文档
        Document document=new Document("name","scofield").
                append("score", new Document("English",45).
                        append("Math", 89).
                        append("Computer", 100));
        List<Document> documents = new ArrayList<Document>();
        documents.add(document);
        //将文档插入集合中
        collection.insertMany(documents);
        System.out.println("文档插入成功");

    }


    @Test
    public void test2() {
        // 连接 MongoDB 数据库
        MongoClient client = new MongoClient("192.168.2.131");
        // 得到要操作的数据库
        MongoDatabase database = client.getDatabase("test");
        // 得到要操作的集合
        MongoCollection<Document> student = database.getCollection("student");
        // 得到集合中的所有文档
        //进行数据查找,查询条件为name=scofield, 对获取的结果集只显示score这个域
        MongoCursor<Document> cursor = student.find(new Document("name", "scofield")).
                projection(new Document("score", 1).append("_id", 0)).iterator();

        while (cursor.hasNext())
            System.out.println(cursor.next().toJson());

        client.close();
    }
}
