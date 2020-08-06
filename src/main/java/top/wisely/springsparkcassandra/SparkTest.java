package top.wisely.springsparkcassandra;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import com.google.common.collect.Maps;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
/**
 * @Author: QuanJiaXing
 * @Description:
 * @Date: 2020/8/5 23:07
 * @Version: 1.0
 * @Modified by:
 */
public class SparkTest {
    public static void main(String[] args) {
        SparkConf conf= new SparkConf(true).set("spark.cassandra.connection.host", "47.92.196.164,39.100.92.216,39.100.64.104");
        SparkContext sc = new SparkContext("spark://iZ8vba8r09vdt7osvopu2nZ:7077", "test", conf);
        CassandraJavaRDD rdd = javaFunctions(sc).cassandraTable("test", "words");
        CassandraJavaRDD result = rdd.where("word in ('foo', 'fo2') and count > 1 and count < 8");
        result.foreach(row -> System.out.println(row));

//        System.out.print(rdd.cassandraCount());
//        System.out.print(result.collect().toArray());
//        官网类似方法



//        Map<String, String> options = Maps.newHashMap();
//        options.put("keyspace", "test");
//        options.put("table", "words");
//
//        SparkSession  ss= SparkSession.builder()
//                .master("spark://iZ8vba8r09vdt7osvopu2nZ:7077")
//                .appName("java test")
//                .config("spark.cassandra.connection.host", "39.101.213.6")
//                .config("spark.cassandra.connection.port", "9042")
//                .config("spark.driver.memory", "1G")
//                .config("spark.executor.memory", "1G")
//                .config("spark.cores.max", "16")
//                .getOrCreate();
//        CassandraConnector connector = CassandraConnector.apply(ss.sparkContext().conf());
//        Session session = connector.openSession();
//        Dataset<Row> ds = ss.read().format("org.apache.spark.sql.cassandra").options(options).load();
//        ds.show();
//        ds.createOrReplaceTempView("temp");
//        ds.sqlContext().sql("select * from temp").show();
        //查询sparksession然后使用sql语句

//
//        SparkSession  ss= SparkSession.builder()
//                .master("spark://iZ8vba8r09vdt7osvopu2nZ:7077")
//                .appName("java test")
//                .config("spark.cassandra.connection.host", "39.101.213.6")
//                .config("spark.cassandra.connection.port", "9042")
//                .config("spark.driver.memory", "1G")
//                .config("spark.executor.memory", "1G")
//                .config("spark.cores.max", "16")
//                .getOrCreate();
//        Dataset<Row> dataset = ss.read()
//              .format("org.apache.spark.sql.cassandra")
//                .options(new HashMap<String, String>() {
//                    {
//                        put("keyspace", "test");
//                        put("table", "words");
//                    }
//                }).load();
//
//        dataset.show();
//        dataset.createOrReplaceTempView("usertable");
//        Dataset<Row> dataset1 = ss.sql("select * from usertable");
//        dataset1.show();
        //类似采用sparksession然后使用sql语句

//        SparkSession sparkSession = SparkSession.builder().master("spark://iZ8vba8r09vdt7osvopu2nZ:7077")
//                .appName("Java Spark SQL")
//                .getOrCreate();
//        Dataset<Row> dataset = sparkSession.read().json("URL");
//        dataset.createOrReplaceTempView("user");
//        Dataset<Row> users = sparkSession.sql("SELECT * FROM user");
//        users.show();


//        ss.stop();

    }
}
