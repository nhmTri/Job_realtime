package com.nhmTri.kafkastreaming;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;


public class SparkSessionSingleTon {
    private static volatile SparkSession sparkSession;
    private SparkSessionSingleTon() {

    }
    public static SparkSession getSparkSession() {
        if (sparkSession == null) {
            synchronized (SparkSessionSingleTon.class) {
                if (sparkSession == null) {
                    SparkConf conf = new SparkConf()
                            .setMaster("local[*]")
                            .setAppName("KafkaToCassandra")
                            .set("spark.cassandra.connection.host", "127.0.0.1")
                            .set("spark.cassandra.connection.port", "9042")
                            .set("spark.streaming.stopGracefullyOnShutdown", "true")
                            .set("spark.sql.catalog.myCatalog", "com.datastax.spark.connector.datasource.CassandraCatalog") // ✅ dòng quan trọng
                            .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
                            .set("spark.sql.session.timeZone", "UTC");

                    sparkSession = SparkSession
                            .builder()
                            .config(conf)
                            .getOrCreate();
                    Logger.getLogger("org.sparkproject.jetty").setLevel(Level.ERROR);
                    Logger.getLogger("org.sparkproject.jetty.server.handler.ContextHandler").setLevel(Level.OFF);

                    Logger.getLogger("org").setLevel(Level.ERROR);
                    Logger.getLogger("akka").setLevel(Level.ERROR);
                    Logger.getLogger("kafka").setLevel(Level.ERROR);
                    Logger.getLogger("com.datastax").setLevel(Level.ERROR);
                    Logger.getLogger("org.apache.kafka").setLevel(Level.ERROR); // ✅ Cụ thể hơn cho Kafka client
                    Logger.getLogger("org.apache.kafka.clients.producer.ProducerConfig").setLevel(Level.OFF); // ✅ Tắt cấu hình


                }
            }
        }
        return sparkSession;
    }
}

