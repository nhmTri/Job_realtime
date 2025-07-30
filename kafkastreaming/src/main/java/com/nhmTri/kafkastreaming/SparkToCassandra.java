package com.nhmTri.kafkastreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class SparkToCassandra {
    private final SparkSession spark;
    private static final String KAFKA_BOOTSTRAP = "localhost:9094";
    private static final String KAFKA_TOPIC = "job-stream";

    public SparkToCassandra() {
        this.spark = SparkSessionSingleTon.getSparkSession();
        spark.sparkContext().setLogLevel("WARN");
    }

    public void start() throws Exception {
        StructType schema = new StructType()
                .add("jobId", DataTypes.StringType)
                .add("title", DataTypes.StringType)
                .add("company", DataTypes.StringType)
                .add("location", DataTypes.StringType)
                .add("salary", DataTypes.StringType)
                .add("skill", DataTypes.StringType)
                .add("category", DataTypes.StringType)
                .add("expiredAt", DataTypes.StringType)
                .add("postedAt", DataTypes.StringType)
                .add("url", DataTypes.StringType)
                .add("source", DataTypes.StringType)
                .add("createdAt", DataTypes.createArrayType(DataTypes.IntegerType));

        Dataset<Row> kafkaRawDF = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("subscribe", KAFKA_TOPIC)
                .option("startingOffsets", "earliest")
                // Không dùng endingOffsets trong readStream
                .load();

        Dataset<Row> parsed = kafkaRawDF
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col("value"), schema).as("data"))
                .select("data.*");

        Dataset<Row> transformed = parsed
                .withColumn("skill", when(col("skill").isNull(), array()).otherwise(split(col("skill"), ",\\s*")))
                .withColumn("scraped_at_string",
                        concat_ws(" ",
                                concat_ws("-",
                                        lpad(col("createdAt").getItem(0).cast("string"), 4, "0"),
                                        lpad(col("createdAt").getItem(1).cast("string"), 2, "0"),
                                        lpad(col("createdAt").getItem(2).cast("string"), 2, "0")
                                ),
                                concat_ws(":",
                                        lpad(col("createdAt").getItem(3).cast("string"), 2, "0"),
                                        lpad(col("createdAt").getItem(4).cast("string"), 2, "0"),
                                        lpad(col("createdAt").getItem(5).cast("string"), 2, "0")
                                )
                        )
                )
                .withColumn("scraped_at", to_timestamp(col("scraped_at_string"), "yyyy-MM-dd HH:mm:ss"))
                .withColumn("expired_at", to_timestamp(col("expiredAt"), "dd/MM/yyyy"))
                .withColumn("posted_at", to_timestamp(col("postedAt"), "dd/MM/yyyy"))
                .withColumnRenamed("jobId", "job_id")
                .drop("createdAt", "expiredAt", "postedAt", "scraped_at_string")
                .filter(col("job_id").isNotNull());

        StreamingQuery query = transformed.writeStream()
                .trigger(Trigger.ProcessingTime("5 seconds"))
                .foreachBatch((batchDF, batchId) -> {
                    batchDF.show(false);
                    System.out.println("Batch ID: " + batchId + " - Rows: " + batchDF.count());
                    batchDF.printSchema();

                    batchDF.write()
                            .format("org.apache.spark.sql.cassandra")
                            .option("keyspace", "job_scrape_data")
                            .option("table", "job_posts")
                            .mode("append")
                            .save();
                })
                .option("checkpointLocation", "file:///C:/tmp/spark-checkpoints")
                .outputMode("append")
                .start();

        // Chờ khoảng 3 lần trigger (3 x 5 giây = 15 giây) rồi dừng streaming
        int numTriggersToWait = 3;
        long triggerIntervalMs = 5000; // 5 giây
        for (int i = 0; i < numTriggersToWait; i++) {
            boolean terminatedEarly = query.awaitTermination(triggerIntervalMs);
            if (terminatedEarly) {
                System.out.println("Query terminated early!");
                break;
            }
        }
        query.stop();
        System.out.println("Streaming query stopped after " + numTriggersToWait + " triggers.");
    }

    public void close() throws Exception {
        spark.stop();
    }
}
