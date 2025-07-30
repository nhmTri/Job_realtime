package com.nhmTri.jobrealtime;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.*;
import scala.Serializable;
import scala.Tuple4;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class CassandraToMySql implements Serializable {
    private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/job_realtime?serverTimezone=Asia/Ho_Chi_Minh";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "root";
    private static final String MYSQL_TABLE = "job_posts";

    private static final String CASSANDRA_KEYSPACE = "job_scrape_data";
    private static final String CASSANDRA_TABLE = "job_posts";
    private transient SparkSession spark;  // transient không serialize

    public void run(SparkSession spark) {
        this.spark = spark;
        Timestamp latestCDC = fetchLatestCDCFromMySQL(spark);
        Dataset<Row> cassandraDF = loadNewRecordsFromCassandra(spark, latestCDC);

        if (cassandraDF.isEmpty()) {
            return;
        }

        Dataset<Row> transformedDF = processDataFrame(spark, cassandraDF);
        writeToMySQL(transformedDF);
    }

    private Timestamp fetchLatestCDCFromMySQL(SparkSession spark) {
        try {
            Dataset<Row> mysqlDF = spark.read()
                    .format("jdbc")
                    .option("url", MYSQL_URL)
                    .option("dbtable", "(SELECT scraped_at FROM " + MYSQL_TABLE + " ORDER BY scraped_at DESC LIMIT 1) AS tmp")
                    .option("user", MYSQL_USER)
                    .option("password", MYSQL_PASSWORD)
                    .option("driver", "com.mysql.cj.jdbc.Driver")
                    .load();

            Row row = mysqlDF.first();
            if (row == null || row.isNullAt(0)) {
                return Timestamp.valueOf("2000-01-01 00:00:00");
            }

            // Lấy giá trị kiểu String, rồi ép về Timestamp
            String maxCdcStr = row.getString(0);
            System.out.println(maxCdcStr);
            return Timestamp.valueOf(maxCdcStr); // Định dạng phải là "yyyy-MM-dd HH:mm:ss"

        } catch (Exception e) {
            System.err.println("[CDC Fetch Error] " + e.getMessage());
            return Timestamp.valueOf("2000-01-01 00:00:00");
        }
    }


    private Dataset<Row> loadNewRecordsFromCassandra(SparkSession spark, Timestamp latestCDC) {
        return spark.read()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", CASSANDRA_KEYSPACE)
                .option("table", CASSANDRA_TABLE)
                .load()
                .filter(col("scraped_at").gt(lit(latestCDC)));
    }

    private Dataset<Row> processDataFrame(SparkSession spark, Dataset<Row> df) {
        registerUDFs(spark);
        df = df.withColumn("cleanCategory", callUDF("splitPhrases", col("category")))
                .withColumn("salary_struct", callUDF("parseSalaryInfo", col("salary")))
                .withColumn("minrangesalary", col("salary_struct.low"))
                .withColumn("maxrangesalary", col("salary_struct.high"))
                .withColumn("avgsalary", col("salary_struct.avg"))
                .withColumn("salaryunit", col("salary_struct.unit"))
                .drop("salary_struct");
        df = df.filter(not(col("salary").contains("Thương lượng")
                .or(col("salary").contains("Cạnh tranh"))));
        df = df.withColumn("scraped_tmp",date_format(df.col("scraped_at"), "yyyy-MM-dd HH:mm:ss"))
                .drop("scraped_at")
                .withColumnRenamed("scraped_tmp", "scraped_at");
        df = df.withColumn("skill", concat_ws(",", col("skill")))
                .drop("category")
                .drop("salary")
                .drop("salary_broker")
                .drop("minrangesalary")
                .drop("maxrangesalary")
                .withColumn("expired_at", callUDF("formatDate", col("expired_at")))
                .withColumn("posted_at", callUDF("formatDate", col("posted_at")))
                .withColumn("expired_at", to_date(col("expired_at"), "MM/dd/yyyy"))
                .withColumn("posted_at", to_date(col("posted_at"), "MM/dd/yyyy"))
                .withColumnRenamed("avgsalary", "salary")
                .withColumnRenamed("cleanCategory", "category");
        return df;
    }

    private void writeToMySQL(Dataset<Row> df) {
        df.write()
                .format("jdbc")
                .option("url", MYSQL_URL)
                .option("dbtable", MYSQL_TABLE)
                .option("user", MYSQL_USER)
                .option("password", MYSQL_PASSWORD)
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .mode(SaveMode.Append)
                .save();

        System.out.println("[INFO] Đã ghi " + df.count() + " dòng vào MySQL bảng " + MYSQL_TABLE);
    }

    private void registerUDFs(SparkSession spark) {
        spark.udf().register("splitPhrases", (UDF1<String, String>) text -> {
            if (text == null || text.isEmpty()) return null;
            List<String> tokens = Arrays.stream(text.toLowerCase().split("[>/,|\\-]"))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toList());
            return tokens.isEmpty() ? null : tokens.get(0);
        }, DataTypes.StringType);

        spark.udf().register("parseSalaryInfo", (UDF1<String, Row>) salaryText -> {
            Tuple4<Double, Double, Double, String> result = parseSalaryInfo(salaryText);
            return RowFactory.create(result._1(), result._2(), result._3(), result._4());
        }, DataTypes.createStructType(new StructField[]{
                new StructField("low", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("high", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("avg", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("unit", DataTypes.StringType, true, Metadata.empty())
        }));

        spark.udf().register("formatDate", (UDF1<Timestamp, String>) timestamp -> {
            try {
                if (timestamp == null) return null;
                SimpleDateFormat output = new SimpleDateFormat("MM/dd/yyyy");
                return output.format(timestamp);
            } catch (Exception e) {
                return null;
            }
        }, DataTypes.StringType);
    }

    private Tuple4<Double, Double, Double, String> parseSalaryInfo(String salaryText) {
        if (salaryText == null || salaryText.isEmpty()) return new Tuple4<>(null, null, null, null);
        salaryText = salaryText.toLowerCase().replaceAll("\\s+", " ").trim();

        if (salaryText.contains("thương lượng") || salaryText.contains("cạnh tranh"))
            return new Tuple4<>(null, null, null, null);

        double multiplier = 1_000_000;
        String unit = "VND";
        if (salaryText.contains("usd") || salaryText.contains("$")) {
            multiplier = 1.0;
            unit = "USD";
        }

        salaryText = salaryText.replaceAll("[₫,]|vnd|vnđ|usd|/tháng|\\$", "").replaceAll("(?i)(từ|tới|~|>)", "").trim();

        Pattern range = Pattern.compile("(\\d+(\\.\\d+)?)[^\\d]+(\\d+(\\.\\d+)?)");
        Matcher matcher = range.matcher(salaryText);
        if (matcher.find()) {
            double low = Double.parseDouble(matcher.group(1)) * multiplier;
            double high = Double.parseDouble(matcher.group(3)) * multiplier;
            return new Tuple4<>(low, high, (low + high) / 2.0, unit);
        }

        Matcher above = Pattern.compile("trên\\s*(\\d+(\\.\\d+)?)").matcher(salaryText);
        if (above.find()) {
            double value = Double.parseDouble(above.group(1)) * multiplier;
            return new Tuple4<>(value, null, value, unit);
        }

        Matcher below = Pattern.compile("dưới\\s*(\\d+(\\.\\d+)?)").matcher(salaryText);
        if (below.find()) {
            double value = Double.parseDouble(below.group(1)) * multiplier;
            return new Tuple4<>(null, value, value, unit);
        }

        Matcher single = Pattern.compile("(\\d+(\\.\\d+)?)").matcher(salaryText);
        if (single.find()) {
            double value = Double.parseDouble(single.group(1)) * multiplier;
            return new Tuple4<>(value, value, value, unit);
        }

        return new Tuple4<>(null, null, null, unit);
    }
}



