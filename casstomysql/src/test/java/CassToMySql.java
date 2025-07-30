import com.datastax.oss.driver.shaded.json.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.*;
import org.json.JSONArray;
import org.junit.jupiter.api.Test;
import scala.Tuple4;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;


public class CassToMySql {


    public static void writeToMySQL(Dataset<Row> df, String tableName) {
        // JDBC connection info
        String jdbcUrl = "jdbc:mysql://localhost:3306/job_data?useSSL=false&serverTimezone=UTC";
        String username = "root"; // Thay bằng user của bạn
        String password = "your_password"; // Thay bằng password của bạn

        // Tạo properties kết nối
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", username);
        connectionProperties.put("password", password);
        connectionProperties.put("driver", "com.mysql.cj.jdbc.Driver");

        // Chọn các cột cần ghi (tùy dataset của bạn, chọn cột phù hợp)
        Dataset<Row> toWrite = df.select("title", "location", "cleanCategory",
                "minrangesalary", "maxrangesalary", "avgsalary", "salaryunit",
                "posted_at", "expired_at");

        // Ghi dữ liệu vào MySQL
        toWrite.write()
                .mode(SaveMode.Append) // Có thể dùng Overwrite / Ignore tùy use-case
                .jdbc(jdbcUrl, tableName, connectionProperties);

        System.out.println(" Đã ghi dữ liệu vào MySQL table: " + tableName);
    }


    public static SparkSession setup() {
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("KafkaToCassandra")
                .set("spark.cassandra.connection.host", "127.0.0.1")
                .set("spark.cassandra.connection.port", "9042")
                .set("spark.streaming.stopGracefullyOnShutdown", "true")
                .set("spark.sql.catalog.myCatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")
                .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
                .set("spark.sql.session.timeZone", "UTC")
                .set("spark.driver.extraJavaOptions",
                        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED " +
                        "--add-exports=java.base/sun.util.calendar=ALL-UNNAMED")
                .set("spark.executor.extraJavaOptions",
                        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED " +
                        "--add-exports=java.base/sun.util.calendar=ALL-UNNAMED");


        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        System.out.println("Driver options: " + spark.conf().get("spark.driver.extraJavaOptions"));
        System.out.println("Executor options: " + spark.conf().get("spark.executor.extraJavaOptions"));
        return spark;
    }


    @Test
    public void cassandraReadTest() {
        SparkSession spark = setup();

        // MAX(CDC) từ MYSQL
        Timestamp latestCDC = getLatestCDC(spark); // yyyy-MM-dd HH:mm:ss
        System.out.println(latestCDC);

        Dataset<Row> df = spark.read().format("org.apache.spark.sql.cassandra")
                .option("keyspace", "job_scrape_data")
                .option("table", "job_posts")
                .load();

        df = df.filter(df.col("scraped_at").gt(to_timestamp(lit(latestCDC))));

//        df.show();
//        df.show(2);
        // Column location
//       cleanLocation(df).show();
        registerSplitPhrases(spark);
        registerSparkSession(spark);
        df = df.withColumn("cleanCategory", functions.callUDF("splitPhrases", df.col("category")));
        df = df.withColumn("salary_broker", functions.callUDF("parseSalaryInfo", df.col("salary")))
                .selectExpr("*",
                        "salary_broker.low as minrangesalary",
                        "salary_broker.high as maxrangesalary",
                        "salary_broker.avg as avgsalary",
                        "salary_broker.unit as salaryunit")
                .drop("salary_struct");
        df = df.filter(functions.not(df.col("salary").contains("Cạnh tranh")
                .or(df.col("salary").contains("Thương lượng"))));
        df = formatPostedAndExpiredDate(spark, df);
//        df = df.filter("posted_at RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'");
        df.printSchema(); // in schema
        df.show();        // kiểm tra có dữ liệu không
        System.out.println("Row count: " + df.count());
        df = df.withColumn("skill", concat_ws(",", df.col("skill")))
                .drop("category")
                .drop("salary_broker")
                .drop("salary")
                .drop("minrangesalary")
                .drop("maxrangesalary")
                .withColumn("expired_at", to_date(df.col("expired_at"), "MM/dd/yyyy"))
                .withColumn("posted_at", to_date(df.col("posted_at"), "MM/dd/yyyy"))
                .withColumnRenamed("avgsalary", "salary")
                .withColumnRenamed("cleanCategory", "category");
        df.show(10);

        df.write()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/job_realtime")
                .option("dbtable", "job_posts")
                .option("user", "root")
                .option("password", "root")
                .mode(SaveMode.Append)
                .save();
    }
    private Dataset<Row> cleanLocation(Dataset<Row> df) {
        Dataset<Row> cleaned = df.filter(functions.not(df.col("location").contains("Hết hạn")));
        Dataset<Row> locationCount = cleaned.groupBy("location").count();
        return locationCount.orderBy(locationCount.col("count").desc());
    }

    private Dataset<Row> cleanCategory(Dataset<Row> df) {
        Dataset<Row> categoryCount = df.groupBy("cleanCategory").count();
        return categoryCount.orderBy(categoryCount.col("count").desc());
    }

    private static void registerSplitPhrases(SparkSession spark) {
        UDF1<String, String> splitPhrases = (String text) -> {
            if (text == null || text.isEmpty()) return null;
            List<String> tokens = Arrays.stream(text.trim().toLowerCase().split("[>/,|\\-]"))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toList());
            return tokens.isEmpty() ? null : tokens.get(0);
        };
        spark.udf().register("splitPhrases", splitPhrases, DataTypes.StringType);
    }

    public static Tuple4<Double, Double, Double, String> parseSalaryInfo(String salaryText) {
        if (salaryText == null || salaryText.isEmpty()) return new Tuple4<>(null, null, null, null);
        salaryText = salaryText.toLowerCase().replaceAll("\\s+", " ").trim();

        // Loại bỏ những trường hợp không xác định
        if (salaryText.contains("thương lượng") || salaryText.contains("cạnh tranh"))
            return new Tuple4<>(null, null, null, null);

        // Xử lý đơn vị và multiplier
        double multiplier = 1.0;
        String unit = "VND";
        if (salaryText.contains("tr") || salaryText.contains("triệu")) {
            multiplier = 1_000_000;
            unit = "VND";
        } else if (salaryText.contains("usd") || salaryText.contains("$")) {
            multiplier = 1.0;
            unit = "USD";
        }

        salaryText = salaryText.replaceAll(",", "")
                .replaceAll("₫", "")
                .replaceAll("vnd|vnđ|usd|/tháng", "")
                .replaceAll("\\$", "")
                .replaceAll("(?i)(từ|tới|~|>)", "")
                .trim();

        // Trường hợp khoảng: "25 Tr - 35 Tr"
        Pattern range = Pattern.compile("(\\d+(\\.\\d+)?)[^\\d]+(\\d+(\\.\\d+)?)");
        Matcher matcher = range.matcher(salaryText);
        if (matcher.find()) {
            double low = Double.parseDouble(matcher.group(1)) * multiplier;
            double high = Double.parseDouble(matcher.group(3)) * multiplier;
            double avg = (low + high) / 2.0;
            return new Tuple4<>(low, high, avg, unit);
        }

        // Trường hợp "trên 20", "dưới 10"
        Pattern above = Pattern.compile("trên\\s*(\\d+(\\.\\d+)?)");
        Pattern below = Pattern.compile("dưới\\s*(\\d+(\\.\\d+)?)");

        Matcher mAbove = above.matcher(salaryText);
        Matcher mBelow = below.matcher(salaryText);

        if (mAbove.find()) {
            double low = Double.parseDouble(mAbove.group(1)) * multiplier;
            return new Tuple4<>(low, null, low, unit);
        } else if (mBelow.find()) {
            double high = Double.parseDouble(mBelow.group(1)) * multiplier;
            return new Tuple4<>(null, high, high, unit);
        }

        // Trường hợp đơn lẻ: "20 triệu"
        Pattern single = Pattern.compile("(\\d+(\\.\\d+)?)");
        Matcher mSingle = single.matcher(salaryText);
        if (mSingle.find()) {
            double value = Double.parseDouble(mSingle.group(1)) * multiplier;
            return new Tuple4<>(value, value, value, unit);
        }

        return new Tuple4<>(null, null, null, unit);
    }


    public static void registerSparkSession(SparkSession spark) {
        spark.udf().register("parseSalaryInfo", (UDF1<String, Row>) salary -> {
            Tuple4<Double, Double, Double, String> result = parseSalaryInfo(salary);
            return RowFactory.create(result._1(), result._2(), result._3(), result._4());
        }, DataTypes.createStructType(new StructField[]{
                new StructField("low", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("high", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("avg", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("unit", DataTypes.StringType, true, Metadata.empty())
        }));

    }

    private static Dataset<Row> formatPostedAndExpiredDate(SparkSession spark, Dataset<Row> df) {
        // Định nghĩa UDF xử lý chuỗi ngày
        UDF1<String, String> formatDateUDF = new UDF1<String, String>() {
            private static final SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd");
            private static final SimpleDateFormat outputFormat = new SimpleDateFormat("MM/dd/yyyy");

            @Override
            public String call(String dateStr) throws Exception {
                if (dateStr == null || dateStr.isEmpty()) return null;
                try {
                    String dateOnly = dateStr.length() >= 10 ? dateStr.substring(0, 10) : dateStr;
                    java.util.Date parsed = inputFormat.parse(dateOnly);
                    return outputFormat.format(parsed);
                } catch (Exception e) {
                    return null;
                }
            }
        };
        spark.udf().register("formatDate", formatDateUDF, DataTypes.StringType);

        // Chuyển cột về chuỗi nếu không phải String
        df = df.withColumn("posted_at", df.col("posted_at").cast(DataTypes.StringType))
                .withColumn("expired_at", df.col("expired_at").cast(DataTypes.StringType));

        // Áp dụng UDF lên các cột
        return df.withColumn("posted_at", functions.callUDF("formatDate", df.col("posted_at")))
                .withColumn("expired_at", functions.callUDF("formatDate", df.col("expired_at")));
    }

    private Timestamp getLatestCDC(SparkSession spark) {
        try {
            Dataset<Row> mysqlDF = spark.read()
                    .format("jdbc")
                    .option("url", "jdbc:mysql://localhost:3306/job_realtime")
                    .option("dbtable", "(SELECT MAX(scraped_at) AS max_cdc FROM job_posts) t")
                    .option("user", "root")
                    .option("password", "root")
                    .option("driver", "com.mysql.cj.jdbc.Driver")
                    .load();

            Row row = mysqlDF.first();
            if (row == null || row.isNullAt(0)) {
                System.out.println(" Không có dữ liệu CDC → trả về mặc định");
                return Timestamp.valueOf("2000-01-01 00:00:00");
            }

            return row.getTimestamp(0);

        } catch (Exception e) {
            System.err.println(" Lỗi khi đọc MAX(cdc): " + e.getMessage());
            return Timestamp.valueOf("2000-01-01 00:00:00");
        }
    }
}










