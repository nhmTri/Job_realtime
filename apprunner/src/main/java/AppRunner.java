//import com.nhmTri.jobrealtime.CareervietScraper;
//import com.nhmTri.jobrealtime.CassandraToMySql;
//import com.nhmTri.jobrealtime.RedisJobLogger;
//import com.nhmTri.jobrealtime.VietnamworkScraper;
//import com.nhmTri.jobrealtime.dto.JobPostDTO;
//import com.nhmTri.kafkastreaming.KafkaJobProducer;
//import com.nhmTri.kafkastreaming.SparkSessionSingleTon;
//import com.nhmTri.kafkastreaming.SparkToCassandra;
//import org.apache.spark.sql.SparkSession;
//
//import java.util.List;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.Future;
//
//public class AppRunner {
//    public static void main(String[] args) {
//        System.out.println("--------------------------------App Start ------------------------------");
//        System.out.println("\u001B[34m[Bước 1]\u001B[0m Scrape Data");
//
//        ExecutorService executor = Executors.newFixedThreadPool(3); // 2 scraper + 1 Spark
//
//        try {
//            // Khởi tạo tài nguyên
//            RedisJobLogger redisJobLogger = new RedisJobLogger();
//            KafkaJobProducer kafkaProducer = new KafkaJobProducer();
//            SparkToCassandra sparkJob = new SparkToCassandra();
//            CassandraToMySql cassandraToMysql = new CassandraToMySql();
//            SparkSession sparkSession = SparkSessionSingleTon.getSparkSession();
//
//            // Shutdown hook
//            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//                try {
//                    kafkaProducer.close();
//                    redisJobLogger.close();
//                    sparkJob.close();
//                    System.out.println("----------------Cleaned up all resources.----------------");
//                } catch (Exception ignored) {
//                }
//            }));
//
//            // VietnamWorks Scraper
//            Future<?> vnFuture = executor.submit(() -> {
//                try {
//                    System.out.println("\u001B[34m[Bước 1.1]\u001B[0m Scraping VietnamWorks...");
//                    List<JobPostDTO> jobs = new VietnamworkScraper().scrapeJobs(2, 20);
//                    for (JobPostDTO job : jobs) {
//                        if (!redisJobLogger.isJobExists(job)) {
//                            redisJobLogger.logJob(job);
//                            kafkaProducer.produce(List.of(job));
//                            System.out.println("\u001B[32m[New VNWorks]\u001B[0m " + job.getTitle());
//                        } else {
//                            System.out.println("\u001B[33m[Exists VNWorks]\u001B[0m " + job.getTitle());
//                        }
//                    }
//                    System.out.println("\u001B[34m[Bước 1.1]\u001B[0m Complete");
//                } catch (Exception e) {
//                    return;
//                }
//            });
//
//            // CareerViet Scraper
//            Future<?> cvFuture = executor.submit(() -> {
//                try {
//                    System.out.println("\u001B[34m[Bước 1.2]\u001B[0m Scraping CareerViet...");
//                    List<JobPostDTO> jobs = new CareervietScraper().scrapeJobs("Data", 5, 5);
//                    for (JobPostDTO job : jobs) {
//                        if (!redisJobLogger.isJobExists(job)) {
//                            redisJobLogger.logJob(job);
//                            kafkaProducer.produce(List.of(job));
//                            System.out.println("\u001B[32m[New CareerViet]\u001B[0m " + job.getTitle());
//                        } else {
//                            System.out.println("\u001B[33m[Exists CareerViet]\u001B[0m " + job.getTitle());
//                        }
//                    }
//                    System.out.println("\u001B[34m[Bước 1.2]\u001B[0m Complete");
//                } catch (Exception e) {
//                    return;
//                }
//            });
//
//            // Đợi 2 scraper hoàn thành
//            vnFuture.get();
//            cvFuture.get();
//
//            System.out.println("\u001B[34m[Bước 2]\u001B[0m Spark Starting");
//
//            // Spark xử lý pipeline
//            Future<?> sparkFuture = executor.submit(() -> {
//                try {
//                    sparkJob.start(); // Thực thi Spark xử lý từ Kafka → Cassandra
//                    System.out.println("\u001B[34m[Bước 2]\u001B[0m Spark Complete");
//
//                    System.out.println("\u001B[34m[Bước 3]\u001B[0m CDC Cassandra → MySQL");
//                    cassandraToMysql.run(sparkSession); // CDC → MySQL
//                    System.out.println("\u001B[34m[Bước 3]\u001B[0m CDC Complete");
//                } catch (Exception e) {
//                    return;
//                }
//            });
//
//            // Đợi spark job hoàn tất
//            sparkFuture.get();
//
//        } catch (Exception e) {
//        } finally {
//            executor.shutdown();
//        }
//    }
//}

import com.nhmTri.jobrealtime.CareervietScraper;
import com.nhmTri.jobrealtime.CassandraToMySql;
import com.nhmTri.jobrealtime.RedisJobLogger;
import com.nhmTri.jobrealtime.VietnamworkScraper;
import com.nhmTri.jobrealtime.dto.JobPostDTO;
import com.nhmTri.kafkastreaming.KafkaJobProducer;
import com.nhmTri.kafkastreaming.SparkSessionSingleTon;
import com.nhmTri.kafkastreaming.SparkToCassandra;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class AppRunner {
    public static void main(String[] args) {
        System.out.println("--------------------------------App Start ------------------------------");
        System.out.println("\u001B[34m[Bước 1]\u001B[0m Scrape Data");

        ExecutorService executor = Executors.newFixedThreadPool(3); // 2 scraper + 1 Spark

        try {
            // Khởi tạo tài nguyên
            RedisJobLogger redisJobLogger = new RedisJobLogger();
            KafkaJobProducer kafkaProducer = new KafkaJobProducer();
            SparkToCassandra sparkJob = new SparkToCassandra();
            CassandraToMySql cassandraToMysql = new CassandraToMySql();

            // Khởi tạo SparkSession
            SparkSession sparkSession = SparkSessionSingleTon.getSparkSession();

            // Shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    kafkaProducer.close();
                    redisJobLogger.close();
                    sparkJob.close();
                    System.out.println("----------------Cleaned up all resources.----------------");
                } catch (Exception ignored) {
                }
            }));

            // VietnamWorks Scraper
            Future<?> vnFuture = executor.submit(() -> {
                try {
                    System.out.println("\u001B[34m[Bước 1.1]\u001B[0m Scraping VietnamWorks...");
                    List<JobPostDTO> jobs = new VietnamworkScraper().scrapeJobs(1, 30);
                    for (JobPostDTO job : jobs) {
                        if (!redisJobLogger.isJobExists(job)) {
                            redisJobLogger.logJob(job);
                            kafkaProducer.produce(List.of(job));
                            System.out.println("\u001B[32m[New VNWorks]\u001B[0m " + job.getTitle());
                        } else {
                            System.out.println("\u001B[33m[Exists VNWorks]\u001B[0m " + job.getTitle());
                        }
                    }
                    System.out.println("\u001B[34m[Bước 1.1]\u001B[0m Complete");
                } catch (Exception e) {
                    // Không in lỗi
                }
            });

            // CareerViet Scraper
            Future<?> cvFuture = executor.submit(() -> {
                try {
                    System.out.println("\u001B[34m[Bước 1.2]\u001B[0m Scraping CareerViet...");
                    List<JobPostDTO> jobs = new CareervietScraper().scrapeJobs("Data",5 , 2);
                    for (JobPostDTO job : jobs) {
                        if (!redisJobLogger.isJobExists(job)) {
                            redisJobLogger.logJob(job);
                            kafkaProducer.produce(List.of(job));
                            System.out.println("\u001B[32m[New CareerViet]\u001B[0m " + job.getTitle());
                        } else {
                            System.out.println("\u001B[33m[Exists CareerViet]\u001B[0m " + job.getTitle());
                        }
                    }
                    System.out.println("\u001B[34m[Bước 1.2]\u001B[0m Complete");
                } catch (Exception e) {
                    // Không in lỗi
                }
            });

            // Đợi 2 scraper hoàn thành
            vnFuture.get();
            cvFuture.get();

            System.out.println("\u001B[34m[Bước 2]\u001B[0m Spark Starting");

            // Spark xử lý pipeline
            Future<?> sparkFuture = executor.submit(() -> {
                try {
                    sparkJob.start(); // Kafka → Spark → Cassandra
                    System.out.println("\u001B[34m[Bước 2]\u001B[0m Spark Complete");

                    System.out.println("\u001B[34m[Bước 3]\u001B[0m CDC Cassandra → MySQL");
                    cassandraToMysql.run(sparkSession);
                    System.out.println("\u001B[34m[Bước 3]\u001B[0m CDC Complete");
                } catch (Exception e) {
                    // Không in lỗi
                }
            });

            // Đợi Spark job hoàn tất
            sparkFuture.get();

        } catch (Exception e) {
            // Không in lỗi
        } finally {
            executor.shutdown();
        }
    }
}

