package com.nhmTri.kafkabitnami;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.nhmTri.jobrealtime.dto.JobPostDTO;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.util.*;

public class KafkaJobProducer {
    private static final String TOPIC = "job-stream";
    private final ObjectMapper objectMapper;
    private final KafkaProducer<String,String> producer;

    public KafkaJobProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // Đảm bảo message được ghi an toàn
        props.put(ProducerConfig.RETRIES_CONFIG, 3);  // Retry nếu Kafka tạm thời lỗi
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5); // batch nhỏ trong 5ms trước khi gửi
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        this.producer = new KafkaProducer<>(props);
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    public void produce(List<JobPostDTO> jobs) {
        if(jobs == null || jobs.isEmpty()) {
            System.out.println("No jobs to send");
            return;
        }
        for (JobPostDTO job : jobs) {
            try{
                //cate dùng làm key -> giúp giữ thứ tự & phân partition
                String key = job.getCategory();

                // convert Java object sang JSON
                String value = objectMapper.writeValueAsString(job);

                // tạo record với topic, key, value
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);
                producer.send(record,(metadata, exception) -> {
                    if(exception == null) {
                        System.out.printf(" Sent to partition %d: offset=%d key=%s%n", metadata.partition(), metadata.offset(), key);
                    } else {
                        System.err.println("Send failed: " + exception.getMessage());
                    }
                });
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }  producer.flush();
    }
    public void close() {
        producer.close();
    }

}
