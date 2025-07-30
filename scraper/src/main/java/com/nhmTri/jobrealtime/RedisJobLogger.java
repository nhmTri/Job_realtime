package com.nhmTri.jobrealtime;
import com.nhmTri.jobrealtime.dto.JobPostDTO;
import redis.clients.jedis.Jedis;

import java.text.Normalizer;
import java.time.LocalDate;
import java.util.Set;


public class RedisJobLogger {
    private static Jedis jedis;
    public RedisJobLogger() {
        jedis = new Jedis("localhost", 6379);
    }

    private String normalizeJob(JobPostDTO job) {
        String title = job.getTitle().toLowerCase().trim().replaceAll("\\s+", " ");
        title = Normalizer.normalize(title, Normalizer.Form.NFC);
        String url = job.getUrl().toLowerCase().trim();
        return "job:" + title + "::" + url;
    }

    public void logJob(JobPostDTO job) {
        String key = "joblog:" + LocalDate.now();
        String value = normalizeJob(job);
        jedis.sadd(key, value);
        if (jedis.ttl(key) == -1) {
            jedis.expire(key, 60);
        }
    }

    public boolean isJobExists(JobPostDTO job) {
        String key = "joblog:" + LocalDate.now();
        String value = normalizeJob(job);
        return jedis.sismember(key, value);
    }

    public Set<String> getAllJobsToDay() {
        String key = "joblog:" + LocalDate.now();
        return jedis.smembers(key);
    }

    public void close(){
        jedis.close();
    }
}

