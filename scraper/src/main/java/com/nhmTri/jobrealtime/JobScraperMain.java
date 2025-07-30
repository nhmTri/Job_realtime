package com.nhmTri.jobrealtime;

import com.nhmTri.jobrealtime.dto.JobPostDTO;

import java.util.List;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class JobScraperMain {
    public static void main(String[] args) {
            VietnamworkScraper scraper = new VietnamworkScraper();
            List<JobPostDTO> jobs = scraper.scrapeJobs(2,7); // scrape 2 page
            for (JobPostDTO job : jobs) {
                RedisJobLogger redisJobLogger = new RedisJobLogger();
                if(!redisJobLogger.isJobExists(job)){
                    redisJobLogger.logJob(job);
                    System.out.println("✅ Ghi log mới vào Redis: " + job.getTitle());
                } else {
                    System.out.println("⏭️ Bỏ qua job đã tồn tại: " + job.getTitle());
                }
                redisJobLogger.close();
            }
            scraper.shutdown();


        }
    }