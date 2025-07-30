package com.nhmTri.jobrealtime.dto;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.security.Timestamp;
import java.time.LocalDateTime;
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JobPostDTO {
    private String jobId;       // Tự tạo theo hệ thống
    private String title;       // Lấy tiêu đề Job
    private String company;     // Lấy tên công ty
    private String location;    // Lất địa điểm công ty
    private String salary;      // Có  thể null hoặc rang lương
    private String skill;       // Kỹ năng
    private String category;
    private String expiredAt;   // Dựa vào ngay het ham
    private String postedAt;
    private String url;
    private String source;
    private LocalDateTime createdAt; // thời gian thực hịện scraper theo hệ thống


}
