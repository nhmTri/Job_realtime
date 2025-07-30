CREATE DATABASE IF NOT EXISTS job_realtime;
USE job_realtime;

-- Tạo bảng chính chứa thông tin việc làm
CREATE TABLE IF NOT EXISTS job_posts (
    job_id VARCHAR(100),
    title VARCHAR(500),
    company VARCHAR(300),
    location VARCHAR(300),
    salary VARCHAR(200),
    salaryunit VARCHAR(10),
    category VARCHAR(200),
    expired_at DATE,
    posted_at DATE,
    skill Text,
    url TEXT,
    source VARCHAR(100),
    scraped_at VARCHAR(30)
);

