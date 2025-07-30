# SAMPLE VIDEO
https://drive.google.com/file/d/1aO0HcwiDrikLpJdjmnNt2w2aO_Rrj1hF/view?usp=sharing

# DATA PIPELINE
<img width="1002" height="577" alt="Image" src="https://github.com/user-attachments/assets/2c9e01d6-6d5e-4719-8607-01d2f787571f" />


# 🛠 Real-Time Job Scraping Pipeline

This system collects and processes **real-time job postings** from sources like **CareerViet** and **VietnamWorks**, using ** Redis, Kafka, Spark, Cassandra, MySQL**, and includes a **CDC (Change Data Capture)** mechanism.

---

## System Architecture
## 🔧 Technologies Used

| Component        | Technology                           |
|------------------|--------------------------------------|
| Scraper          |  Selenium, Playwright                |
| Cache DB         | Redis                                |
| Messaging Queue  | Apache Kafka                         |
| Stream Processing| Apache Spark (local mode)            |
| DataLake         | Apache Cassandra                     |
| OLAP Storage     | MySQL                                |
| CDC Logic        | scrape_at in Cassandra > scraper_at in MySQL  |
| Dashboard        | Grafana (Pie Chart by Job Category)  |
| Utils             | Docker, Gemini API, LLM model in GoogleColab  |

---

## Project Structure (Multi-Module Maven)
job_realtime/

├── jobrealtime/ # Scraper: Scrape & sends data to Redis
├── kafkastreaming/ # Streaming: Kafka Produce && Consumer → SingleTon Spark writes to Cassandra
├── common-lib/ # package utils about DTO, API GEMINI
├── casstomysql/ # Use Change Data Capture
├── app-runner/ # AppRunner.java - Main pipeline runner
├── README.md


## Pipeline Overview

### 1. **Job Scraping**

- Crawls job listings from VietnamWorks and CareerViet (title, company, skills, etc.)
- Sends structured data to Kafka topics (`vietnamworks-topic`, `careerviet-topic`)

### 2. **Streaming Processing && Batch Processing**

- Spark Streaming reads from Kafka
- Parses JSON → Writes to `job_posts` table in Cassandra
- Performs OLAP transformation for analysis

### 3. **Change Data Capture (CDC)**

- Uses a `while (true)` loop to compare `scraped_at` in Cassandra with `cdc` timestamp in MySQL
- Writes only new/changed data to MySQL for reporting/dashboard

### 4. **OLAP Output Performance**
- Grafana
---

## Cassandra Table Schema
--- sql
CREATE TABLE IF NOT EXISTS job_posts (
    job_id VARCHAR(100),
    title VARCHAR(500),
    company VARCHAR(300),
    location VARCHAR(300),
    salary VARCHAR(200),
    salaryunit VARCHAR(20),
    category VARCHAR(200),
    expired_at DATE,
    posted_at DATE,
    skill Text,
    url TEXT,
    source VARCHAR(100),
    scraped_at TIMESTAMP,
    timeforcdc TIMESTAMP DEFAULT CURRENT_TIMESTAMP

