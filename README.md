# 🛠 Real-Time Job Scraping Pipeline

This system collects and processes **real-time job postings** from sources like **CareerViet** and **VietnamWorks**, using **Kafka, Spark, Cassandra, MySQL**, and includes a **CDC (Change Data Capture)** mechanism.

---

## System Architecture
## 🔧 Technologies Used

| Component        | Technology                           |
|------------------|--------------------------------------|
| Scraper          | Java, Jsoup                          |
| Messaging Queue  | Apache Kafka                         |
| Stream Processing| Apache Spark (local mode)            |
| Raw Storage      | Apache Cassandra                     |
| OLAP Storage     | MySQL                                |
| CDC Logic        | Spark + Custom `while true` polling  |
| Dashboard        | Grafana (Pie Chart by Job Category)  |

---

## Project Structure (Multi-Module Maven)
job_realtime/
├── jobrealtime/ # Scraper: Crawls & sends data to Kafka
├── kafkastreaming/ # Spark: Consumes Kafka → writes to Cassandra
├── common-lib/ # package utils about DTO, API GEMINI
├── app-runner/ # AppRunner.java - Main pipeline runner
├── README.md

## 🚀 Pipeline Overview

### 1. **Job Scraping**

- Crawls job listings from VietnamWorks and CareerViet (title, company, skills, etc.)
- Sends structured data to Kafka topics (`vietnamworks-topic`, `careerviet-topic`)

### 2. **Streaming & Processing**

- Spark Streaming reads from Kafka
- Parses JSON → Writes to `job_posts` table in Cassandra
- Performs OLAP transformation for analysis

### 3. **Change Data Capture (CDC)**

- Uses a `while (true)` loop to compare `scraped_at` in Cassandra with `cdc` timestamp in MySQL
- Writes only new/changed data to MySQL for reporting/dashboard

### 4. **OLAP Output Performance**

> Processed **10M raw rows** → **1M OLAP rows** in **under 250 seconds** using PySpark.

---

## 💾 Cassandra Table Schema
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
); 
## Visualized as Pie Chart in Grafana
