CREATE KEYSPACE IF NOT EXISTS job_scrape_data
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}; -- local


USE job_scrape_data;



CREATE TABLE IF NOT EXISTS job_posts (
    jobId TEXT,
    title TEXT,
    company TEXT,
    location TEXT,
    salary TEXT,
    skill LIST<TEXT>,
    category TEXT,
    expired_at TIMESTAMP,
    posted_at TIMESTAMP,
    url TEXT,
    source TEXT,                  
    scraped_at TIMESTAMP,          


    PRIMARY KEY ((source), scraped_at, job_id)
)
WITH CLUSTERING ORDER BY (scraped_at DESC, job_id ASC);