package com.nhmTri.jobrealtime;

import com.nhmTri.jobrealtime.dto.JobPostDTO;
import org.openqa.selenium.*;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VietnamworkScraper {
    private static final Logger logger = LoggerFactory.getLogger(com.nhmTri.jobrealtime.VietnamworkScraper.class);
    private static final String BASE_URL = "https://www.vietnamworks.com/viec-lam?page=%d";
    private static final String SOURCE = "VietnamWorks";
    private WebDriver driver;
    private static WebDriverWait wait;

    public VietnamworkScraper() {
        setupDriver();
    }
    private void setupDriver() {
        ChromeOptions options = new ChromeOptions();
        options.addArguments("--headless");
        options.addArguments("--no-sandbox");
        options.addArguments("--disable-gpu");
        options.addArguments("--disable-notifications");
        options.addArguments("--disable-extensions");
        options.addArguments("--remote-allow-origins=*");
        options.addArguments("--disable-dev-shm-usage");
        options.addArguments("--blink-settings=imagesEnabled=false"); // tắt ảnh
        options.addArguments("--disable-blink-features=AutomationControlled");
        options.addArguments("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36");

        driver = new ChromeDriver(options);
        wait = new WebDriverWait(driver, Duration.ofSeconds(5));
    }
    public List<JobPostDTO> scrapeJobs (int maxPages, int jobsPerPage) {
    List<JobPostDTO> jobs = new ArrayList<>();
    try {
        for (int page = 1; page <= maxPages; page++) {
            driver.get(String.format(BASE_URL, page));
            System.out.println(" Processing page  " + page);
            scrollToBottom();
            WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(10000));
            List<WebElement> jobCards = driver.findElements(By.cssSelector("div.new-job-card"));
            System.out.printf("Page %d: Found %d job cards%n", page, jobCards.size());
            for (int i = 0; i < Math.min(jobsPerPage, jobCards.size()); i++) {
                String company;
                try {
                    jobCards = driver.findElements(By.cssSelector("div.new-job-card"));
                    WebElement jobCard = jobCards.get(i);
                    try {
                        WebElement companyElement = jobCard.findElement(By.cssSelector("a[href*='/nha-tuyen-dung/']"));
                        company = companyElement.getText().trim();
                    } catch (NoSuchElementException e) {
                        company = null;
                    }
                    WebElement linkElement = jobCard.findElement(By.cssSelector("a.img_job_card"));
                    String detailUrl = linkElement.getAttribute("href");
                    ((JavascriptExecutor) driver).executeScript("window.open(arguments[0], '_blank');", detailUrl);
                    List<String> tabs = new ArrayList<>(driver.getWindowHandles());
                    driver.switchTo().window(tabs.get(tabs.size() - 1));  // đảm bảo là tab mới nhất
//                    String currentUrl = driver.getCurrentUrl();
//                    driver.navigate().to(detailUrl);
                    // Đợi trang load và lấy thông tin chi tiết
                    wait.until(ExpectedConditions.presenceOfElementLocated(By.cssSelector("h1")));
                        try {
                            String jobID = UUID.randomUUID().toString();
                            String title = safeGetText(By.cssSelector("h1[name ='title']"));
                            String location = getLocationFromSpanList();
                            String salary = safeGetText(By.cssSelector("span[name='label'][class*='cVbwLK']"));
                            String category = getFieldByLabelText("NGÀNH NGHỀ");
//                            String skill = getFieldByLabel("KỸ NĂNG");
                            String skill = extractSkills();
                            String postedate = getFieldByLabelText("NGÀY ĐĂNG");
                            String expire_text = safeGetText(By.cssSelector("span.sc-ab270149-0.ePOHWr"));
                            String expiredate = calculateExpiredDate(postedate,expire_text );
                            System.out.printf("Page %d - Job %d:\n", page, i + 1);
                            JobPostDTO job = JobPostDTO.builder()
                                    .jobId(jobID)
                                    .title(title)
                                    .company(company)
                                    .location(location)
                                    .salary(salary)
                                    .skill(skill) // Bạn có thể thêm nếu xác định được
                                    .category(category)
                                    .expiredAt(expiredate)
                                    .postedAt(postedate)
                                    .url(detailUrl)
                                    .source(SOURCE)
                                    .createdAt(LocalDateTime.now())
                                    .build();
                            jobs.add(job);
                            driver.close();
                            driver.switchTo().window(tabs.get(0));
                            wait.until(ExpectedConditions.presenceOfElementLocated(By.cssSelector("div.new-job-card")));
//                            driver.close(); // đóng tab chi tiết
//                            driver.switchTo().window(tabs.get(0)); // quay lại tab chính
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    } catch (Exception e) {
        throw new RuntimeException(e);
    } finally {
        driver.quit();
    }
        return jobs;
    }
    private String safeGetText(By by) {
        try {
            return driver.findElement(by).getText().trim();
        } catch (NoSuchElementException e) {
            return null;  // hoặc null nếu bạn muốn thể hiện rõ
        }
    }
    private String getFieldByLabelText(String labelText) {
        try {
            String fullXpath = "//label[normalize-space(text())='" + labelText + "']/following-sibling::p";
            WebElement element = driver.findElement(By.xpath(fullXpath));
            return element.getText().trim();
        } catch (StaleElementReferenceException e) {
            try{
                    String fullXpath = "//label[normalize-space(text())='" + labelText + "']/following-sibling::p";
                    WebElement element = driver.findElement(By.xpath(fullXpath));
                    return element.getText().trim();
                } catch (Exception ex) {
                    System.err.println(" Retry failed (still stale): " + ex.getMessage());
                    return null;
                }
            } catch (NoSuchElementException e)
        {return null;} // hoặc trả chuỗi rỗng nếu bạn muốn ""
        }
    private String getFieldByLabel(String labelText) {
        try {
            // Cách 1: Tìm p là following-sibling của label
            String xpath1 = "//label[normalize-space(text())='" + labelText + "']/following-sibling::p";
            WebElement element = driver.findElement(By.xpath(xpath1));
            return element.getText().trim();
        } catch (StaleElementReferenceException e1) {
            try {
                String xpath1 = "//label[normalize-space(text())='" + labelText + "']/following-sibling::p";
                WebElement element = driver.findElement(By.xpath(xpath1));
                return element.getText().trim();
            } catch (Exception ex1) {
                System.err.println("Retry failed (still stale): " + ex1.getMessage());
                return null;
            }
        } catch (NoSuchElementException e2) {
            // Fallback: Cách 2 - label và p cùng trong một div cha
            try {
                String xpath2 = "//label[normalize-space(text())='" + labelText + "']/parent::div/p";
                WebElement element = driver.findElement(By.xpath(xpath2));
                return element.getText().trim();
            } catch (Exception ex2) {
                System.err.println("Fallback to parent::div/p failed: " + ex2.getMessage());
                return null;
            }
        }
    }
    private void scrollToBottom() {
        long lastHeight = (long) ((JavascriptExecutor) driver).executeScript("return document.body.scrollHeight");
        while (true) {
            ((JavascriptExecutor) driver).executeScript("window.scrollTo(0, document.body.scrollHeight);");
            try { Thread.sleep(300); } catch (InterruptedException ignored) {}
            long newHeight = (long) ((JavascriptExecutor) driver).executeScript("return document.body.scrollHeight");
            if (lastHeight == newHeight) break;
            lastHeight = newHeight;
        }
    }
    private String getLocationFromSpanList() {
        try {
            List<WebElement> spans = driver.findElements(By.cssSelector("span.sc-ab270149-0.ePOHWr"));
            if (spans.size() >= 3) {
                return spans.get(2).getText().trim();  // Lấy cái thứ 3
            }
        } catch (Exception e) {
            System.err.println("❌ Lỗi khi lấy location: " + e.getMessage());
        }
        return null;
    }
    private static String calculateExpiredDate(String postedDateStr, String expiredInText) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
            LocalDate postedDate = LocalDate.parse(postedDateStr, formatter);

            if (expiredInText == null || expiredInText.isEmpty()) return null;

            expiredInText = expiredInText.toLowerCase().trim();

            int amount = extractNumber(expiredInText);

            if (expiredInText.contains("ngày")) {
                return postedDate.plusDays(amount).format(formatter);
            } else if (expiredInText.contains("tháng")) {
                return postedDate.plusMonths(amount).format(formatter);
            } else if (expiredInText.contains("tuần")) {
                return postedDate.plusWeeks(amount).format(formatter);
            } else {
                // fallback mặc định: +30 ngày nếu không rõ đơn vị
                return postedDate.plusDays(30).format(formatter);
            }
        } catch (Exception e) {
            System.err.println(" Lỗi khi xử lý expired date: " + e.getMessage());
            return null;
        }
    }
    // Helper để trích số đầu tiên từ chuỗi
    private static int extractNumber(String input) {
        try {
            Pattern p = Pattern.compile("(\\d+)");
            Matcher m = p.matcher(input);
            if (m.find()) {
                return Integer.parseInt(m.group(1));
            }
        } catch (Exception ignored) {}
        return 0;
    }
    private String extractSkills() {
        try {
            List<WebElement> buttons = driver.findElements(By.xpath("//button[normalize-space()='Xem thêm']"));
            if (!buttons.isEmpty()) {
                try {
                    buttons.get(0).click();
                    Thread.sleep(100);
                } catch (Exception ignored) {}
            }

            List<WebElement> labelElements = driver.findElements(By.xpath("//label[normalize-space()='KỸ NĂNG']"));
            if (!labelElements.isEmpty()) {
                WebElement label = labelElements.get(0);
                try {
                    WebElement container = label.findElement(By.xpath("./ancestor::div[1]"));
                    WebElement p = container.findElement(By.xpath(".//p"));
                    return p.getText().trim();
                } catch (Exception ignored) {}
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }
    public void shutdown() {
        if (driver != null) {
            driver.quit();
        }
    }
}
