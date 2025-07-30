import com.microsoft.playwright.options.LoadState;
import com.nhmTri.jobrealtime.dto.JobPostDTO;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.openqa.selenium.*;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class VietnamWork {
    private static WebDriver driver;
    private static WebDriverWait wait;
    private static final String BASE_URL = "https://www.vietnamworks.com/viec-lam?page=%d";

    @BeforeAll
    public static void setUp() {
        ChromeOptions options = new ChromeOptions();
        options.addArguments("--headless=new");
        options.addArguments("--no-sandbox");
        options.addArguments("--disable-gpu");
        options.addArguments("--disable-notifications");
        options.addArguments("--disable-extensions");
        options.addArguments("--remote-allow-origins=*");
        options.addArguments("--disable-dev-shm-usage");
        options.addArguments("--blink-settings=imagesEnabled=false");
        options.addArguments("--disable-blink-features=AutomationControlled");
        options.addArguments("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36");

        driver = new ChromeDriver(options);
        wait = new WebDriverWait(driver, Duration.ofSeconds(5));
    }

    @AfterAll
    public static void tearDown() {
        if (driver != null) {
            driver.quit();
        }
        System.gc();
    }

    private void scrollToBottom() {
        long lastHeight = (long) ((JavascriptExecutor) driver).executeScript("return document.body.scrollHeight");
        while (true) {
            ((JavascriptExecutor) driver).executeScript("window.scrollTo(0, document.body.scrollHeight);");
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long newHeight = (long) ((JavascriptExecutor) driver).executeScript("return document.body.scrollHeight");
            if (lastHeight == newHeight) break;
            lastHeight = newHeight;
        }
    }

    private String getFieldByLabelText(String labelText) {
        try {
            String fullXpath = "//label[normalize-space(text())='" + labelText + "']/following-sibling::p";
            WebElement element = driver.findElement(By.xpath(fullXpath));
            return element.getText().trim();
        } catch (StaleElementReferenceException | NoSuchElementException e) {
            return null;
        }
    }

    private String safeGetText(By by) {
        try {
            return driver.findElement(by).getText().trim();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    private String getFieldByLabel(String labelText) {
        String[] xpaths = {
                "//label[normalize-space(text())='" + labelText + "']/following-sibling::p",
                "//label[normalize-space(text())='" + labelText + "']/parent::div/p"
        };

        for (String xpath : xpaths) {
            try {
                WebElement element = driver.findElement(By.xpath(xpath));
                if (element != null && element.isDisplayed()) {
                    return element.getText().trim();
                }
            } catch (NoSuchElementException | StaleElementReferenceException ignored) {
            }
        }
        return null;
    }

    private String getLocationFromSpanList() {
        try {
            List<WebElement> spans = driver.findElements(By.cssSelector("span.sc-ab270149-0.ePOHWr"));
            if (spans.size() >= 3) {
                return spans.get(2).getText().trim();
            }
        } catch (Exception e) {
            return null;
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
                return postedDate.plusDays(30).format(formatter);
            }
        } catch (Exception e) {
            return null;
        }
    }

    private static int extractNumber(String input) {
        try {
            Pattern p = Pattern.compile("(\\d+)");
            Matcher m = p.matcher(input);
            if (m.find()) {
                return Integer.parseInt(m.group(1));
            }
        } catch (Exception ignored) {
        }
        return 0;
    }
    public String extractSkillIfExists(WebDriver driver) {
        WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(5));
        try {
            // Bước 1: Nếu có nút "Xem thêm" thì click
            List<WebElement> buttons = driver.findElements(By.xpath("//button[normalize-space()='Xem thêm']"));
            if (!buttons.isEmpty()) {
                buttons.get(0).click();
                // Chờ một chút để nội dung mở rộng được render ra
                Thread.sleep(500); // hoặc dùng wait cho chuyên nghiệp hơn
            }

            // Bước 2: Tìm nhãn "KỸ NĂNG"
            List<WebElement> skillLabels = driver.findElements(By.xpath("//label[normalize-space()='KỸ NĂNG']"));
            if (!skillLabels.isEmpty()) {
                WebElement label = skillLabels.get(0);
                WebElement container = label.findElement(By.xpath("./ancestor::div[1]"));
                WebElement skillParagraph = container.findElement(By.xpath(".//p"));
                return skillParagraph.getText();
            } else {
                return null; // Không có kỹ năng
            }
        } catch (Exception e) {
            // Có thể log ra nếu cần
            return null;
        }
    }
    public static String extractSkills(WebDriver driver) {
        try {
            WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(5));

            // Bước 1: Click nút "Xem thêm" nếu có
            List<WebElement> buttons = driver.findElements(By.xpath("//button[normalize-space()='Xem thêm']"));
            if (!buttons.isEmpty()) {
                try {
                    buttons.get(0).click();
                    // Chờ nội dung render (có thể thay Thread.sleep bằng wait if needed)
                    Thread.sleep(100); // Hoặc wait.until(...);
                } catch (Exception ignore) {
                    // Không sao nếu không click được
                }
            }

            // Bước 2: Tìm nhãn "KỸ NĂNG"
            List<WebElement> labelElements = driver.findElements(By.xpath("//label[normalize-space()='KỸ NĂNG']"));
            if (!labelElements.isEmpty()) {
                WebElement label = labelElements.get(0);

                // Cách 1: label → parent::div → p
                try {
                    WebElement p1 = label.findElement(By.xpath("parent::div/p"));
                    String skills = p1.getText().trim();
                    if (!skills.isEmpty()) return skills;
                } catch (Exception ignore) {}

                // Cách 2: label → ancestor::div[1] → .//p
                try {
                    WebElement container = label.findElement(By.xpath("./ancestor::div[1]"));
                    WebElement p2 = container.findElement(By.xpath(".//p"));
                    String skills = p2.getText().trim();
                    if (!skills.isEmpty()) return skills;
                } catch (Exception ignore) {}


            }
            return null; // Không tìm thấy kỹ năng
        } catch (Exception e) {
            return null;
        }
    }
    @Test
    public void testScrapeJobsFromOnePage() {
        List<JobPostDTO> jobs = new ArrayList<>();
        driver.get(String.format(BASE_URL, 1));
        scrollToBottom();
        List<WebElement> jobCards = driver.findElements(By.cssSelector("div.new-job-card"));
        Assertions.assertFalse(jobCards.isEmpty(), "Không tìm thấy job nào");

        for (int i = 0; i < Math.min(15, jobCards.size()); i++) {  // Chỉ lấy 2 job đầu tiên để test
            try {
                jobCards = driver.findElements(By.cssSelector("div.new-job-card"));
                WebElement jobCard = jobCards.get(i);
                String employerName = jobCard.findElement(By.cssSelector("div.sc-bwGlVi.vdnbR a[href*='/nha-tuyen-dung/']")).getText().trim();
                WebElement linkElement = jobCard.findElement(By.cssSelector("a.img_job_card"));
                String detailUrl = linkElement.getAttribute("href");
                ((JavascriptExecutor) driver).executeScript("window.open(arguments[0], '_blank');", detailUrl);
                List<String> tabs = new ArrayList<>(driver.getWindowHandles());
                driver.switchTo().window(tabs.get(tabs.size() - 1));

                wait.until(ExpectedConditions.presenceOfElementLocated(By.cssSelector("h1")));

                String title = safeGetText(By.cssSelector("h1[name ='title']"));
//                String company = safeGetText(By.cssSelector("a[href*='/nha-tuyen-dung/']"));
//              String company = safeGetText(By.cssSelector("a.sc-ab270149-0.egZKeY.sc-f0821106-0.gWSkfE"));
                String location = getLocationFromSpanList();
                String salary = safeGetText(By.cssSelector("span[name='label'][class*='cVbwLK']"));
                String category = getFieldByLabelText("NGÀNH NGHỀ");
                String skills = extractSkills(driver);
                String postedate = getFieldByLabelText("NGÀY ĐĂNG");
                String expire_text = safeGetText(By.cssSelector("span.sc-ab270149-0.ePOHWr"));
                String expiredate = calculateExpiredDate(postedate, expire_text);
                Assertions.assertNotNull(title);
                Assertions.assertNotNull(postedate);
                System.out.println("=========================================================");
                System.out.printf("Title: %s\n", title);
                System.out.printf("Company: %s\n", employerName);
                System.out.printf("Posted: %s - Expire: %s\n", postedate, expiredate);
                if (skills != null)  System.out.println("Kỹ năng: " + skills);
                else System.out.println("Không có kỹ năng");
                System.out.println("Link " + detailUrl);
                driver.close();
                driver.switchTo().window(tabs.get(0));
            } catch (Exception e) {
                e.printStackTrace();
                Assertions.fail(" Lỗi khi xử lý job card: " + e.getMessage());
            }
        }
    }

    @Test
    public void testScrapeJobsFromMultiplePages() {
        int totalPages = 3; // Số trang bạn muốn test
        List<JobPostDTO> jobs = new ArrayList<>();

        for (int page = 1; page <= totalPages; page++) {
            System.out.println("Đang xử lý trang: " + page);
            driver.get(String.format(BASE_URL, page));
            scrollToBottom();
            wait.until(ExpectedConditions.presenceOfAllElementsLocatedBy(By.cssSelector("div.new-job-card")));
            List<WebElement> jobCards = driver.findElements(By.cssSelector("div.new-job-card"));
            if (jobCards.isEmpty()) {
                System.out.println("Không tìm thấy job nào ở trang " + page);
                continue;
            }
            for (int i = 0; i < Math.min(10, jobCards.size()); i++) {  // Test mỗi trang 5 job
                try {
                    jobCards = driver.findElements(By.cssSelector("div.new-job-card"));
                    WebElement jobCard = jobCards.get(i);
                    String employerName = jobCard.findElement(By.cssSelector("div.sc-bwGlVi.vdnbR a[href*='/nha-tuyen-dung/']")).getText().trim();
                    WebElement linkElement = jobCard.findElement(By.cssSelector("a.img_job_card"));
                    String detailUrl = linkElement.getAttribute("href");

                    ((JavascriptExecutor) driver).executeScript("window.open(arguments[0], '_blank');", detailUrl);
                    List<String> tabs = new ArrayList<>(driver.getWindowHandles());
                    driver.switchTo().window(tabs.get(tabs.size() - 1));

                    wait.until(ExpectedConditions.presenceOfElementLocated(By.cssSelector("h1")));

                    String title = safeGetText(By.cssSelector("h1[name ='title']"));
                    String location = getLocationFromSpanList();
                    String salary = safeGetText(By.cssSelector("span[name='label'][class*='cVbwLK']"));
                    String category = getFieldByLabelText("NGÀNH NGHỀ");
                    String skills = extractSkills(driver);
                    String postedate = getFieldByLabelText("NGÀY ĐĂNG");
                    String expire_text = safeGetText(By.cssSelector("span.sc-ab270149-0.ePOHWr"));
                    String expiredate = calculateExpiredDate(postedate, expire_text);

                    Assertions.assertNotNull(title);
                    Assertions.assertNotNull(postedate);
                    System.out.println("=========================================================");
                    System.out.printf("Page %d - Job %d:\n", page, i + 1);
                    System.out.printf("Title: %s\n", title);
                    System.out.printf("Company: %s\n", employerName);
                    System.out.println("Category: " + category);
                    System.out.printf("Posted: %s - Expire: %s\n", postedate, expiredate);
                    if (skills != null) System.out.println("Kỹ năng: " + skills);
                    else System.out.println("Không có kỹ năng");
                    System.out.println("Link: " + detailUrl);

                    driver.close();
                    driver.switchTo().window(tabs.get(0));
                } catch (Exception e) {
                    e.printStackTrace();
                    Assertions.fail("Lỗi khi xử lý job card ở trang " + page + ": " + e.getMessage());
                }
            }
        }
    }
}


