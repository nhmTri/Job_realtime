
import com.google.genai.types.Content;
import com.google.genai.types.GenerateContentResponse;
import com.google.genai.types.Part;
import com.microsoft.playwright.*;
import com.microsoft.playwright.options.LoadState;
import com.nhmTri.jobrealtime.dto.JobPostDTO;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.genai.Client;

import java.sql.SQLOutput;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class CareerViet {
    static Playwright playwright;
    static Browser browser;
    static BrowserContext context;
    static Page page;

    @BeforeAll
    static void setUp() {
        playwright = Playwright.create();
        browser = playwright.chromium().launch(new BrowserType.LaunchOptions().setHeadless(false));
        context = browser.newContext();
        page = context.newPage();
    }

    @Test
    void testKeywordInput() {
        List<JobPostDTO> jobs = new ArrayList<>();

        // Truy cập trang chính
        page.navigate("https://careerviet.vn/vi");
        // Nhập từ khoá
        page.locator("input#keyword").fill("data");

        // Click nút tìm việc
        page.locator("button:has-text(\"TÌM VIỆC NGAY\")").click();
        page.waitForLoadState(LoadState.NETWORKIDLE);

        // Scroll đến cuối để load thêm
        for (int i = 0; i < 5; i++) {
            page.mouse().wheel(0, 5000);
            page.waitForTimeout(500);
        }

        int maxPages = 5;
        for (int i = 0; i < maxPages; i++) {
            System.out.println("=====================================================" + " PAGE " + (i + 1) + " ===================================================");

            List<Locator> jobAll = page.locator("div.job-item").all();
            System.out.println(" Find  " + jobAll.size());
            for (Locator locator : jobAll) {
                String jobID = locator.getAttribute("id");
                Locator jobLink = locator.locator("a.job_link");
//                String jobID = jobLink.first().getAttribute("data-id").trim();
                String detailUrl = jobLink.first().getAttribute("href");
                Page detailPage = browser.newPage();
                try {
                    detailPage.navigate(detailUrl);
                    detailPage.waitForSelector("h1.title", new Page.WaitForSelectorOptions().setTimeout(5000));
                    Locator divTitle = detailPage.locator("div[style='display: flex; align-items: center; gap: 15px;']");
                    if (divTitle.count() > 0 && divTitle.locator("h1.title").count() > 0) {
                        String jobTitle = divTitle.locator("h1.title").innerText().trim();
                        Locator companyElement = detailPage.locator("a.employer.job-company-name");
                        String companyName = companyElement.innerText().trim();
                        Locator locationElement = detailPage.locator("div.map p a");
                        String location = locationElement.innerText().trim();
                        // Lấy ngày

                        Locator detailBox = detailPage.locator("div.detail-box.has-background").first();
                        String postedDate = detailBox.locator("li:has-text('Ngày cập nhật') p").first().textContent().trim();

                        Locator categoryAnchor = detailBox.locator("li:has-text('Ngành nghề') a").first();
                        String category = categoryAnchor.textContent().trim();
                        String categoryHref = categoryAnchor.getAttribute("href");
                        Locator detailBox2 = detailPage.locator("div.detail-box.has-background").nth(1);
                        String salary = detailBox2.locator("li:has-text('Lương') p").first().textContent().trim();
                        String deadline = detailBox2.locator("li:has-text('Hết hạn nộp') p").first().textContent().trim();

                        Locator requirementSection = detailPage.locator("div.detail-row.reset-bullet")
                                .filter(new Locator.FilterOptions().setHasText("Yêu Cầu Công Việc"));
                        Locator liItems = requirementSection.locator("li");
                        Locator pItems = requirementSection.locator("p");
                        List<String> IRequirements = liItems.allInnerTexts();
                        List<String> PRequirements = pItems.allInnerTexts();
                        List<String> requirements= new ArrayList<>();
                        requirements.addAll(IRequirements);
                        requirements.addAll(PRequirements);
                        String combined = String.join(", ", requirements);
                        System.out.println("[Pass] " + combined);
//                        String skill =geminiTest(combined);
//                        System.out.println("[Pass] " + jobTitle + " | " + detailUrl);
//                        System.out.println("[Pass] " + companyName);
//                        System.out.println("[Pass] " + location);
//                        System.out.println("[Pass] " + postedDate);
//                        System.out.println("[Pass] " + category);
//                        System.out.println("[Pass] " + salary);
//                        System.out.println("[Pass] " + deadline);
////                    System.out.println("[Pass] " + skill);
                        System.out.println("[Pass] " + jobID);
                        JobPostDTO job = JobPostDTO.builder()
                                .jobId(jobID)
                                .title(jobTitle)
                                .skill(combined)
                                .company(companyName)
                                .category(category)
                                .createdAt(LocalDateTime.now())
                                .expiredAt(deadline)
                                .postedAt(postedDate)
                                .url(detailUrl)
                                .source("Careeviet")
                                .location(location)
                                .build();

                        System.out.println(job);
                        jobs.add(job);

                    } else {
                        System.out.println("[Error]" + detailUrl);
                    }
                } catch (Exception e) {
                    System.out.println("[Step] " + e.getMessage());
                } finally {
                    detailPage.close();
                }

            }

            Locator nextButton = page.locator("li.next-page a");
            if (nextButton.isVisible()) {
                System.out.println("[Next Page] ");
                nextButton.click(new Locator.ClickOptions().setTimeout(5000));
                page.waitForLoadState(LoadState.DOMCONTENTLOADED);
            } else {
                System.out.println("============================================");
                break;
            }
        }
    }
    String testFirstProject(String longtext) {
        return geminiTest(longtext);
    }

    String geminiTest(String longtext) {
//        Client client = Client.builder().apiKey("").build();
        String promptText = "Extract only the most relevant keywords or phrases from the following Vietnamese job requirements. Return them as a comma-separated string only:\n" + longtext ;
        try (Client client = Client.builder().apiKey("").build()){
            // Xây dựng yêu cầu gửi đến mô hình Gemini.
            GenerateContentResponse response = client.models.generateContent("gemini-2.0-flash",promptText,null);
            return response.text();
        }
    }
    @Test
    void testPaginationScrape() throws InterruptedException {
        page.navigate("https://careerviet.vn/viec-lam/data-k-trang-1-vi.html");
        page.waitForLoadState(LoadState.NETWORKIDLE);

        int maxPages = 2;
        for (int i = 0; i < maxPages; i++) {
            scrapeCurrentPage(page);

            Locator nextButton = page.locator("li.next-page a");

            if (nextButton.isVisible()) {
                nextButton.click(new Locator.ClickOptions().setTimeout(5000));
                page.waitForLoadState(LoadState.DOMCONTENTLOADED);
                Thread.sleep(1500); // Wait for DOM to stabilize
            } else {
                System.out.println("✅ Đã đến trang cuối cùng.");
                break;
            }
        }
    }

    private void scrapeCurrentPage(Page page) {
        Locator jobs = page.locator("a.job_link");
        int count = jobs.count();
        for (int i = 0; i < count; i++) {
            Locator job = jobs.nth(i);
            String title = job.innerText().trim();
            String href = job.getAttribute("href");
            System.out.println("🔗 " + title + " → " + href);
        }
    }



    @AfterAll
    static void tearDown() {
        page.close();
        context.close();
        browser.close();
        playwright.close();
    }
}
