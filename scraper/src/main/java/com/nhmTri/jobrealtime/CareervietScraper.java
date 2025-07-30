package com.nhmTri.jobrealtime;

import com.microsoft.playwright.*;
import com.microsoft.playwright.options.LoadState;
import com.nhmTri.jobrealtime.dto.JobPostDTO;
import com.nhmTri.jobrealtime.nlp.GeminiClient;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class CareervietScraper {
    private static final String BASE_URL = "https://careerviet.vn/vi";
    private final Browser browser;
    private final Page page;
    private final String SOURCE = "Careerviet";

    public CareervietScraper() {
        Playwright playwright = Playwright.create();
        this.browser = playwright.chromium().launch(new BrowserType.LaunchOptions().setHeadless(false));
        this.page = browser.newContext().newPage();
    }

    public List<JobPostDTO> scrapeJobs(String keyword, int maxPages, int jobPerPage) {
        List<JobPostDTO> jobs = new ArrayList<>();
        try {
            page.navigate(BASE_URL);
            page.locator("input#keyword").fill(keyword);
            page.locator("button:has-text(\"TÌM VIỆC NGAY\")").click();
            page.waitForLoadState(LoadState.NETWORKIDLE);

            // Scroll để tải lazy-load
            for (int i = 0; i < 5; i++) {
                page.mouse().wheel(0, 5000);
                page.waitForTimeout(100);
            }

            for (int i = 0; i < maxPages; i++) {
                System.out.println("[Page] " + (i + 1));

                List<Locator> jobItems = page.locator("div.job-item").all();
                System.out.println("[All Job] " + jobItems.size());
                for (int j = 0 ; j < Math.min(jobPerPage,jobItems.size()); j++)
                    { JobPostDTO job = scrapeJobDetail(jobItems.get(j));
                        if (job != null) {
                            jobs.add(job);
                        }
                    }
                Locator nextBtn = page.locator("li.next-page a");
                if (nextBtn.isVisible()) {
                    nextBtn.click(new Locator.ClickOptions().setTimeout(5000));
                    page.waitForLoadState(LoadState.DOMCONTENTLOADED);
                } else {
                    System.out.println("[Error] No next job found] ");
                    break;
                }
            }
            page.close();
        } catch (Exception e) {
            System.err.println("[Error] " + e.getMessage());
        } finally {
            page.context().browser().close();
        }

        return jobs;
    }
    private JobPostDTO scrapeJobDetail(Locator jobItem) {
        String jobID = jobItem.getAttribute("id");
        String detailUrl = jobItem.locator("a.job_link").first().getAttribute("href");

        try (Page detailPage = page.context().newPage()) {
            detailPage.navigate(detailUrl);
            detailPage.waitForSelector("h1.title", new Page.WaitForSelectorOptions().setTimeout(3000));

            String title = detailPage.locator("h1.title").innerText().trim();
            String company = detailPage.locator("a.employer.job-company-name").innerText().trim();
            String location = detailPage.locator("div.map p a").innerText().trim();

            Locator box1 = detailPage.locator("div.detail-box.has-background").first();
            String postedAt = box1.locator("li:has-text('Ngày cập nhật') p").first().textContent().trim();
            String category = box1.locator("li:has-text('Ngành nghề') a").first().textContent().trim();

            Locator box2 = detailPage.locator("div.detail-box.has-background").nth(1);
            String salary = box2.locator("li:has-text('Lương') p").first().textContent().trim();
            String deadline = box2.locator("li:has-text('Hết hạn nộp') p").first().textContent().trim();

            // Trích xuất yêu cầu kỹ năng
            Locator requirementSection = detailPage.locator("div.detail-row.reset-bullet")
                    .filter(new Locator.FilterOptions().setHasText("Yêu Cầu Công Việc"));
            List<String> requirements = new ArrayList<>();
            requirements.addAll(requirementSection.locator("li").allInnerTexts());
            requirements.addAll(requirementSection.locator("p").allInnerTexts());
            String combinedText = String.join(", ", requirements);

            // Use API GEMINI
            GeminiClient geminiClient = new GeminiClient();
            String skill = geminiClient.extractKeywords(combinedText);
            return JobPostDTO.builder()
                    .jobId(jobID)
                    .title(title)
                    .salary(salary)
                    .company(company)
                    .location(location)
                    .category(category)
                    .postedAt(postedAt)
                    .expiredAt(deadline)
                    .skill(skill)
                    .url(detailUrl)
                    .source(SOURCE)
                    .createdAt(LocalDateTime.now())
                    .build();
        } catch (Exception e) {
            System.err.println("[Error] " + e.getMessage());
        return null;
        }
    }
}
