package com.nhmTri.jobrealtime.nlp;
import com.google.genai.Client;
import com.google.genai.types.GenerateContentResponse;
public class GeminiClient {
    private final String apiKey;

    public GeminiClient() {
        this.apiKey = "";
    }
    public String extractKeywords(String vietnameseText) {
        String prompt = "Trích xuất 3 kỹ năng hoặc từ khóa quan trọng nhất từ yêu cầu công việc sau đây bằng tiếng Việt. "
                + "Chỉ trả về một chuỗi phân tách bằng dấu phẩy, không giải thích thêm:\n" + vietnameseText;

        try (Client client = Client.builder().apiKey(apiKey).build()) {
            GenerateContentResponse response = client.models
                    .generateContent("gemini-2.0-flash", prompt, null);
            return response.text().trim();
        } catch (Exception e) {
            System.err.println("[Error] " + e.getMessage());
            return "";
        }
    }
}


