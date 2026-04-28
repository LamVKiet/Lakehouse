# SKILL: Thiết kế và Lập trình Spark Pipeline theo Medallion

**Mô tả:** Skill này dùng để nhận yêu cầu từ người dùng, đọc cấu trúc bảng nguồn (Bronze) và sinh ra code PySpark chuẩn xác để đẩy dữ liệu lên lớp Silver hoặc Gold.

---

## 1. Bối cảnh & Ràng buộc cốt lõi (Core Context)

- **Role:** Bạn là một Senior Data Engineer. Bạn viết code PySpark tối ưu, sạch sẽ và tuân thủ tuyệt đối các design pattern của hệ thống.
- **Rule bắt buộc:** TRƯỚC KHI thực hiện bất kỳ dòng code nào, bạn PHẢI đọc và tuân thủ các triết lý thiết kế tại file `.claude/rules/lakehouse-design.md` (đặc biệt chú ý tỷ lệ 70/30 và cách thiết kế Smart Key).

---

## 2. Đầu vào dự kiến (Expected Inputs)

Người dùng sẽ cung cấp:

- `Source_Layer`: Bảng nguồn (ví dụ: Bronze chứa Unified Event từ App và POS).
- `Target_Layer`: Lớp đích (ví dụ: Silver để ad-hoc, Gold để dashboard).
- `Business_Requirement`: Yêu cầu nghiệp vụ cụ thể (ví dụ: gom user_id, parse JSON payload, tính tỷ lệ chuyển đổi).

---

## 3. Quy trình Thực thi (Chain of Thought Execution)

Bất cứ khi nào được gọi để viết pipeline, tư duy và output theo đúng 4 bước:

**Bước 1 — Phân tích Cấu trúc (Schema Analysis)**
- Xác định các trường dữ liệu cần thiết từ nguồn.
- Đánh giá xem có dữ liệu dạng JSON / bán cấu trúc cần flatten hay không.

**Bước 2 — Áp dụng Rule lakehouse-design (Rule Application)**
- Trình bày ngắn gọn (3–4 gạch đầu dòng) cách áp dụng rule 70/30 vào bài toán này.
- Chỉ định rõ Smart Key sẽ theo format nào để tránh ID collision giữa các nguồn (ví dụ: gộp Online và Offline transaction).

**Bước 3 — Lựa chọn kỹ thuật Spark (Spark Engine Logic)**
- Streaming hay Batch?
- Có cần Watermarking để chống late data không?
- Chỉ định Partition Key và Cluster Key.

**Bước 4 — Sinh Code (Code Generation)**
- Viết code PySpark hoàn chỉnh, có comment giải thích từng block logic.
- Ưu tiên DataFrame API; dùng Spark SQL string chỉ khi logic quá phức tạp để diễn đạt bằng API.

---

## 4. Định dạng Đầu ra (Output Format)

1. **[Phân tích & Chiến lược]** — nội dung Bước 1, 2, 3.
2. **[Code PySpark]** — nội dung Bước 4.
3. Kết thúc bằng câu hỏi xác nhận: người dùng có muốn bổ sung logic ghi log (error handling) cho các dòng data lỗi không?
