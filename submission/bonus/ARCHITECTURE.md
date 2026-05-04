# Thiết kế Kiến trúc: LLM Observability Lakehouse Quy mô Lớn (1B Req/Day)

## 1. Tuyên bố bài toán (Problem Statement)

Nhóm chúng tôi có nhiệm vụ xây dựng một hệ thống Lakehouse giám sát LLM để xử lý **1 tỷ yêu cầu mỗi ngày**. Với trung bình mỗi yêu cầu/phản hồi là 5 KB, tổng lượng dữ liệu thô nạp vào đạt mức **5 TB mỗi ngày**.

**Các thách thức chính bao gồm:**
1. **Tốc độ vận hành**: Cung cấp dashboard chi phí và độ trễ theo từng khách hàng (tenant) được làm mới mỗi **5 phút**, đòi hỏi một pipeline nạp và tổng hợp dữ liệu gần như thời gian thực.
2. **Khả năng tra cứu và Lưu trữ**: Cho phép tìm kiếm toàn văn để tra cứu sự cố trên **35 TB dữ liệu "nóng"** (cửa sổ 7 ngày), đồng thời duy trì lịch sử dữ liệu tổng hợp trong 1 năm.
3. **Ràng buộc ngân sách (FinOps)**: Đạt được các mục tiêu trên với ngân sách cứng **$5.000/tháng** cho cả lưu trữ và tính toán.
4. **Bảo mật**: Bắt buộc phải lọc bỏ dữ liệu định danh cá nhân (PII) trước khi bất kỳ ai có thể truy cập vào log.

Điều này đòi hỏi một kiến trúc Medallion tinh vi, tận dụng phân tầng dữ liệu quyết liệt, lọc PII tại tầng Bronze và sử dụng các kỹ thuật index nâng cao (Z-order) để tối ưu hóa truy vấn theo từng khách hàng mà không làm tăng chi phí lưu trữ.

---

## 2. Sơ đồ kiến trúc (Architecture Diagram)

*(Xem hình vẽ ASCII bên dưới)*

```text
[ LLM API Logs ] --> [ Kafka / Kinesis ] --> [ Spark Streaming ]
                                                   |
                                                   v
        +---------------------------------------------------------------------------+
        |                              S3 DATA LAKE                                 |
        |  +-------------------+      +-----------------------+      +-----------+  |
        |  |   Bronze Layer    |      |     Silver Layer      |      | Gold Layer|  |
        |  | (Raw Parquet/JSON)|      |   (Redacted Delta)    |      | (Aggs)    |  |
        |  |   Retention: 24h  | -->  |   Retention: 7 days   | -->  | Retention:|  |
        |  |                   |      |   Z-Order: TenantID   |      | 1 Year    |  |
        |  +-------------------+      +-----------------------+      +-----------+  |
        +---------------------------------------------------------------------------+
                                                   |
                                        +----------+----------+
                                        |                     |
                                [ Incident Review ]    [ Cost/Latency Dash ]
                                (Ad-hoc Queries)       (5-min Refresh)
```

---

## 3. Các quyết định then chốt (Key Decisions)

| Quyết định | Giải pháp lựa chọn | Phương án bị loại bỏ | Lý do đánh đổi (Trade-off) |
| :--- | :--- | :--- | :--- |
| **Định dạng bảng** | **Delta Lake** | Apache Iceberg | Tôi chọn Delta vì tính năng **Z-Order** cực mạnh giúp lọc dữ liệu theo `tenant_id` nhanh hơn Iceberg. Đồng thời hỗ trợ Deletion Vectors để xóa PII nhanh mà không cần ghi lại toàn bộ file. |
| **Lưu trữ tầng Bronze** | **S3 Standard (24h)** | S3 Standard (Vĩnh viễn) | Loại bỏ việc lưu trữ vĩnh viễn ở tầng Bronze vì tốn phí (~$115/ngày). Chỉ giữ dữ liệu thô trong 24 giờ để tái xử lý nếu gặp lỗi, sau đó xóa bỏ để tiết kiệm ngân sách. |
| **Quản lý vòng đời** | **S3 Intelligent-Tiering** | S3 Standard | Tôi chọn Intelligent-Tiering cho tầng Silver (7 ngày). Dù tốn phí quản lý nhỏ, nhưng nó tự động đẩy dữ liệu ít dùng sang tầng rẻ hơn, giúp duy trì mức chi phí dưới $5.000/tháng. |
| **Tối ưu hóa truy vấn** | **Z-Order theo TenantID** | Partition theo TenantID | Nếu partition theo `tenant_id`, sẽ gặp lỗi "Small Files Problem" vì có hàng triệu khách hàng. Z-Order cho phép gộp dữ liệu vào các file lớn nhưng vẫn giữ được tốc độ lọc nhanh. |
| **Xử lý bảo mật** | **Redaction tại Bronze** | Mã hóa tại Gold | Tôi chọn che dữ liệu ngay khi vừa nạp vào. Nếu đợi đến tầng Gold mới mã hóa thì dữ liệu nhạy cảm vẫn tồn tại ở dạng thô quá lâu trong hệ thống, gây rủi ro rò rỉ. |

---

## 4. Các tình huống thất bại (Failure Modes)

1. **Lỗi logic lọc PII (Redaction Bug)**: Một bản cập nhật code lỗi khiến dữ liệu nhạy cảm lọt vào tầng Silver.
   - **Phát hiện**: Job Data Quality (DQ) quét định kỳ tìm kiếm email/số điện thoại trong tầng Silver.
   - **Xử lý**: Sử dụng **Time Travel** của Delta Lake để quay lại phiên bản sạch trước đó. Sửa code và thực hiện **Re-process** dữ liệu từ tầng Bronze (nhờ chính sách giữ Bronze 24h).

2. **Dữ liệu nạp vào tăng đột biến (Traffic Spike)**: Số lượng log tăng gấp 5 lần khiến Spark Streaming bị nghẽn (lag).
   - **Phát hiện**: Cảnh báo Kafka consumer lag vượt ngưỡng 10 phút.
   - **Xử lý**: Kích hoạt Auto-scaling cụm Spark. Nếu vẫn không đáp ứng, áp dụng "Load Shedding" - tạm thời bỏ qua các log không ưu tiên để bảo vệ pipeline tính tiền.

3. **Vượt ngân sách FinOps**: Chi phí tính toán Spark vượt mức $160/ngày.
   - **Phát hiện**: AWS Cost Explorer gửi cảnh báo hàng ngày.
   - **Xử lý**: Giảm tần suất thực hiện `OPTIMIZE` hoặc tăng cửa sổ Dashboard từ 5 phút lên 15 phút để giảm tài nguyên tính toán cho đến khi tối ưu được pipeline.

---

## 5. Tính toán chi phí (Cost Back-of-envelope)

Giả định quy mô 150 TB dữ liệu nạp vào mỗi tháng (5 TB/ngày). Ngân sách: **$5.000/tháng**.

### Chi phí Lưu trữ (S3):
- **Tầng Silver (7 ngày nóng)**: 35 TB x $23/TB (S3 Standard) = **$805/tháng**.
- **Tầng Bronze (24h thô)**: 5 TB x $23/TB / 30 ngày x 30 = **$115/tháng**.
- **Tầng Gold (Dữ liệu tổng hợp 1 năm)**: Giả định 10 GB/tháng x 12 = 120 GB (Chi phí không đáng kể).
- **Tổng lưu trữ**: ~$920/tháng.

### Chi phí Tính toán (Compute - Spark):
- **Ngân sách còn lại**: $5.000 - $920 = **$4.080/tháng**.
- **Mục tiêu**: Chạy cụm Spark liên tục với chi phí ~$5.6/giờ.
- **Giải pháp**: Sử dụng **AWS EC2 Spot Instances** để giảm chi phí xuống 60-70%. Với $5.6/giờ, ta có thể chạy một cụm Spark gồm 1 Master và 4-6 Workers (m5.xlarge) đủ để xử lý nạp dữ liệu và tổng hợp mỗi 5 phút.

---

## 6. Lộ trình xây dựng MVP (One-week MVP)

Tôi sẽ xây dựng một lát cắt nhỏ nhưng hoàn chỉnh trong 1 tuần:
1. Thiết lập **Spark Streaming** đọc từ một Kafka topic giả lập.
2. Triển khai logic **PII Redaction** đơn giản (Regex cho Email/Phone) khi ghi vào tầng Silver.
3. Chạy lệnh **Z-Order** trên một mẫu dữ liệu (100 GB) và demo tốc độ truy vấn theo `tenant_id`.
4. Tính toán **Aggregates** (latency) trên cửa sổ 5 phút và hiển thị trên một dashboard đơn giản.

