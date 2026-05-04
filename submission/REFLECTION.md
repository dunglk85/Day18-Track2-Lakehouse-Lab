# Reflection: Day 18 Spark Lakehouse Lab

## 1. Những gì đã hoàn thành tốt (What went well)
- Thiết lập thành công môi trường Lakehouse toàn diện (Spark, Delta Lake, MinIO) trên nền tảng Docker.
- Vượt qua các rào cản kỹ thuật phức tạp về cấu hình môi trường trên Windows (JVM, PYTHONPATH, Permissions).
- Thực hiện thành công việc tạo 1.000.000 dòng dữ liệu mẫu, kiểm chứng được khả năng ghi dữ liệu quy mô lớn vào Delta Lake.
- Hoàn thiện bản thiết kế kiến trúc Bonus (Topic A - LLM Observability) với các đánh đổi (trade-offs) thực tế về FinOps và hiệu năng.

## 2. Những thách thức (Challenges)
- **Cấu hình môi trường**: Gặp nhiều lỗi về quyền ghi thư mục `.ivy2` và `.cache` của người dùng `jovyan` trong container, đòi hỏi phải can thiệp lệnh `chown` từ root.
- **Quản lý tài nguyên**: Việc tạo dữ liệu lớn gây lỗi `OutOfMemoryError: Java heap space`, cần điều chỉnh tăng `driver-memory` lên 2GB để ổn định hệ thống.
- **Dependency Management**: Việc tải `pyspark` và các JARs từ Maven gặp khó khăn do tốc độ mạng, cần phải tối ưu hóa bằng cách tận dụng bộ thư viện có sẵn trong image và cấu hình `PYTHONPATH` thủ công.

## 3. Bài học kinh nghiệm (Lessons Learned)
- **Kiến trúc Medallion**: Hiểu rõ luồng dữ liệu Bronze -> Silver -> Gold và vai trò của từng tầng trong việc làm sạch và tổng hợp dữ liệu.
- **Tính năng Delta Lake**: Nắm vững cách sử dụng `Z-Order` để tối ưu hóa truy vấn theo `tenant_id` và ứng dụng `Time Travel` để phục hồi dữ liệu khi có lỗi logic (redaction failure).
- **FinOps**: Học cách tính toán chi phí lưu trữ (S3 Tiering) và tính toán (Spot Instances) để vận hành hệ thống 5TB/ngày với ngân sách hạn hẹp.

## 4. Hướng phát triển tiếp theo (Future Improvements)
- Tìm hiểu sâu hơn về **Data Quality Frameworks** (như Soda hoặc Great Expectations) để tự động hóa việc phát hiện lỗi PII ở tầng Silver.
- Thử nghiệm các Catalog trung lập như **Apache Polaris** hoặc **Project Nessie** để quản lý giao dịch và versioning ở cấp độ toàn bộ Lakehouse.
