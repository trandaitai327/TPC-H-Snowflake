# Snowflake 
## Hệ Thống Phân Tích Dữ Liệu Kinh Doanh Quốc Tế (TPC-H) 

**Người thực hiện:** Trần Đại Tài  
**Email:** trandaitai327@gmail.com

---

## Giới thiệu

Đây là đồ án cuối khóa của mình về xây dựng hệ thống phân tích dữ liệu sử dụng bộ dữ liệu TPC-H trên nền tảng Snowflake. Dự án này bao gồm các phần chính:

- Medallion Architecture (Bronze → Silver → Gold)
- Automation với Tasks và Streams
- Security với masking policies
- Data quality checks và performance optimization
- Snowpark Python và UDFs

## Về bộ dữ liệu TPC-H

TPC-H là bộ dữ liệu benchmark chuẩn cho các hệ thống phân tích, gồm 8 bảng:
1. REGION (5 dòng)
2. NATION (25 dòng)
3. CUSTOMER (150,000 dòng)
4. SUPPLIER (10,000 dòng)
5. PART (200,000 dòng)
6. PARTSUPP (800,000 dòng)
7. ORDERS (1,500,000 dòng)
8. LINEITEM (6,000,000 dòng)

## Cấu trúc project

```
tpch_analytics_project/
├── README.md
├── data/                          # Các file CSV
│   ├── tpch_sf1_region.csv
│   ├── tpch_sf1_nation.csv
│   ├── tpch_sf1_customer.csv
│   ├── tpch_sf1_supplier.csv
│   ├── tpch_sf1_part.csv
│   ├── tpch_sf1_partsupp.csv
│   ├── tpch_sf1_orders.csv
│   └── tpch_sf1_line_item.csv
├── src/
│   ├── 01_database_stage_roles.sql
│   ├── 02_medallion_data_pipeline_automation.sql
│   ├── 03_data_quality_check.sql
│   ├── 04_masking_policies_secure_data_sharing.sql
│   ├── 05_snowpark.py
│   └── 05_udfs.sql
└── DO_AN_CUOI_KHOA_FINAL.md
```

## Hướng dẫn setup

### Yêu cầu

Trước khi bắt đầu, bạn cần:
1. Tài khoản Snowflake (có quyền ACCOUNTADMIN hoặc SYSADMIN)
2. Warehouse đã được tạo (mình dùng `COMPUTE_WH`)
3. Các file CSV trong thư mục `data/`
4. Python 3.8+ (cho phần Snowpark, nếu có làm)

### Cài đặt Python packages (nếu cần)

```bash
pip install snowflake-snowpark-python pandas
```

### Bước 1: Upload files lên Snowflake

Mình upload bằng 2 cách, bạn chọn cách nào cũng được:

**Cách 1: Dùng SnowSQL**

```bash
# Kết nối
snowsql -a <account_identifier> -u <username>

# Upload (sau khi chạy file 01 để tạo stage)
PUT file://D:/Snowflake/TPC-H Project/data/tpch_sf1_region.csv @TPCH_DATA_STAGE;
PUT file://D:/Snowflake/TPC-H Project/data/tpch_sf1_nation.csv @TPCH_DATA_STAGE;
PUT file://D:/Snowflake/TPC-H Project/data/tpch_sf1_customer.csv @TPCH_DATA_STAGE;
PUT file://D:/Snowflake/TPC-H Project/data/tpch_sf1_supplier.csv @TPCH_DATA_STAGE;
PUT file://D:/Snowflake/TPC-H Project/data/tpch_sf1_part.csv @TPCH_DATA_STAGE;
PUT file://D:/Snowflake/TPC-H Project/data/tpch_sf1_partsupp.csv @TPCH_DATA_STAGE;
PUT file://D:/Snowflake/TPC-H Project/data/tpch_sf1_orders.csv @TPCH_DATA_STAGE;
PUT file://D:/Snowflake/TPC-H Project/data/tpch_sf1_line_item.csv @TPCH_DATA_STAGE;
```

**Cách 2: Dùng Snowsight UI**

1. Login vào Snowsight
2. Vào database `TPCH_ANALYTICS_DB` → schema `STAGING`
3. Tab "Files" → "Stages" → chọn `TPCH_DATA_STAGE`
4. Click "Upload Files" và chọn các file CSV

### Bước 2: Chạy các file SQL

Chạy theo thứ tự từ 01 đến 05:

#### Phần 1: Setup Database và Roles

File: `src/01_database_stage_roles.sql`

File này sẽ tạo:
- Database `TPCH_ANALYTICS_DB` và các schemas
- 4 roles: TPCH_ADMIN, TPCH_DEVELOPER, TPCH_ANALYST, TPCH_VIEWER
- Stage `TPCH_DATA_STAGE`
- 8 bảng raw (Bronze layer)
- Load data từ CSV vào tables

**Lưu ý:**
- Nếu warehouse khác `COMPUTE_WH`, nhớ sửa lại trong file
- Upload files lên stage trước khi chạy phần COPY INTO
- Sau khi load xong, check số dòng mỗi bảng xem có đúng không

#### Phần 2: Medallion Architecture

File: `src/02_medallion_data_pipeline_automation.sql`

File này tạo:
- Silver layer: ORDERS_SILVER, CUSTOMER_SILVER, LINEITEM_SILVER (đã được clean và enrich)
- Gold layer: CUSTOMER_LTV, DAILY_SALES_SUMMARY (business metrics)
- Streams cho CDC
- Stored Procedures cho transformation
- Tasks để tự động hóa

**Lưu ý:**
- Sửa warehouse trong Tasks nếu cần
- Sau khi tạo xong, chạy thủ công các stored procedures lần đầu để load data vào Silver và Gold
- Tasks sẽ tự động chạy theo schedule (mỗi giờ)

#### Phần 3: Data Quality và Exploration

File: `src/03_data_quality_check.sql`

Gồm các queries để:
- Data profiling (phân tích theo quốc gia, top products, etc.)
- Kiểm tra data quality (NULL, duplicates, referential integrity)
- Performance với EXPLAIN
- Phân tích distribution

**Lưu ý:**
- File này có nhiều queries, có thể chạy từng phần
- Dùng EXPLAIN để xem execution plan
- Xem Query Profile trong Snowsight để phân tích performance

#### Phần 4: Security

File: `src/04_masking_policies_secure_data_sharing.sql`

Bao gồm:
- Bảng CUSTOMER_SENSITIVE với thông tin nhạy cảm
- Masking policies cho EMAIL, PHONE, SSN, ACCTBAL, ADDRESS
- Secure views cho sharing
- Setup secure data sharing (cần ACCOUNTADMIN)

**Để test masking policies:**

```sql
USE ROLE TPCH_ADMIN;
SELECT C_CUSTKEY, EMAIL, C_PHONE, C_ACCTBAL 
FROM TPCH_ANALYTICS_DB.ANALYTICS.CUSTOMER_SENSITIVE LIMIT 5;

USE ROLE TPCH_ANALYST;
SELECT C_CUSTKEY, EMAIL, C_PHONE, C_ACCTBAL 
FROM TPCH_ANALYTICS_DB.ANALYTICS.CUSTOMER_SENSITIVE LIMIT 5;

USE ROLE TPCH_VIEWER;
SELECT C_CUSTKEY, EMAIL, C_PHONE, C_ACCTBAL 
FROM TPCH_ANALYTICS_DB.ANALYTICS.CUSTOMER_SENSITIVE LIMIT 5;
```

#### Phần 5: Snowpark và UDFs

**SQL UDFs:** `src/05_udfs.sql`

Tạo các UDFs:
- FN_CUSTOMER_TIER: Phân loại khách hàng
- FN_VALIDATE_PHONE, FN_VALIDATE_EMAIL: Validate
- FN_FORMAT_CURRENCY: Format tiền
- FN_EXTRACT_SENTIMENT: Phân tích sentiment (Python UDF)
- Và một số UDFs khác

**Snowpark Python:** `src/05_snowpark.py`

Chạy các phân tích:
- Customer RFM Segmentation
- Sales Trend Analysis
- Product Performance
- Customer Retention

**Cách chạy:**
1. Sửa connection parameters trong hàm `create_session()`
2. Hoặc tạo file `config.json` (có file example trong src/)
3. Chạy: `python src/05_snowpark.py`

## Kiến trúc Data Pipeline

```
Data Files (CSV)
    ↓
Stage (TPCH_DATA_STAGE)
    ↓
BRONZE (STAGING Schema) - Raw data
    ↓ Streams (CDC)
    ↓
SILVER (SILVER Schema) - Cleaned & Enriched
    ↓ Tasks
    ↓
GOLD (GOLD Schema) - Business Metrics
    ↓
Reports & Dashboards
```

## Kiểm tra kết quả

### Check số dòng

```sql
-- Bronze
SELECT 'ORDERS' AS TABLE_NAME, COUNT(*) FROM TPCH_ANALYTICS_DB.STAGING.ORDERS
UNION ALL
SELECT 'CUSTOMER', COUNT(*) FROM TPCH_ANALYTICS_DB.STAGING.CUSTOMER
UNION ALL
SELECT 'LINEITEM', COUNT(*) FROM TPCH_ANALYTICS_DB.STAGING.LINEITEM;

-- Silver
SELECT COUNT(*) AS COUNT FROM TPCH_ANALYTICS_DB.SILVER.ORDERS_SILVER;
SELECT COUNT(*) AS COUNT FROM TPCH_ANALYTICS_DB.SILVER.CUSTOMER_SILVER;

-- Gold
SELECT COUNT(*) AS COUNT FROM TPCH_ANALYTICS_DB.GOLD.CUSTOMER_LTV;
SELECT COUNT(*) AS COUNT FROM TPCH_ANALYTICS_DB.GOLD.DAILY_SALES_SUMMARY;
```

### Check Tasks

```sql
SHOW TASKS IN SCHEMA TPCH_ANALYTICS_DB.ANALYTICS;

-- Xem history
SELECT NAME, STATE, SCHEDULE, NEXT_SCHEDULED_TIME
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP())
))
WHERE DATABASE_NAME = 'TPCH_ANALYTICS_DB'
ORDER BY SCHEDULED_TIME DESC;
```

## Screenshots cần chụp

- Database và schemas
- Roles và permissions
- Stages với files
- Số dòng mỗi bảng
- Task execution history
- Masking policies với các roles
- Query execution plans
- Kết quả Snowpark Python

## Một số lỗi thường gặp

### Load data bị lỗi
- Check files đã upload đúng chưa
- Check file format (delimiter, header)
- Check warehouse có đang chạy không

### Tasks không chạy
- Check đã RESUME tasks chưa
- Check warehouse name đúng chưa
- Check dependencies giữa tasks

### Python UDFs lỗi
- Check Python runtime version
- Check packages đã install
- Check handler function name

### Snowpark connection lỗi
- Check account, user, password
- Check network
- Check role có đủ quyền

## Tài liệu tham khảo

- [Snowflake Docs](https://docs.snowflake.com/)
- [TPC-H Sample Data](https://docs.snowflake.com/en/user-guide/sample-data-tpch)
- [Snowpark Guide](https://docs.snowflake.com/en/developer-guide/snowpark/python/index.html)
- [UDF Best Practices](https://docs.snowflake.com/en/developer-guide/udf/udf-overview.html)
- [Tasks Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-task.html)
- [Streams Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-stream.html)

---

**Liên hệ:** trandaitai327@gmail.com

**Deadline:** 15/12/2025
