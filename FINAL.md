
## Hệ Thống Phân Tích Dữ Liệu Kinh Doanh Quốc Tế (TPC-H)

---

## 📋 Tổng Quan Dự Án

Xây dựng một hệ thống phân tích dữ liệu kinh doanh hoàn chỉnh sử dụng **bộ dữ liệu TPC-H**. Bạn sẽ thiết kế data warehouse, tạo các phép biến đổi, xây dựng analytics queries, và phát triển UDFs để phân tích dữ liệu bán hàng quốc tế.

TPC-H là một bộ tiêu chuẩn (benchmark) được phát triển bởi Transaction Processing Performance Council (TPC), được sử dụng để đánh giá hiệu suất của các hệ thống hỗ trợ quyết định (decision support systems) và cơ sở dữ liệu phân tích (data warehouses). 
Mục đích chính của TPC-H là mô phỏng các tác vụ phân tích và báo cáo phức tạp trong môi trường doanh nghiệp thực tế, chẳng hạn như phân tích thị trường hoặc dự báo bán hàng, để kiểm tra khả năng xử lý truy vấn và khả năng mở rộng của một hệ thống cơ sở dữ liệu.

![alt text](image.png)

**Bộ dữ liệu:** [TPC-H Sample Data](https://docs.snowflake.com/en/user-guide/sample-data-tpch)  
**Thời gian thực hiện:** 15-20 giờ

### 🏆 Yêu cầu chính của dự án

1. **Medallion Architecture** - Bronze → Silver → Gold layers cho data quality
2. **Automation** - Tasks, Streams, và CDC cho real-time data pipeline
3. **Security** - Role-based access control và masking policies, secure data sharing
4. **Performance** - Query optimization với EXPLAIN và profiling
5. **Data transformation** - Snowpark Python, UDFs

---

## 🎯 Mục Tiêu Học Tập

Đồ án này áp dụng kiến thức từ các bài học:
- ✅ **Bài 1:** Roles & Access Control, Stages, File Formats, Data Loading từ files
- ✅ **Bài 2:** Tasks, Data Transformation & Automation, Data Modeling, Medallion Architecture
- ✅ **Bài 3:** Snowpipe, Snowflake Streams
- ✅ **Bài 4:** Performance Optimization, Security, Secure Data Sharing
- ✅ **Bài 5:** Snowpark, UDFs (SQL & Python)

---

## 📊 Bộ Dữ Liệu TPC-H

### Giới Thiệu
TPC-H là bộ dữ liệu benchmark chuẩn cho các hệ thống phân tích kinh doanh, bao gồm:
- 8 bảng dữ liệu liên quan
- Dữ liệu về khách hàng, đơn hàng, sản phẩm, nhà cung cấp
- Nhiều quy mô dữ liệu khác nhau

### Các Bảng Dữ Liệu

Database: `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1` (1GB data)

**8 bảng chính:**

1. **CUSTOMER** - Thông tin khách hàng (150,000 dòng)
   - C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT

2. **ORDERS** - Đơn hàng (1,500,000 dòng)
   - O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT

3. **LINEITEM** - Chi tiết đơn hàng (6,000,000 dòng)
   - L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE

4. **PART** - Sản phẩm (200,000 dòng)
   - P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT

5. **SUPPLIER** - Nhà cung cấp (10,000 dòng)
   - S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE, S_ACCTBAL, S_COMMENT

6. **PARTSUPP** - Quan hệ sản phẩm-nhà cung cấp (800,000 dòng)
   - PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT

7. **NATION** - Quốc gia (25 dòng)
   - N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT

8. **REGION** - Khu vực (5 dòng)
   - R_REGIONKEY, R_NAME, R_COMMENT


## 🗂️ Phần 1: Cài Đặt Môi Trường & Quản Lý Truy Cập

### Yêu Cầu:

**1.1 Tạo Roles và Phân Quyền**

```sql
-- Tạo các roles cho dự án
TPCH_ADMIN;           -- Quản trị toàn bộ
TPCH_DEVELOPER;       -- Developer: Load data, transform
TPCH_ANALYST;         -- Analyst: Query, report
TPCH_VIEWER;          -- Viewer: Chỉ xem reports
```

**1.2 Tạo Database và Schemas**

```sql
-- Tạo database cho dự án
TPCH_ANALYTICS_DB;

-- Tạo các schemas
TPCH_ANALYTICS_DB.STAGING;      -- Dữ liệu gốc từ files
TPCH_ANALYTICS_DB.ANALYTICS;    -- Dữ liệu đã biến đổi
TPCH_ANALYTICS_DB.REPORTS;      -- Báo cáo cuối cùng
TPCH_ANALYTICS_DB.UDFS;         -- User-defined functions

-- Grant quyền phù hợp cho các roles trên schemas
```

**1.3 Tạo Stages**

```sql
-- Tạo internal stage cho data files
STAGE TPCH_DATA_STAGE -- Stage chứa TPC-H data files

-- Grant quyền trên stage

-- List files trong stage
```

**1.4 Tạo Các Bảng RAW**

```sql
-- Bảng 1: REGION
CREATE OR REPLACE TABLE REGION (
    R_REGIONKEY NUMBER(38,0),
    R_NAME VARCHAR(25),
    R_COMMENT VARCHAR(152)
);

-- Bảng 2: NATION
CREATE OR REPLACE TABLE NATION (
    N_NATIONKEY NUMBER(38,0),
    N_NAME VARCHAR(25),
    N_REGIONKEY NUMBER(38,0),
    N_COMMENT VARCHAR(152)
);

-- Bảng 3: CUSTOMER
CREATE OR REPLACE TABLE CUSTOMER (
    C_CUSTKEY NUMBER(38,0),
    C_NAME VARCHAR(25),
    C_ADDRESS VARCHAR(40),
    C_NATIONKEY NUMBER(38,0),
    C_PHONE VARCHAR(15),
    C_ACCTBAL NUMBER(12,2),
    C_MKTSEGMENT VARCHAR(10),
    C_COMMENT VARCHAR(117)
);

-- Bảng 4: SUPPLIER
CREATE OR REPLACE TABLE SUPPLIER (
    S_SUPPKEY NUMBER(38,0),
    S_NAME VARCHAR(25),
    S_ADDRESS VARCHAR(40),
    S_NATIONKEY NUMBER(38,0),
    S_PHONE VARCHAR(15),
    S_ACCTBAL NUMBER(12,2),
    S_COMMENT VARCHAR(101)
);

-- Bảng 5: PART
CREATE OR REPLACE TABLE PART (
    P_PARTKEY NUMBER(38,0),
    P_NAME VARCHAR(55),
    P_MFGR VARCHAR(25),
    P_BRAND VARCHAR(10),
    P_TYPE VARCHAR(25),
    P_SIZE NUMBER(38,0),
    P_CONTAINER VARCHAR(10),
    P_RETAILPRICE NUMBER(12,2),
    P_COMMENT VARCHAR(23)
);

-- Bảng 6: PARTSUPP
CREATE OR REPLACE TABLE PARTSUPP (
    PS_PARTKEY NUMBER(38,0),
    PS_SUPPKEY NUMBER(38,0),
    PS_AVAILQTY NUMBER(38,0),
    PS_SUPPLYCOST NUMBER(12,2),
    PS_COMMENT VARCHAR(199)
);

-- Bảng 7: ORDERS
CREATE OR REPLACE TABLE ORDERS (
    O_ORDERKEY NUMBER(38,0),
    O_CUSTKEY NUMBER(38,0),
    O_ORDERSTATUS VARCHAR(1),
    O_TOTALPRICE NUMBER(12,2),
    O_ORDERDATE DATE,
    O_ORDERPRIORITY VARCHAR(15),
    O_CLERK VARCHAR(15),
    O_SHIPPRIORITY NUMBER(38,0),
    O_COMMENT VARCHAR(79)
);

-- Bảng 8: LINEITEM
CREATE OR REPLACE TABLE LINEITEM (
    L_ORDERKEY NUMBER(38,0),
    L_PARTKEY NUMBER(38,0),
    L_SUPPKEY NUMBER(38,0),
    L_LINENUMBER NUMBER(38,0),
    L_QUANTITY NUMBER(12,2),
    L_EXTENDEDPRICE NUMBER(12,2),
    L_DISCOUNT NUMBER(12,2),
    L_TAX NUMBER(12,2),
    L_RETURNFLAG VARCHAR(1),
    L_LINESTATUS VARCHAR(1),
    L_SHIPDATE DATE,
    L_COMMITDATE DATE,
    L_RECEIPTDATE DATE,
    L_SHIPINSTRUCT VARCHAR(25),
    L_SHIPMODE VARCHAR(10),
    L_COMMENT VARCHAR(44)
);
```

**1.5 Load Dữ Liệu từ Files vào Tables**

**Download và Load từ Files thực tế**

```sql
-- Bước 1: Download TPC-H data files được cung cấp (csv format)

-- Bước 2: Upload files lên stage

-- Bước 3: Verify files đã upload

-- Bước 4: Load data từ stage vào tables
```

**1.6 Kiểm Tra Dữ Liệu và Phân Quyền**

```sql
-- Kiểm tra số lượng records trong mỗi bảng đẩy đủ với file gốc

-- Phân quyền role nào được access vào data raw
```

**Sản phẩm nộp:**
- [ ] File SQL: `01_database_stage_roles.sql`
- [ ] Screenshot: Cấu trúc database và số dòng mỗi bảng
- [ ] Screenshot: Danh sách roles và quyền đã grant
- [ ] Screenshot: Stages với files đã upload

---

## 🏗️ Phần 2: Xây Dựng Data Pipeline với Medallion Architecture

### Yêu Cầu:

Xây dựng data pipeline theo kiến trúc **Medallion (Bronze → Silver → Gold)** với automation sử dụng **Tasks** và **Streams**.

**Kiến trúc tổng quan:**
- **Bronze Layer:** Raw tables (ORDERS, CUSTOMER, LINEITEM...) - Dữ liệu thô được load từ files qua stage
- **Silver Layer:** Cleaned & enriched tables - Dữ liệu đã làm sạch, deduplicate, và enrich
- **Gold Layer:** Aggregated metrics tables - KPIs và metrics cho business
- **Streams:** Capture changes từ Bronze tables (CDC - Change Data Capture)
- **Tasks:** Tự động hóa transformations từ Bronze → Silver → Gold

> **💡 Lưu ý quan trọng:**  
> Bronze Layer = Các bảng raw (ORDERS, CUSTOMER, LINEITEM, PART, SUPPLIER, PARTSUPP, NATION, REGION) đã được tạo và load dữ liệu ở **Phần 1**.  
> Ở Phần 2 này, chúng ta sẽ:
> 1. Tạo Silver & Gold layers
> 2. Setup Streams để track changes trên Bronze tables
> 3. Tạo Stored Procedures để transform dữ liệu
> 4. Tạo Tasks để tự động hóa pipeline
> 5. Test incremental loading: Files mới → COPY INTO Bronze → Stream capture → Task transform → Silver/Gold updated

**2.1 Bronze Layer (Raw Data) - Đã có sẵn từ Phần 1**

```sql

-- Kiểm tra dữ liệu Bronze layer
SELECT 'ORDERS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM ORDERS
UNION ALL
SELECT 'CUSTOMER', COUNT(*) FROM CUSTOMER
UNION ALL
SELECT 'LINEITEM', COUNT(*) FROM LINEITEM;
```

**2.2 Tạo Silver Layer (Cleaned & Enriched Data)**

```sql
-- Silver Table: Orders với dữ liệu đã làm sạch và làm giàu
CREATE OR REPLACE TABLE ORDERS_SILVER (
    O_ORDERKEY          NUMBER(38,0) PRIMARY KEY,
    O_CUSTKEY           NUMBER(38,0),
    O_ORDERSTATUS       VARCHAR(1),
    O_ORDERSTATUS_DESC  VARCHAR(20),          -- Enriched
    O_TOTALPRICE        NUMBER(12,2),
    O_ORDERDATE         DATE,
    O_ORDER_YEAR        NUMBER(4,0),          -- Derived
    O_ORDER_MONTH       NUMBER(2,0),          -- Derived
    O_ORDER_QUARTER     NUMBER(1,0),          -- Derived
    O_ORDERPRIORITY     VARCHAR(15),
    O_PRIORITY_RANK     NUMBER(1,0),          -- Derived
    O_CLERK             VARCHAR(15),
    O_CLERK_ID          NUMBER(9,0),          -- Derived
    O_SHIPPRIORITY      NUMBER(38,0),
    O_COMMENT           VARCHAR(79),
    -- Metadata columns
    SOURCE_FILE         VARCHAR(256),
    FIRST_LOADED_AT     TIMESTAMP_NTZ,
    LAST_UPDATED_AT     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Silver Table: Customers với enrichment
CREATE OR REPLACE TABLE CUSTOMER_SILVER (
    C_CUSTKEY           NUMBER(38,0) PRIMARY KEY,
    C_NAME              VARCHAR(25),
    C_ADDRESS           VARCHAR(40),
    C_NATIONKEY         NUMBER(38,0),
    C_NATION_NAME       VARCHAR(25),          -- Joined from NATION
    C_REGIONKEY         NUMBER(38,0),         -- Joined from NATION->REGION
    C_REGION_NAME       VARCHAR(25),          -- Joined from REGION
    C_PHONE             VARCHAR(15),
    C_ACCTBAL           NUMBER(12,2),
    C_MKTSEGMENT        VARCHAR(10),
    C_COMMENT           VARCHAR(117),
    LOAD_TIMESTAMP      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Silver Table: Lineitem với enrichment
CREATE OR REPLACE TABLE LINEITEM_SILVER (
    L_ORDERKEY          NUMBER(38,0),
    L_LINENUMBER        NUMBER(38,0),
    L_PARTKEY           NUMBER(38,0),
    L_PART_NAME         VARCHAR(55),          -- Joined from PART
    L_PART_TYPE         VARCHAR(25),          -- Joined from PART
    L_SUPPKEY           NUMBER(38,0),
    L_SUPPLIER_NAME     VARCHAR(25),          -- Joined from SUPPLIER
    L_QUANTITY          NUMBER(12,2),
    L_EXTENDEDPRICE     NUMBER(12,2),
    L_DISCOUNT          NUMBER(12,2),
    L_TAX               NUMBER(12,2),
    L_RETURNFLAG        VARCHAR(1),
    L_LINESTATUS        VARCHAR(1),
    L_SHIPDATE          DATE,
    L_COMMITDATE        DATE,
    L_RECEIPTDATE       DATE,
    L_SHIPINSTRUCT      VARCHAR(25),
    L_SHIPMODE          VARCHAR(10),
    L_COMMENT           VARCHAR(44),
    -- Calculated columns
    L_NET_PRICE         NUMBER(12,2),        -- EXTENDEDPRICE * (1 - DISCOUNT)
    L_FINAL_PRICE       NUMBER(12,2),        -- NET_PRICE * (1 + TAX)
    L_SHIP_DELAY_DAYS   NUMBER(38,0),        -- Days between commit and receipt
    LOAD_TIMESTAMP      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (L_ORDERKEY, L_LINENUMBER)
);
```

**2.3 Tạo Gold Layer (Aggregated & Business Metrics)**

```sql

-- Gold Table để tính Daily Sales Summary
CREATE OR REPLACE TABLE DAILY_SALES_SUMMARY (
    SUMMARY_DATE        DATE PRIMARY KEY,
    ORDER_YEAR          NUMBER(4,0),
    ORDER_MONTH         NUMBER(2,0),
    ORDER_QUARTER       NUMBER(1,0),
    TOTAL_ORDERS        NUMBER(38,0),
    TOTAL_CUSTOMERS     NUMBER(38,0),
    TOTAL_REVENUE       NUMBER(15,2),
    AVG_ORDER_VALUE     NUMBER(15,2),
    MIN_ORDER_VALUE     NUMBER(15,2),
    MAX_ORDER_VALUE     NUMBER(15,2),
    LOAD_TIMESTAMP      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Gold Table để tính Customer Lifetime Value
CREATE OR REPLACE TABLE CUSTOMER_LTV (
    C_CUSTKEY           NUMBER(38,0) PRIMARY KEY,
    C_NAME              VARCHAR(25),
    C_NATION_NAME       VARCHAR(25),
    C_REGION_NAME       VARCHAR(25),
    C_MKTSEGMENT        VARCHAR(10),
    TOTAL_ORDERS        NUMBER(38,0),
    TOTAL_SPENT         NUMBER(15,2),
    AVG_ORDER_VALUE     NUMBER(15,2),
    FIRST_ORDER_DATE    DATE,
    LAST_ORDER_DATE     DATE,
    CUSTOMER_TENURE_DAYS NUMBER(38,0),
    CUSTOMER_TIER       VARCHAR(20),          -- VIP, GOLD, SILVER, BRONZE, STANDARD
    IS_ACTIVE           BOOLEAN,              -- Has order in last 90 days
    LOAD_TIMESTAMP      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

**2.4 Tạo Streams cho CDC (Change Data Capture)**

```sql
-- Stream trên các bảng raw (Bronze layer) để track changes

-- Verify streams

-- Check stream contents
```

**2.5 Tạo Stored Procedures cho Transformation Logic**

```sql
-- Bước 1: Transform Bronze → Silver
-- Tạo Stored Procedures để transform data từ Broze -> Silver với những cột dữ liệu mới, logic tính toán, join những cột mới đã note ở phần trên.

Ví dụ: LINEITEM_SILVER table
L_NET_PRICE = EXTENDEDPRICE * (1 - DISCOUNT)

-- Bước 2: Transform Silver → Gold (Customer LTV)
-- Tạo Stored Procedures để transform data từ Silver -> Gold với những cột dữ liệu mới, logic tính toán, join những cột mới.

-- Logic tính toán có thể tham khảo như sau
PROCEDURE SP_CALCULATE_CUSTOMER_LTV()
        DATEDIFF('day', MIN(OS.O_ORDERDATE), MAX(OS.O_ORDERDATE)) AS CUSTOMER_TENURE_DAYS,
        -- Customer Tier based on total spending
        CASE 
            WHEN SUM(OS.O_TOTALPRICE) >= 500000 THEN 'VIP'
            WHEN SUM(OS.O_TOTALPRICE) >= 200000 THEN 'GOLD'
            WHEN SUM(OS.O_TOTALPRICE) >= 100000 THEN 'SILVER'
            WHEN SUM(OS.O_TOTALPRICE) >= 50000 THEN 'BRONZE'
            ELSE 'STANDARD'
        END AS CUSTOMER_TIER,
        -- Active if order in last 90 days
        MAX(OS.O_ORDERDATE) >= DATEADD('day', -90, CURRENT_DATE()) AS IS_ACTIVE
```

**2.6 Tạo Tasks để Tự Động Hóa Pipeline**

```sql
-- Task 1: Bronze → Silver (Orders - Incremental via Stream)

-- Task 2: Bronze → Silver (Customers - Incremental via Stream)

-- Task 3: Silver → Gold (Customer LTV)

-- Task 4: Silver → Gold (Daily Sales Summary)

-- Check task status

-- Lưu ý: Khi có files mới được COPY INTO Bronze tables,
-- Streams sẽ capture changes và trigger tasks tự động
```

**2.7 Monitor và Test Pipeline**

```sql
-- ============================================================================
-- TEST 1: Simulate incremental data loading vào Bronze layer
-- ============================================================================

-- Simulate loading from new files
-- 1. Prepare new data file and upload to stage
-- 2. Run COPY INTO to load data vào tables ở Bronze layer

-- ============================================================================
-- TEST 2: Check Stream has captured changes
-- ============================================================================

-- Kiểm tra streams có change information

-- ============================================================================
-- TEST 3: Verify data flowed through pipeline
-- ============================================================================

-- Kiểm tra bảng ở Silver layer được update

-- Check stream consume

-- Check Gold layer metrics được update

-- ============================================================================
-- TEST 4: Monitor task execution history
-- ============================================================================

-- Xem task execution history
```

**Data Flow Architecture:**

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          DATA PIPELINE FLOW                              │
└─────────────────────────────────────────────────────────────────────────┘

📁 DATA FILES (orders.csv, customers.csv, lineitem.csv...)
            │
            │ [Upload Files]
            ↓
🗄️  STAGE (TPCH_DATA_STAGE)
            │
            │ [COPY INTO / Snowpipe]
            ↓
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃  🟤 BRONZE LAYER (Raw Data) - STAGING Schema                      ┃
┃     • ORDERS                                                       ┃
┃     • CUSTOMER                                                     ┃
┃     • LINEITEM                                                     ┃
┃     • PART, SUPPLIER, PARTSUPP, NATION, REGION                    ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
            │
            │ [Streams Capture Changes - CDC]
            ↓
📊 STREAMS (STREAM_ORDERS, STREAM_CUSTOMER, STREAM_LINEITEM)
            │
            │ [Tasks Trigger on Data]
            ↓
🔄 TASKS (TASK_BRONZE_TO_SILVER_*)
            │
            │ [Stored Procedures Transform Data]
            ↓
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃  🥈 SILVER LAYER (Cleaned & Enriched) - SILVER Schema             ┃
┃     • ORDERS_SILVER (+ status desc, date parts, clerk ID)         ┃
┃     • CUSTOMER_SILVER (+ nation name, region name)                ┃
┃     • LINEITEM_SILVER (+ part name, supplier, net price)          ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
            │
            │ [Tasks Aggregate Data]
            ↓
🔄 TASKS (TASK_SILVER_TO_GOLD_*)
            │
            │ [Calculate Business Metrics]
            ↓
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃  🥇 GOLD LAYER (Business Metrics) - GOLD Schema                   ┃
┃     • CUSTOMER_LTV (Customer Lifetime Value)                      ┃
┃     • DAILY_SALES_SUMMARY (Daily KPIs)                            ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
            │
            ↓
📊 REPORTS & DASHBOARDS
```

**Key Features:**
- ✅ **Incremental Loading**: Streams capture only changes, không cần full reload
- ✅ **Automation**: Tasks tự động chạy khi có data mới
- ✅ **Data Quality**: Deduplicate, enrich, và validate tại Silver layer
- ✅ **Scalability**: Bronze → Silver → Gold cho phép scale từng layer độc lập
- ✅ **Real-time**: Snowpipe + Streams + Tasks = near real-time pipeline

**Sản phẩm nộp:**
- [ ] File SQL: `02_medallion_data_pipeline_automation.sql`
- [ ] Diagram: Data flow từ Files → Stage → Bronze → Silver → Gold

---

## 📥 Phần 3: Khám Phá & Kiểm Tra Dữ Liệu

### Yêu Cầu:

**3.1 Data Profiling - Khám phá dữ liệu**

Viết queries để hiểu dữ liệu:

```sql
-- 1. Đếm số dòng mỗi bảng

-- 2. Phân tích khách hàng theo quốc gia
Example: 

```sql
SELECT 
    N.N_NAME AS COUNTRY,
    COUNT(DISTINCT C.C_CUSTKEY) AS CUSTOMER_COUNT
FROM CUSTOMER C
JOIN NATION N ON C.C_NATIONKEY = N.N_NATIONKEY
GROUP BY N.N_NAME
ORDER BY CUSTOMER_COUNT DESC;

-- 3. Phân tích đơn hàng theo trạng thái

-- 4. Top 10 sản phẩm được bán nhiều nhất
```

**3.2 Data Quality Checks**

```sql
-- Kiểm tra NULL values

-- Kiểm tra duplicates

```


**3.3 Performance Optimization với EXPLAIN**

```sql
-- Sử dụng EXPLAIN để phân tích query execution plan

-- Phân tích Query Profile trong Snowsight UI sau khi chạy

-- Kiểm tra Query History
```

**Sản phẩm nộp:**
- [ ] File SQL: `03_data_quality_check.sql`

---

## 🔒 Phần 4: Security - Masking Policies & Data Sharing

### Yêu Cầu:

**4.1 Tạo Bảng với Sensitive Data**

```sql

-- Tạo bảng customers với thông tin nhạy cảm
CREATE OR REPLACE TABLE CUSTOMER_SENSITIVE AS
SELECT 
    C.C_CUSTKEY,
    C.C_NAME,
    C.C_ADDRESS,
    C.C_PHONE,
    C.C_ACCTBAL,
    N.N_NAME AS NATION,
    R.R_NAME AS REGION,
    C.C_MKTSEGMENT,
    -- Thêm thông tin nhạy cảm (giả lập)
    'customer_' || C.C_CUSTKEY || '@company.com' AS EMAIL,
    LPAD(ABS(MOD(C.C_CUSTKEY * 123456789, 1000000000)), 9, '0') AS SSN_LAST_9
FROM TPCH_ANALYTICS_DB.STAGING.CUSTOMER C
JOIN TPCH_ANALYTICS_DB.STAGING.NATION N ON C.C_NATIONKEY = N.N_NATIONKEY
JOIN TPCH_ANALYTICS_DB.STAGING.REGION R ON N.N_REGIONKEY = R.R_REGIONKEY;
```

**4.2 Tạo Masking Policies**

```sql
-- Masking policy cho EMAIL

-- Masking policy cho PHONE

-- Masking policy cho SSN

-- Masking policy cho ACCOUNT BALANCE
```

**4.3 Apply Masking Policies**

```sql
-- Apply policies vào columns;
```

**4.4 Test Masking Policies**

```sql
-- Test với role ADMIN (xem full data)

-- Test với role ANALYST (xem partial data)

-- Test với role VIEWER (data bị mask)
```

**4.5 Secure Data Sharing**

```sql
-- Tạo secure view cho external sharing

-- Note: Không bao gồm EMAIL, PHONE, SSN, BALANCE

-- Tạo secure share

-- View share details
```

**Sản phẩm nộp:**
- [ ] File SQL: `04_masking_policies_secure_data_sharing.sql`
- [ ] Screenshot: Masking results với các roles khác nhau

---

## 🐍 Phần 5: Snowpark Python Analytics & UDFs

### Yêu Cầu:

Tạo file: `05_snowpark.py`

**5.1 Customer Segmentation với RFM**

```python
"""
Customer RFM Segmentation using Snowpark
"""    
    # Calculate RFM metrics
    rfm_df = (customers
        .join(orders, customers["C_CUSTKEY"] == orders["O_CUSTKEY"], "left")
        .group_by("C_CUSTKEY", "C_NAME")
        .agg([
            max_("O_ORDERDATE").alias("LAST_ORDER_DATE"),
            count("O_ORDERKEY").alias("FREQUENCY"),
            sum_("O_TOTALPRICE").alias("MONETARY")
        ])
        .with_column("RECENCY_DAYS", 
            datediff("day", col("LAST_ORDER_DATE"), current_date()))
    )
    
    # Save to table
    rfm_df.write.mode("overwrite").save_as_table("CUSTOMER_RFM_SCORES")
    
    print(f"✅ RFM Segmentation completed!")
    print(f"   Total customers processed: {rfm_df.count()}")
    
    # Show sample
    rfm_df.show(10)
```

**5.2 Sales Trend Analysis**

```python
"""
Sales Trend Analysis using Snowpark
"""

def analyze_sales_trend(session):
    """Analyze monthly sales trends"""
    
    # Monthly aggregation
    monthly_sales = (orders
        .with_column("MONTH", date_trunc("month", col("O_ORDERDATE")))
        .group_by("MONTH")
        .agg([
            count("O_ORDERKEY").alias("ORDER_COUNT"),
            sum_("O_TOTALPRICE").alias("TOTAL_REVENUE"),
            avg("O_TOTALPRICE").alias("AVG_ORDER_VALUE")
        ])
        .sort("MONTH")
    )
    
    # Convert to pandas for visualization
    df_pandas = monthly_sales.to_pandas()
```

Tạo file: `05_udfs.sql`

**5.3 SQL UDFs**

```sql
-- UDF 1: Phân loại khách hàng theo revenue
    CASE 
        WHEN total_revenue >= 500000 THEN 'VIP'
        WHEN total_revenue >= 200000 THEN 'GOLD'
        WHEN total_revenue >= 100000 THEN 'SILVER'
        WHEN total_revenue >= 50000 THEN 'BRONZE'
        ELSE 'STANDARD'
    END

-- UDF 2: Validate phone number

-- UDF 3: Validate email
```


**Sản phẩm nộp:**
- [ ] `05_snowpark.py`
- [ ] `05_udfs.sql`
- [ ] Screenshots kết quả test

---

## 🎁 Bonus Challenges (Tùy chọn, +10 điểm)

**Advanced Visualizations & Dashboards**
- Kết nối Tableau/Power BI/ open-source BI tools (Superset) và tạo professional dashboard, screenshot kết quả

---

## 📝 Yêu Cầu Nộp Bài

### Cấu Trúc Thư Mục:

```
tpch_analytics_project/
├── README.md                               # Tài liệu dự án
├── src/
│   # PART 1: Setup & Access Control
│   ├── 01_database_stage_roles.sql
│   ├── 01_screenshot.png
│   
│   # PART 2: Medallion Architecture & Data pipeline Automation
│   ├── 02_medallion_data_pipeline_automation.sql
│   ├── 02_screenshot.png
│   
│   # PART 3: Data Exploration & Quality
│   ├── 03_data_quality_check.sql
│   ├── 03_screenshot.png
│   
│   # PART 4: Security Data Masking
│   ├── 04_masking_policies_secure_data_sharing.sql
│   ├── 04_screenshot.png
│   
│   # PART 5: Snowpark UDFs
│   ├── 05_snowpark.py
│   ├── 05_udfs.sql
│   ├── 05_screenshot.png
│   
└── bonus/                              # Nếu làm bonus
    └── visualizations.pdf
```

### File README.md phải bao gồm:

1. **Tổng quan dự án**
2. **Hướng dẫn setup**
   - Cách tạo database, phân quyền
3. **Hướng dẫn chạy**
   - Thứ tự chạy các file SQL
   - Cách chạy Python scripts
4. **Kết quả chính**
   - Insights từ phân tích
   - Screenshots quan trọng
6. **Tính năng bonus** (nếu có)

---

## 📚 Tài Liệu Tham Khảo

- [TPC-H Sample Data](https://docs.snowflake.com/en/user-guide/sample-data-tpch)
- [Snowpark Developer Guide](https://docs.snowflake.com/en/developer-guide/snowpark/python/index.html)
- [UDF Best Practices](https://docs.snowflake.com/en/developer-guide/udf/udf-overview.html)

---

## 🚀 Chúc Bạn Thành Công!

**Lưu ý:** TPC-H là bộ dữ liệu rất quen thuộc trong ngành. Làm tốt đồ án này cho thấy bạn có khả năng làm việc với dữ liệu thực tế quy mô lớn.

---

**Deadline:** 15/12/2025

**Cách nộp bài:** Google Classroom 

**Liên hệ:** hungts510@gmail.com

