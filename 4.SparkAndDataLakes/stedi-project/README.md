# STEDI Data Engineering Project

---

## Project Overview

This project processes IoT sensor data for the fictional company STEDI. The goal is to build an ETL pipeline that ingests raw JSON data from S3 landing zones, filters and transforms it through trusted and curated zones using AWS Glue jobs, and prepares datasets suitable for machine learning analysis.

Data sources include:

- Customer data  
- Accelerometer sensor readings  
- Step trainer sensor readings  

The data is processed in stages:

1. Landing zone: Raw JSON data stored in S3 and registered in Athena as external tables.  
2. Trusted zone: Filtered and cleaned data with customer consent validation.  
3. Curated zone: Joined and aggregated datasets prepared for analytics and ML workloads.

---

## AWS Resources

- **AWS Glue Database:** `stedi`  
- **S3 Bucket:** `s3://stedi-sri-825/`  
- **IAM Role for Glue Jobs:** `AWSGlueServiceRole-STEDI`  

—
## Landing Tables Schema

The landing tables are created in Athena with the following schemas:

**Customer_landing schema**


**Accelerometer_landing schema** 


**Step_trainer_landing schema**





---
## Glue Jobs and Execution Order

Run the following AWS Glue jobs in this exact order to process and transform the data successfully:

1. **customer_landing_to_trusted**  
   - Filters customers to only those with research consent (`shareWithResearchAsOfDate` not null/empty).  
   - Outputs table: `customer_trusted`  

2. **accelerometer_landing_to_trusted**  
   - Joins accelerometer readings with trusted customers (by matching `user` to customer `email`).  
   - Outputs table: `accelerometer_trusted`  

3. **customer_trusted_to_curated**  
   - Produces curated customer data by joining trusted customers with accelerometer data.  
   - Outputs table: `customer_curated`  

4. **step_trainer_landing_to_step_trainer_trusted**  
   - Filters step trainer readings for customers in curated customer dataset (using serial numbers).  
   - Outputs table: `step_trainer_trusted`  

5. **machine_learning_curated**  
   - Joins step trainer trusted readings with accelerometer trusted readings on matching timestamps.  
   - Outputs table: `machine_learning_curated`  

---

## Validation: Expected Row Counts in Athena

After running each job, run these queries in Athena and verify the counts match:

```sql
-- Landing Zone Counts
SELECT count(*) FROM stedi.customer_landing;       -- Expected: 956
SELECT count(*) FROM stedi.accelerometer_landing;  -- Expected: 81273
SELECT count(*) FROM stedi.step_trainer_landing;   -- Expected: 28680

-- Trusted Zone Counts
SELECT count(*) FROM stedi.customer_trusted;       -- Expected: 482
SELECT count(*) FROM stedi.accelerometer_trusted;  -- Expected: ~40981
SELECT count(*) FROM stedi.step_trainer_trusted;   -- Expected: 14460

-- Curated Zone Counts
SELECT count(*) FROM stedi.customer_curated;        -- Expected: 482
SELECT count(*) FROM stedi.machine_learning_curated;-- Expected: 43681
```

## Notes and Considerations

- The Glue IAM role `AWSGlueServiceRole-STEDI` was created with necessary permissions for S3 access and Glue job execution.
- The pipeline follows the rubric exactly without deviations.
- Timestamp columns are typed correctly in landing tables for efficient querying.
- Join operations in Glue jobs use SQL Transform nodes with appropriate filtering to avoid duplication or zero-result issues.
- No additional anonymization or filtering beyond consent checks was performed.

---

## S3 Bucket and Glue Database Structure

The S3 bucket is organized as follows:

```
s3://stedi-sri-825/
├── customer/
│   ├── landing/
│   ├── trusted/
│   └── curated/
├── accelerometer/
│   ├── landing/
│   └── trusted/
├── step_trainer/
│   ├── landing/
│   └── trusted/
└── ml/
    └── curated/


```

The Glue database `stedi` contains tables corresponding to each stage and data type, as follows:

- `customer_landing`  
- `customer_trusted`  
- `customer_curated`  
- `accelerometer_landing`  
- `accelerometer_trusted`  
- `step_trainer_landing`  
- `step_trainer_trusted`  
- `machine_learning_curated`

---

## Screenshots

All Athena query result screenshots validating the row counts are saved in the `screenshots/` folder, as follows:

- `customer_landing.png`  
- `accelerometer_landing.png`  
- `step_trainer_landing.png`  
- `customer_trusted.png`  
- `accelerometer_trusted.png`  
- `step_trainer_trusted.png`  
- `customer_curated.png`  
- `machine_learning_curated.png`  

Refer to these screenshots to verify that the pipeline outputs match expectations.

---

## How to Run

1. Upload raw JSON data to the S3 landing prefixes.  
2. Run landing table creation queries (located in the `sql/` folder) via Athena.  
3. Run the Glue jobs in the order specified earlier.  
4. Validate output row counts using Athena queries.  
5. Review screenshots as proof of correct processing.

