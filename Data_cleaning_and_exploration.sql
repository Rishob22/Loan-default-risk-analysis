/* 
   Credit Card Default Drivers — Cleaning + EDA (MySQL 8+)
   */

-- 0) Database
CREATE DATABASE IF NOT EXISTS credit_default_eda;
USE credit_default_eda;

-- 1) Raw table (schema matches the UCI CSV I used)
DROP TABLE IF EXISTS cc_raw;
CREATE TABLE cc_raw (
  id                              INT PRIMARY KEY,
  limit_bal                       INT,
  sex                             TINYINT,
  education                       TINYINT,
  marriage                        TINYINT,
  age                             INT,
  pay_0                           TINYINT,
  pay_2                           TINYINT,
  pay_3                           TINYINT,
  pay_4                           TINYINT,
  pay_5                           TINYINT,
  pay_6                           TINYINT,
  bill_amt1                       INT,
  bill_amt2                       INT,
  bill_amt3                       INT,
  bill_amt4                       INT,
  bill_amt5                       INT,
  bill_amt6                       INT,
  pay_amt1                        INT,
  pay_amt2                        INT,
  pay_amt3                        INT,
  pay_amt4                        INT,
  pay_amt5                        INT,
  pay_amt6                        INT,
  default_payment_next_month      TINYINT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- (I import the CSV into cc_raw using Workbench's import wizard.)

/* 2) Clean table with readable labels and simple features
      - map coded columns to labels
      - create age bands and limit buckets
      - build utilization and payment ratio (last month)
      - build late flags and late count (past 6 months)                      */

DROP TABLE IF EXISTS cc_clean;
CREATE TABLE cc_clean (
  id                INT PRIMARY KEY,
  limit_bal         INT,
  limit_bucket      VARCHAR(20),
  sex_label         VARCHAR(10),
  education_label   VARCHAR(20),
  marriage_label    VARCHAR(12),
  age               INT,
  age_band          VARCHAR(10),
  bill_last         INT,
  pay_last          INT,
  util_last         DECIMAL(6,3),  -- last bill / limit
  util_bucket       VARCHAR(12),
  pay_ratio_last    DECIMAL(6,3),  -- last payment / last bill
  pay_ratio_bucket  VARCHAR(12),
  recent_late_flag  TINYINT,       -- from PAY_0
  late_count_6m     TINYINT,       -- across PAY_0,2..6
  ever_late_6m      TINYINT,       -- late_count_6m > 0
  default_flag      TINYINT        -- target
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO cc_clean
SELECT
  r.id,
  r.limit_bal,

  CASE
    WHEN r.limit_bal <  50000 THEN '0–50k'
    WHEN r.limit_bal < 100000 THEN '50–100k'
    WHEN r.limit_bal < 200000 THEN '100–200k'
    WHEN r.limit_bal < 300000 THEN '200–300k'
    WHEN r.limit_bal < 500000 THEN '300–500k'
    ELSE '500k+'
  END AS limit_bucket,

  CASE r.sex
    WHEN 1 THEN 'Male'
    WHEN 2 THEN 'Female'
    ELSE 'Unknown'
  END AS sex_label,

  CASE r.education
    WHEN 1 THEN 'GradSchool'
    WHEN 2 THEN 'University'
    WHEN 3 THEN 'HighSchool'
    WHEN 4 THEN 'Other'
    ELSE 'Unknown'
  END AS education_label,

  CASE r.marriage
    WHEN 1 THEN 'Married'
    WHEN 2 THEN 'Single'
    WHEN 3 THEN 'Other'
    ELSE 'Unknown'
  END AS marriage_label,

  r.age,

  CASE
    WHEN r.age IS NULL          THEN 'Unknown'
    WHEN r.age < 25             THEN '18–24'
    WHEN r.age BETWEEN 25 AND 34 THEN '25–34'
    WHEN r.age BETWEEN 35 AND 44 THEN '35–44'
    WHEN r.age BETWEEN 45 AND 54 THEN '45–54'
    WHEN r.age BETWEEN 55 AND 64 THEN '55–64'
    ELSE '65+'
  END AS age_band,

  r.bill_amt1 AS bill_last,
  r.pay_amt1  AS pay_last,

  CASE
    WHEN r.limit_bal IS NULL OR r.limit_bal = 0 THEN NULL
    ELSE ROUND(r.bill_amt1 / r.limit_bal, 3)
  END AS util_last,

  CASE
    WHEN r.limit_bal IS NULL OR r.limit_bal = 0 THEN 'Unknown'
    WHEN r.bill_amt1 / r.limit_bal < 0.30 THEN '<0.3'
    WHEN r.bill_amt1 / r.limit_bal < 0.60 THEN '0.3–0.6'
    WHEN r.bill_amt1 / r.limit_bal < 0.90 THEN '0.6–0.9'
    WHEN r.bill_amt1 / r.limit_bal < 1.20 THEN '0.9–1.2'
    ELSE '>=1.2'
  END AS util_bucket,

  CASE
    WHEN r.bill_amt1 IS NULL OR r.bill_amt1 = 0 THEN NULL
    ELSE ROUND(r.pay_amt1 / r.bill_amt1, 3)
  END AS pay_ratio_last,

  CASE
    WHEN r.bill_amt1 IS NULL OR r.bill_amt1 = 0 THEN 'Unknown'
    WHEN r.pay_amt1 / r.bill_amt1 = 0    THEN '0x'
    WHEN r.pay_amt1 / r.bill_amt1 < 0.5  THEN '<0.5x'
    WHEN r.pay_amt1 / r.bill_amt1 < 1.0  THEN '0.5–1x'
    WHEN r.pay_amt1 / r.bill_amt1 < 1.5  THEN '1–1.5x'
    ELSE '>=1.5x'
  END AS pay_ratio_bucket,

  CASE WHEN r.pay_0 >= 1 THEN 1 ELSE 0 END AS recent_late_flag,

  -- count how many months were late in PAY_0, PAY_2..PAY_6
  (
    (CASE WHEN r.pay_0 >= 1 THEN 1 ELSE 0 END) +
    (CASE WHEN r.pay_2 >= 1 THEN 1 ELSE 0 END) +
    (CASE WHEN r.pay_3 >= 1 THEN 1 ELSE 0 END) +
    (CASE WHEN r.pay_4 >= 1 THEN 1 ELSE 0 END) +
    (CASE WHEN r.pay_5 >= 1 THEN 1 ELSE 0 END) +
    (CASE WHEN r.pay_6 >= 1 THEN 1 ELSE 0 END)
  ) AS late_count_6m,

  CASE
    WHEN ( (CASE WHEN r.pay_0 >= 1 THEN 1 ELSE 0 END) +
           (CASE WHEN r.pay_2 >= 1 THEN 1 ELSE 0 END) +
           (CASE WHEN r.pay_3 >= 1 THEN 1 ELSE 0 END) +
           (CASE WHEN r.pay_4 >= 1 THEN 1 ELSE 0 END) +
           (CASE WHEN r.pay_5 >= 1 THEN 1 ELSE 0 END) +
           (CASE WHEN r.pay_6 >= 1 THEN 1 ELSE 0 END) ) > 0
      THEN 1 ELSE 0
  END AS ever_late_6m,

  r.default_payment_next_month AS default_flag
FROM cc_raw r;

-- 3) Indexes that help the EDA run fast
CREATE INDEX idx_default    ON cc_clean (default_flag);
CREATE INDEX idx_age_band   ON cc_clean (age_band);
CREATE INDEX idx_limit_bkt  ON cc_clean (limit_bucket);
CREATE INDEX idx_util_bkt   ON cc_clean (util_bucket);

-- =========================
-- E D A   Q U E R I E S
-- =========================

-- Overall default rate
SELECT
  COUNT(*) AS customers,
  ROUND(AVG(default_flag)*100,2) AS default_rate_pct
FROM cc_clean;

-- Default by age band (ordered so bands read naturally)
SELECT
  age_band,
  COUNT(*) AS customers,
  ROUND(AVG(default_flag)*100,2) AS default_rate_pct
FROM cc_clean
GROUP BY age_band
ORDER BY FIELD(age_band,'18–24','25–34','35–44','45–54','55–64','65+','Unknown');

-- Default by sex
SELECT
  sex_label,
  COUNT(*) AS customers,
  ROUND(AVG(default_flag)*100,2) AS default_rate_pct
FROM cc_clean
GROUP BY sex_label
ORDER BY default_rate_pct DESC;

-- Default by education level
SELECT
  education_label,
  COUNT(*) AS customers,
  ROUND(AVG(default_flag)*100,2) AS default_rate_pct
FROM cc_clean
GROUP BY education_label
ORDER BY default_rate_pct DESC;

-- Default by marital status
SELECT
  marriage_label,
  COUNT(*) AS customers,
  ROUND(AVG(default_flag)*100,2) AS default_rate_pct
FROM cc_clean
GROUP BY marriage_label
ORDER BY default_rate_pct DESC;

-- Default by credit limit bucket
SELECT
  limit_bucket,
  COUNT(*) AS customers,
  ROUND(AVG(default_flag)*100,2) AS default_rate_pct
FROM cc_clean
GROUP BY limit_bucket
ORDER BY
  CASE limit_bucket
    WHEN '0–50k' THEN 1 WHEN '50–100k' THEN 2 WHEN '100–200k' THEN 3
    WHEN '200–300k' THEN 4 WHEN '300–500k' THEN 5 WHEN '500k+' THEN 6
    ELSE 7
  END;

-- Default by utilization bucket (last bill ÷ limit)
SELECT
  util_bucket,
  COUNT(*) AS customers,
  ROUND(AVG(default_flag)*100,2) AS default_rate_pct
FROM cc_clean
GROUP BY util_bucket
ORDER BY
  CASE util_bucket
    WHEN '<0.3' THEN 1 WHEN '0.3–0.6' THEN 2 WHEN '0.6–0.9' THEN 3
    WHEN '0.9–1.2' THEN 4 WHEN '>=1.2' THEN 5 ELSE 6
  END;

-- Default vs recent late flag
SELECT
  recent_late_flag,
  COUNT(*) AS customers,
  ROUND(AVG(default_flag)*100,2) AS default_rate_pct
FROM cc_clean
GROUP BY recent_late_flag;

-- Default by late_count in the last 6 months
SELECT
  late_count_6m,
  COUNT(*) AS customers,
  ROUND(AVG(default_flag)*100,2) AS default_rate_pct
FROM cc_clean
GROUP BY late_count_6m
ORDER BY late_count_6m;

-- Default by payment ratio bucket (last payment ÷ last bill)
SELECT
  pay_ratio_bucket,
  COUNT(*) AS customers,
  ROUND(AVG(default_flag)*100,2) AS default_rate_pct
FROM cc_clean
GROUP BY pay_ratio_bucket
ORDER BY
  CASE pay_ratio_bucket
    WHEN '0x' THEN 1 WHEN '<0.5x' THEN 2 WHEN '0.5–1x' THEN 3
    WHEN '1–1.5x' THEN 4 WHEN '>=1.5x' THEN 5 ELSE 6
  END;
