#!/bin/bash
# Sample ETL shell script for testing lineage analysis
# This script demonstrates bteq SQL blocks commonly found in ETL processes

# Set environment variables
export DB_HOST="datawarehouse.company.com"
export DB_USER="etl_user"
export DB_PASS="etl_password"

# Check if stop file exists
if [[ -f /tmp/etl_stop_flag ]]
then
    echo "ETL process stopped by flag file"
    exit 0
fi

# Execute ETL process using bteq
bteq <<EOF
.logon ${DB_HOST}/${DB_USER},${DB_PASS};
.SET ECHOREQ OFF;
.SET ERROROUT STDOUT;
.SET WIDTH 200;
.SET RETRY OFF;

.IF ERRORCODE <> 0 THEN GOTO ErrorHandler

-- Create temporary table for data processing
CREATE VOLATILE TABLE TEMP_SALES_DATA AS (
SELECT DISTINCT 
    p.product_id,
    p.product_name,
    p.category,
    s.sale_id,
    s.sale_date,
    s.quantity,
    s.unit_price,
    s.total_amount
FROM DEMO.PRODUCT_V p
LEFT OUTER JOIN DEMO.SALES_FACT s
ON p.product_id = s.product_id
WHERE p.product_status = 'ACTIVE'
) WITH DATA ON COMMIT PRESERVE ROWS;

.IF ERRORCODE <> 0 THEN GOTO ErrorHandler

BT;

-- Update product summary table
UPDATE A FROM DEMO.PRODUCT_SUMMARY A, 
(SELECT product_id, COUNT(*) as sale_count, SUM(total_amount) as total_revenue 
 FROM TEMP_SALES_DATA 
 GROUP BY product_id) B
SET sale_count = B.sale_count,
    total_revenue = B.total_revenue,
    last_updated = CURRENT_TIMESTAMP
WHERE A.product_id = B.product_id;

.IF ERRORCODE <> 0 THEN GOTO ErrorHandler

-- Insert new product records
INSERT INTO DEMO.PRODUCT_DETAILS
(product_id, product_name, category, sale_count, total_revenue, 
 created_date, last_updated)
SELECT 
    product_id,
    product_name,
    category,
    COUNT(*) as sale_count,
    SUM(total_amount) as total_revenue,
    CURRENT_DATE as created_date,
    CURRENT_TIMESTAMP as last_updated
FROM TEMP_SALES_DATA
GROUP BY product_id, product_name, category;

.IF ERRORCODE <> 0 THEN GOTO ErrorHandler

-- Insert into reporting table with reference data
INSERT INTO DEMO.SALES_REPORTING
(report_date, product_id, product_name, category_name, 
 sale_count, total_revenue, region)
SELECT
    CURRENT_DATE as report_date,
    p.product_id,
    p.product_name,
    c.category_name,
    COUNT(s.sale_id) as sale_count,
    SUM(s.total_amount) as total_revenue,
    r.region_name as region
FROM TEMP_SALES_DATA p
LEFT OUTER JOIN DEMO.CATEGORY c
ON p.category = c.category_code
LEFT OUTER JOIN DEMO.SALES_FACT s
ON p.product_id = s.product_id
LEFT OUTER JOIN DEMO.REGION r
ON s.region_id = r.region_id
GROUP BY p.product_id, p.product_name, c.category_name, r.region_name;

.IF ERRORCODE <> 0 THEN GOTO ErrorHandler

-- Insert into audit log
INSERT INTO DEMO.AUDIT_LOG
(process_name, table_name, record_count, process_date, status)
SELECT
    'SALES_ETL' as process_name,
    'DEMO.SALES_REPORTING' as table_name,
    COUNT(*) as record_count,
    CURRENT_DATE as process_date,
    'COMPLETED' as status
FROM DEMO.SALES_REPORTING
WHERE report_date = CURRENT_DATE;

.IF ERRORCODE <> 0 THEN GOTO ErrorHandler

-- Commit and exit
ET;
.LABEL ErrorHandler;
.export reset;
.logoff;
.quit;
EOF

# Check exit status
if [ $? -eq 0 ]
then
    echo "ETL process completed successfully"
    # Send success notification
    echo "Sales ETL completed at $(date)" | mail -s "ETL Success" admin@company.com
else
    echo "ETL process failed"
    # Send failure notification
    echo "Sales ETL failed at $(date)" | mail -s "ETL Failure" admin@company.com
    exit 1
fi 