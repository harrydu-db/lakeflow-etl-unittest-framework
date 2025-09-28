CREATE VIEW IF NOT EXISTS DEMO.PRODUCT_V AS
SELECT     
    product_id,
    product_name,
    category,
    product_status
FROM DEMO.PRODUCT_DETAILS;