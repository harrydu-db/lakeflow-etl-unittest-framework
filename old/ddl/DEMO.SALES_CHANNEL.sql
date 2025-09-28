CREATE TABLE DEMO.SALES_CHANNEL (
    channel_id STRING,
    channel_name STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.feature.catalogOwned-preview' = 'supported', 
  'delta.feature.allowColumnDefaults' = 'supported'
);

-- Sample data for SALES_CHANNEL table
INSERT INTO DEMO.SALES_CHANNEL VALUES
('ONLINE', 'Online Store'),
('RETAIL', 'Retail Store'),
('WHOLESALE', 'Wholesale'),
('MOBILE', 'Mobile App'),
('CALL', 'Call Center'),
('PARTNER', 'Partner Channel');