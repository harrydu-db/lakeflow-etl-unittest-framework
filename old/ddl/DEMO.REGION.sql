CREATE TABLE DEMO.REGION (
    region_id STRING,
    region_name STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.feature.catalogOwned-preview' = 'supported', 
  'delta.feature.allowColumnDefaults' = 'supported'
);

-- Sample data for REGION table
INSERT INTO DEMO.REGION VALUES
('NA', 'North America'),
('EU', 'Europe'),
('AP', 'Asia Pacific'),
('LA', 'Latin America'),
('AF', 'Africa'),
('ME', 'Middle East');