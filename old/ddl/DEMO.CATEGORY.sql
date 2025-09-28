CREATE TABLE DEMO.CATEGORY (
    category_code STRING,
    category_name STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.feature.catalogOwned-preview' = 'supported', 
  'delta.feature.allowColumnDefaults' = 'supported'
);

-- Sample data for CATEGORY table
INSERT INTO DEMO.CATEGORY VALUES
('ELEC', 'Electronics'),
('CLOTH', 'Clothing'),
('BOOKS', 'Books & Media'),
('HOME', 'Home & Garden'),
('SPORT', 'Sports & Outdoors'),
('BEAUTY', 'Beauty & Health'),
('AUTO', 'Automotive'),
('TOYS', 'Toys & Games');