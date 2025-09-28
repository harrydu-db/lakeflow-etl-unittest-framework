CREATE TABLE DEMO.SALES_REPORTING (
    report_date DATE,
    product_id STRING,
    product_name STRING,
    category_name STRING,
    sale_count BIGINT,
    total_revenue DECIMAL(18,2),
    region STRING,
    sales_channel STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.feature.catalogOwned-preview' = 'supported', 
  'delta.feature.allowColumnDefaults' = 'supported'
);

-- Sample data for SALES_REPORTING table (denormalized data matching other tables)
INSERT INTO DEMO.SALES_REPORTING VALUES
('2024-11-01', 'P001', 'iPhone 15 Pro', 'Electronics', 3, 4500.00, 'North America', 'Online Store'),
('2024-11-01', 'P002', 'Samsung Galaxy S24', 'Electronics', 3, 3600.00, 'Asia Pacific', 'Mobile App'),
('2024-11-01', 'P004', 'Nike Air Max 270', 'Sports & Outdoors', 5, 750.00, 'Latin America', 'Retail Store'),
('2024-11-01', 'P006', 'The Great Gatsby', 'Books & Media', 10, 150.00, 'Asia Pacific', 'Online Store'),
('2024-11-01', 'P012', 'Lego Creator Set', 'Toys & Games', 4, 200.00, 'Latin America', 'Online Store'),
('2024-11-01', 'P015', 'Michelin Pilot Sport Tires', 'Automotive', 1, 300.00, 'Europe', 'Partner Channel'),
('2024-11-02', 'P001', 'iPhone 15 Pro', 'Electronics', 1, 1500.00, 'Europe', 'Retail Store'),
('2024-11-02', 'P005', 'Adidas Ultraboost 22', 'Sports & Outdoors', 4, 600.00, 'Europe', 'Wholesale'),
('2024-11-02', 'P008', 'Levi''s 501 Jeans', 'Clothing', 3, 450.00, 'Latin America', 'Retail Store'),
('2024-11-02', 'P011', 'Dyson V15 Vacuum', 'Home & Garden', 1, 500.00, 'North America', 'Retail Store'),
('2024-11-02', 'P013', 'Barbie Dreamhouse', 'Toys & Games', 2, 300.00, 'Asia Pacific', 'Mobile App'),
('2024-11-03', 'P003', 'MacBook Pro M3', 'Electronics', 1, 3000.00, 'North America', 'Online Store'),
('2024-11-03', 'P007', 'Dune Messiah', 'Books & Media', 6, 120.00, 'Europe', 'Online Store'),
('2024-11-03', 'P010', 'KitchenAid Mixer', 'Home & Garden', 1, 300.00, 'Asia Pacific', 'Online Store'),
('2024-11-03', 'P014', 'Olay Regenerist Cream', 'Beauty & Health', 8, 320.00, 'North America', 'Online Store'),
('2024-11-04', 'P004', 'Nike Air Max 270', 'Sports & Outdoors', 2, 300.00, 'North America', 'Online Store'),
('2024-11-04', 'P008', 'Levi''s 501 Jeans', 'Clothing', 2, 300.00, 'North America', 'Online Store'),
('2024-11-05', 'P006', 'The Great Gatsby', 'Books & Media', 8, 120.00, 'North America', 'Retail Store'),
('2024-11-05', 'P012', 'Lego Creator Set', 'Toys & Games', 3, 150.00, 'Europe', 'Retail Store'),
('2024-11-06', 'P001', 'iPhone 15 Pro', 'Electronics', 1, 1500.00, 'Asia Pacific', 'Online Store'),
('2024-11-06', 'P002', 'Samsung Galaxy S24', 'Electronics', 2, 2400.00, 'North America', 'Retail Store'),
('2024-11-07', 'P003', 'MacBook Pro M3', 'Electronics', 1, 3000.00, 'Europe', 'Online Store'),
('2024-11-08', 'P004', 'Nike Air Max 270', 'Sports & Outdoors', 3, 450.00, 'Latin America', 'Mobile App'),
('2024-11-09', 'P005', 'Adidas Ultraboost 22', 'Sports & Outdoors', 2, 300.00, 'Asia Pacific', 'Wholesale'),
('2024-11-10', 'P006', 'The Great Gatsby', 'Books & Media', 5, 75.00, 'North America', 'Online Store'),
('2024-11-11', 'P007', 'Dune Messiah', 'Books & Media', 3, 60.00, 'Europe', 'Retail Store'),
('2024-11-12', 'P008', 'Levi''s 501 Jeans', 'Clothing', 1, 150.00, 'Latin America', 'Online Store'),
('2024-11-13', 'P009', 'Zara Blazer', 'Clothing', 2, 300.00, 'Asia Pacific', 'Call Center'),
('2024-11-14', 'P010', 'KitchenAid Mixer', 'Home & Garden', 1, 300.00, 'North America', 'Retail Store');