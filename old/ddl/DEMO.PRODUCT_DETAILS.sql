CREATE TABLE DEMO.PRODUCT_DETAILS (
    product_id STRING,
    product_name STRING,
    category STRING,
    sale_count BIGINT,
    total_revenue DECIMAL(18,2),
    created_date DATE,
    last_updated TIMESTAMP,
    product_status STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.feature.catalogOwned-preview' = 'supported', 
  'delta.feature.allowColumnDefaults' = 'supported'
);

-- Sample data for PRODUCT_DETAILS table
INSERT INTO DEMO.PRODUCT_DETAILS VALUES
('P001', 'iPhone 15 Pro', 'ELEC', 1250, 1875000.00, '2024-01-15', '2024-12-01 10:30:00', 'ACTIVE'),
('P002', 'Samsung Galaxy S24', 'ELEC', 980, 1176000.00, '2024-02-01', '2024-12-01 11:15:00', 'ACTIVE'),
('P003', 'MacBook Pro M3', 'ELEC', 750, 2250000.00, '2024-01-20', '2024-12-01 09:45:00', 'ACTIVE'),
('P004', 'Nike Air Max 270', 'SPORT', 2100, 315000.00, '2024-01-10', '2024-12-01 14:20:00', 'ACTIVE'),
('P005', 'Adidas Ultraboost 22', 'SPORT', 1800, 270000.00, '2024-01-25', '2024-12-01 13:10:00', 'ACTIVE'),
('P006', 'The Great Gatsby', 'BOOKS', 3200, 48000.00, '2024-01-05', '2024-12-01 16:30:00', 'ACTIVE'),
('P007', 'Dune Messiah', 'BOOKS', 1500, 30000.00, '2024-02-10', '2024-12-01 15:45:00', 'ACTIVE'),
('P008', 'Levi''s 501 Jeans', 'CLOTH', 2800, 420000.00, '2024-01-12', '2024-12-01 12:00:00', 'ACTIVE'),
('P009', 'Zara Blazer', 'CLOTH', 1200, 180000.00, '2024-02-05', '2024-12-01 11:30:00', 'ACTIVE'),
('P010', 'KitchenAid Mixer', 'HOME', 450, 135000.00, '2024-01-18', '2024-12-01 08:15:00', 'ACTIVE'),
('P011', 'Dyson V15 Vacuum', 'HOME', 380, 190000.00, '2024-02-08', '2024-12-01 07:45:00', 'ACTIVE'),
('P012', 'Lego Creator Set', 'TOYS', 2200, 110000.00, '2024-01-30', '2024-12-01 17:20:00', 'ACTIVE'),
('P013', 'Barbie Dreamhouse', 'TOYS', 1800, 270000.00, '2024-02-15', '2024-12-01 18:00:00', 'ACTIVE'),
('P014', 'Olay Regenerist Cream', 'BEAUTY', 3200, 128000.00, '2024-01-22', '2024-12-01 19:30:00', 'ACTIVE'),
('P015', 'Michelin Pilot Sport Tires', 'AUTO', 150, 45000.00, '2024-02-12', '2024-12-01 20:15:00', 'ACTIVE');