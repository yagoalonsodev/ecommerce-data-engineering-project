-- Schema estrella para warehouse eCommerce (NeonDB / PostgreSQL)
-- Ejecutar ANTES de escribir datos con PySpark (run_schema_neon.py o manualmente).

-- Dimensiones
CREATE TABLE IF NOT EXISTS dim_products (
    product_id   VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(500),
    category     VARCHAR(200),
    subcategory  VARCHAR(200),
    product_brand VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id    VARCHAR(50) PRIMARY KEY,
    customer_name  VARCHAR(300),
    customer_gender VARCHAR(20),
    customer_age   NUMERIC(5, 2),
    country        VARCHAR(100),
    city           VARCHAR(200),
    customer_email VARCHAR(320)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key     DATE PRIMARY KEY,
    year         SMALLINT NOT NULL,
    month        SMALLINT NOT NULL,
    day          SMALLINT NOT NULL,
    week_of_year SMALLINT NOT NULL,
    quarter      SMALLINT NOT NULL
);

-- Tabla de hechos (sin FK para permitir overwrite ETL; integridad vía aplicación/consultas)
CREATE TABLE IF NOT EXISTS fact_sales (
    order_id       VARCHAR(50) NOT NULL,
    customer_id    VARCHAR(50) NOT NULL,
    product_id     VARCHAR(50) NOT NULL,
    date_key       DATE NOT NULL,
    quantity       INTEGER NOT NULL,
    unit_price     NUMERIC(12, 2) NOT NULL,
    total_amount   NUMERIC(12, 2) NOT NULL,
    discount       NUMERIC(6, 2) NOT NULL,
    total_revenue  NUMERIC(12, 2) NOT NULL,
    payment_method VARCHAR(50),
    order_status   VARCHAR(50),
    order_year     SMALLINT,
    order_month    SMALLINT,
    PRIMARY KEY (order_id, product_id)
);

-- Índices para consultas analíticas
CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON fact_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON fact_sales(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_order_year_month ON fact_sales(order_year, order_month);
