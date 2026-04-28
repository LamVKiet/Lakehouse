-- MySQL Raw Layer: Backend operational data for Apparel Retail (Omnichannel)
-- Tables: customers, products, branches, pos_transactions, pos_transaction_details,
--         online_transactions, online_transaction_details

CREATE DATABASE IF NOT EXISTS apparel_retail;
USE apparel_retail;

-- ─────────────────────────────────────────────────────────────────────────────
-- DIMENSION: customers
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS customers (
    customer_id         VARCHAR(50)  NOT NULL,
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    phone               VARCHAR(15),
    dob                 DATE,
    age                 INT,
    gender              TINYINT      DEFAULT 0 COMMENT '1=Nam, 2=Nu, 0=Other',
    address_line        VARCHAR(500),
    is_deleted          TINYINT(1)   NOT NULL DEFAULT 0 COMMENT '0=alive, 1=soft-deleted',
    registered_datetime DATETIME     NOT NULL,
    created_at          DATETIME     NOT NULL,
    updated_at          DATETIME     NOT NULL,
    source              VARCHAR(20)  NOT NULL COMMENT 'offline | online_web | app_store',
    PRIMARY KEY (customer_id)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- DIMENSION: category  (product taxonomy — referenced by products.category_id)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS category (
    category_id   VARCHAR(50)  NOT NULL,
    category_name VARCHAR(100) NOT NULL,
    is_current    TINYINT(1)   NOT NULL DEFAULT 1,
    created_at    DATETIME     NOT NULL,
    updated_at    DATETIME     NOT NULL,
    PRIMARY KEY (category_id)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- DIMENSION: products  (SCD2 — multiple rows per product_id, is_current marks active)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS products (
    product_id           VARCHAR(50)    NOT NULL,
    product_name         VARCHAR(255)   NOT NULL,
    product_display_name VARCHAR(255),
    category_id          VARCHAR(50)    NOT NULL,
    sales_unit           VARCHAR(50),
    color                VARCHAR(50)    NOT NULL,
    size                 VARCHAR(20)    NOT NULL,
    unit_price           DECIMAL(15, 2) NOT NULL,
    created_at           DATETIME       NOT NULL,
    updated_at           DATETIME       NOT NULL,
    is_current           TINYINT(1)     NOT NULL DEFAULT 1 COMMENT '1 = active record',
    PRIMARY KEY (product_id, created_at)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- DIMENSION: branches  (SCD2 — retail store locations)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS branches (
    branch_id      VARCHAR(50)  NOT NULL,
    branch_name    VARCHAR(255) NOT NULL,
    branch_type    VARCHAR(20)  NOT NULL COMMENT 'flagship | mall_kiosk | outlet | standard',
    region         VARCHAR(100),
    city_province  VARCHAR(100),
    ward           VARCHAR(100),
    status         VARCHAR(20)  NOT NULL DEFAULT 'active',
    open_date      DATE         NOT NULL,
    created_at     DATETIME     NOT NULL,
    updated_at     DATETIME     NOT NULL,
    is_current     TINYINT(1)   NOT NULL DEFAULT 1,
    PRIMARY KEY (branch_id, created_at)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- FACT: pos_transactions  (POS / in-store transaction headers)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pos_transactions (
    transaction_id       VARCHAR(50)    NOT NULL,
    branch_id            VARCHAR(50)    NOT NULL,
    transaction_datetime DATETIME       NOT NULL,
    payment_type         VARCHAR(50),
    trans_total_amount   DECIMAL(15, 2) NOT NULL DEFAULT 0,
    trans_total_line     INT            NOT NULL DEFAULT 0,
    trans_total_sell_sku INT            NOT NULL DEFAULT 0,
    customer_id          VARCHAR(50),
    order_status         VARCHAR(1)     NOT NULL DEFAULT 'W' COMMENT 'W=Waiting|R=Rejected|O=Open|D=Delivering|I=Invoiced|C=Cancelled|B=Returned|P=PendingPayment|F=PaymentFailed',
    delivery_date        DATE,
    created_at           DATETIME       NOT NULL,
    updated_at           DATETIME       NOT NULL,
    PRIMARY KEY (transaction_id)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- FACT: pos_transaction_details  (POS line items — apparel variants)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pos_transaction_details (
    transaction_detail_id VARCHAR(50)    NOT NULL,
    transaction_id        VARCHAR(50)    NOT NULL,
    branch_id             VARCHAR(50)    NOT NULL,
    product_id            VARCHAR(50)    NOT NULL,
    is_promo              TINYINT(1)     NOT NULL DEFAULT 0,
    product_uom_code      VARCHAR(20),
    trans_qty             INT            NOT NULL DEFAULT 0,
    trans_line_amount     DECIMAL(15, 2) NOT NULL DEFAULT 0,
    variant_size          VARCHAR(20),
    variant_color         VARCHAR(50),
    created_at            DATETIME       NOT NULL,
    PRIMARY KEY (transaction_detail_id)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- FACT: online_transactions  (web/app transaction headers)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS online_transactions (
    transaction_id       VARCHAR(50)    NOT NULL,
    branch_id            VARCHAR(50)    NOT NULL,
    transaction_datetime DATETIME       NOT NULL,
    payment_type         VARCHAR(50),
    trans_total_amount   DECIMAL(15, 2) NOT NULL DEFAULT 0,
    customer_id          VARCHAR(50),
    order_status         VARCHAR(1)     NOT NULL DEFAULT 'W' COMMENT 'W=Waiting|R=Rejected|O=Open|D=Delivering|I=Invoiced|C=Cancelled|B=Returned|P=PendingPayment|F=PaymentFailed',
    delivery_date        DATE,
    created_at           DATETIME       NOT NULL,
    updated_at           DATETIME       NOT NULL,
    PRIMARY KEY (transaction_id)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- FACT: online_transaction_details  (web/app line items — apparel variants)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS online_transaction_details (
    transaction_detail_id VARCHAR(50)    NOT NULL,
    transaction_id        VARCHAR(50)    NOT NULL,
    product_id            VARCHAR(50)    NOT NULL,
    is_promo              TINYINT(1)     NOT NULL DEFAULT 0,
    product_uom_code      VARCHAR(20),
    trans_qty             INT            NOT NULL DEFAULT 0,
    trans_line_amount     DECIMAL(15, 2) NOT NULL DEFAULT 0,
    variant_size          VARCHAR(20),
    variant_color         VARCHAR(50),
    created_at            DATETIME       NOT NULL,
    PRIMARY KEY (transaction_detail_id)
);
