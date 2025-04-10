CREATE TABLE products (
    product_id VARCHAR PRIMARY KEY,
    product_name VARCHAR,
    category VARCHAR,
    price DECIMAL
);

CREATE TABLE stores (
    store_id VARCHAR PRIMARY KEY,
    store_name VARCHAR,
    city VARCHAR,
    state VARCHAR
);

CREATE TABLE sales (
    sale_id VARCHAR PRIMARY KEY,
    product_id VARCHAR REFERENCES products(product_id),
    store_id VARCHAR REFERENCES stores(store_id),
    sale_date DATE,
    quantity INT
);
