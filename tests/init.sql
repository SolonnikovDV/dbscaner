-- Создание схемы для тестовых объектов
CREATE SCHEMA IF NOT EXISTS test_graph;

-- Создание пользовательских типов
CREATE TYPE test_graph.user_status AS ENUM ('active', 'inactive', 'blocked');
CREATE TYPE test_graph.address_type AS (
    street VARCHAR(100),
    city VARCHAR(50),
    postal_code VARCHAR(10)
);

-- Создание последовательностей
CREATE SEQUENCE test_graph.user_id_seq;
CREATE SEQUENCE test_graph.order_id_seq;

-- Создание базовых таблиц
CREATE TABLE test_graph.users (
    user_id BIGINT DEFAULT nextval('test_graph.user_id_seq'),
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    status test_graph.user_status DEFAULT 'active',
    address test_graph.address_type,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id)
);

CREATE TABLE test_graph.products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    stock_quantity INTEGER NOT NULL
);

CREATE TABLE test_graph.orders (
    order_id BIGINT DEFAULT nextval('test_graph.order_id_seq'),
    user_id BIGINT REFERENCES test_graph.users(user_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10,2),
    PRIMARY KEY (order_id)
);

CREATE TABLE test_graph.order_items (
    order_id BIGINT REFERENCES test_graph.orders(order_id),
    product_id INTEGER REFERENCES test_graph.products(product_id),
    quantity INTEGER NOT NULL,
    price_per_unit DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (order_id, product_id)
);

-- Создание представлений
CREATE VIEW test_graph.active_users AS
SELECT user_id, username, email, status
FROM test_graph.users
WHERE status = 'active';

CREATE VIEW test_graph.order_summary AS
SELECT 
    o.order_id,
    u.username,
    COUNT(oi.product_id) as total_items,
    o.total_amount
FROM test_graph.orders o
JOIN test_graph.users u ON o.user_id = u.user_id
JOIN test_graph.order_items oi ON o.order_id = oi.order_id
GROUP BY o.order_id, u.username, o.total_amount;

-- Создание функций
CREATE OR REPLACE FUNCTION test_graph.calculate_order_total(p_order_id BIGINT)
RETURNS DECIMAL(10,2) AS $$
DECLARE
    v_total DECIMAL(10,2);
BEGIN
    SELECT SUM(quantity * price_per_unit)
    INTO v_total
    FROM test_graph.order_items
    WHERE order_id = p_order_id;
    
    RETURN v_total;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_graph.update_stock_quantity()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE test_graph.products
    SET stock_quantity = stock_quantity - NEW.quantity
    WHERE product_id = NEW.product_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Создание триггера
CREATE TRIGGER trg_update_stock
AFTER INSERT ON test_graph.order_items
FOR EACH ROW
EXECUTE FUNCTION test_graph.update_stock_quantity();

-- Создание материализованного представления
CREATE MATERIALIZED VIEW test_graph.monthly_sales AS
SELECT 
    DATE_TRUNC('month', o.order_date) as month,
    p.product_id,
    p.name,
    SUM(oi.quantity) as total_quantity,
    SUM(oi.quantity * oi.price_per_unit) as total_sales
FROM test_graph.orders o
JOIN test_graph.order_items oi ON o.order_id = oi.order_id
JOIN test_graph.products p ON oi.product_id = p.product_id
GROUP BY DATE_TRUNC('month', o.order_date), p.product_id, p.name;

-- Создание индексов
CREATE INDEX idx_users_email ON test_graph.users(email);
CREATE INDEX idx_orders_user_id ON test_graph.orders(user_id);
CREATE INDEX idx_order_items_product_id ON test_graph.order_items(product_id);

-- Добавление тестовых данных
INSERT INTO test_graph.users (username, email, status) VALUES
('john_doe', 'john@example.com', 'active'),
('jane_smith', 'jane@example.com', 'active'),
('bob_wilson', 'bob@example.com', 'inactive');

INSERT INTO test_graph.products (name, price, stock_quantity) VALUES
('Laptop', 999.99, 50),
('Smartphone', 499.99, 100),
('Headphones', 79.99, 200);

-- Создание хранимой процедуры
CREATE OR REPLACE PROCEDURE test_graph.create_order(
    p_user_id BIGINT,
    p_product_id INTEGER,
    p_quantity INTEGER
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_order_id BIGINT;
    v_price DECIMAL(10,2);
BEGIN
    -- Получаем цену продукта
    SELECT price INTO v_price
    FROM test_graph.products
    WHERE product_id = p_product_id;
    
    -- Создаем заказ
    INSERT INTO test_graph.orders (user_id, total_amount)
    VALUES (p_user_id, v_price * p_quantity)
    RETURNING order_id INTO v_order_id;
    
    -- Добавляем позицию заказа
    INSERT INTO test_graph.order_items (order_id, product_id, quantity, price_per_unit)
    VALUES (v_order_id, p_product_id, p_quantity, v_price);
    
    -- Вызываем функцию обновления total_amount
    UPDATE test_graph.orders
    SET total_amount = test_graph.calculate_order_total(v_order_id)
    WHERE order_id = v_order_id;
END;
$$;