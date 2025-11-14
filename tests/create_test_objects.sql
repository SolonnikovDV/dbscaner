-- Создание схемы для тестовых объектов
CREATE SCHEMA IF NOT EXISTS test_objects;

-- Пользовательский тип данных
CREATE TYPE test_objects.user_status AS ENUM ('active', 'inactive', 'blocked');

CREATE TYPE test_objects.address AS (
    street VARCHAR(100),
    city VARCHAR(50),
    postal_code VARCHAR(10)
);

-- Последовательности
CREATE SEQUENCE test_objects.user_id_seq
    INCREMENT BY 1
    START WITH 1;

CREATE SEQUENCE test_objects.order_id_seq
    INCREMENT BY 1
    START WITH 1000;

-- Таблицы
CREATE TABLE test_objects.users (
    id BIGINT DEFAULT nextval('test_objects.user_id_seq'),
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE,
    status test_objects.user_status DEFAULT 'active',
    address test_objects.address,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

CREATE TABLE test_objects.products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    description TEXT
);

CREATE TABLE test_objects.orders (
    id BIGINT DEFAULT nextval('test_objects.order_id_seq'),
    user_id BIGINT REFERENCES test_objects.users(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(12,2),
    PRIMARY KEY (id)
);

CREATE TABLE test_objects.order_items (
    order_id BIGINT REFERENCES test_objects.orders(id),
    product_id INTEGER REFERENCES test_objects.products(id),
    quantity INTEGER NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (order_id, product_id)
);

-- Представления
CREATE VIEW test_objects.active_users AS
SELECT id, username, email, created_at
FROM test_objects.users
WHERE status = 'active';

CREATE MATERIALIZED VIEW test_objects.order_summary AS
SELECT 
    o.id as order_id,
    u.username,
    COUNT(oi.product_id) as items_count,
    SUM(oi.quantity * oi.price) as total_amount
FROM test_objects.orders o
JOIN test_objects.users u ON o.user_id = u.id
JOIN test_objects.order_items oi ON o.id = oi.order_id
GROUP BY o.id, u.username;

-- Функции
CREATE OR REPLACE FUNCTION test_objects.calculate_order_total(p_order_id BIGINT)
RETURNS DECIMAL AS $$
DECLARE
    v_total DECIMAL(12,2);
BEGIN
    SELECT SUM(quantity * price)
    INTO v_total
    FROM test_objects.order_items
    WHERE order_id = p_order_id;
    
    UPDATE test_objects.orders
    SET total_amount = v_total
    WHERE id = p_order_id;
    
    RETURN v_total;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_objects.get_user_orders(p_user_id BIGINT)
RETURNS TABLE (
    order_id BIGINT,
    order_date TIMESTAMP,
    total_amount DECIMAL,
    items_count BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        o.id,
        o.created_at,
        o.total_amount,
        COUNT(oi.product_id)
    FROM test_objects.orders o
    LEFT JOIN test_objects.order_items oi ON o.id = oi.order_id
    WHERE o.user_id = p_user_id
    GROUP BY o.id, o.created_at, o.total_amount;
END;
$$ LANGUAGE plpgsql;