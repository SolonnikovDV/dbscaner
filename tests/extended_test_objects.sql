-- Расширенная схема тестовых объектов с 4 уровнями зависимостей вверх и вниз
-- для тестирования логики графа зависимостей (Greenplum compatible - без FK, PK, SERIAL)

DROP SCHEMA IF EXISTS test_dependencies CASCADE;
CREATE SCHEMA test_dependencies;

-- === УРОВЕНЬ 4 ВНИЗ (базовые объекты) ===
-- Пользовательские типы
CREATE TYPE test_dependencies.base_address AS (
    street VARCHAR(100),
    city VARCHAR(50),
    postal_code VARCHAR(10)
);

CREATE TYPE test_dependencies.contact_info AS (
    phone VARCHAR(20),
    email VARCHAR(100),
    website VARCHAR(100)
);

CREATE TYPE test_dependencies.product_category AS ENUM ('electronics', 'books', 'clothing', 'home', 'sports');

-- Базовые таблицы (без первичных ключей)
CREATE TABLE test_dependencies.base_entities (
    id BIGINT,
    name VARCHAR(100),
    description TEXT,
    address test_dependencies.base_address,
    contact test_dependencies.contact_info,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE test_dependencies.categories (
    id BIGINT,
    name VARCHAR(50),
    category_type test_dependencies.product_category DEFAULT 'electronics',
    parent_id BIGINT
);

-- === УРОВЕНЬ 3 ВНИЗ ===
-- Таблицы, зависящие от базовых (упоминаются в коде вышестоящих объектов)
CREATE TABLE test_dependencies.products (
    id BIGINT,
    name VARCHAR(100),
    category_id BIGINT,
    base_entity_id BIGINT,
    price DECIMAL(10,2),
    stock_quantity INTEGER DEFAULT 0,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE test_dependencies.suppliers (
    id BIGINT,
    name VARCHAR(100),
    base_entity_id BIGINT,
    rating DECIMAL(3,2),
    is_active BOOLEAN DEFAULT true
);

-- === УРОВЕНЬ 2 ВНИЗ ===
-- Таблицы, зависящие от уровня 3
CREATE TABLE test_dependencies.supplier_products (
    supplier_id BIGINT,
    product_id BIGINT,
    supply_price DECIMAL(10,2),
    min_quantity INTEGER
);

CREATE TABLE test_dependencies.customers (
    id BIGINT,
    base_entity_id BIGINT,
    loyalty_points INTEGER DEFAULT 0,
    preferred_category test_dependencies.product_category,
    registration_date DATE DEFAULT CURRENT_DATE
);

-- === УРОВЕНЬ 1 ВНИЗ ===
-- Таблицы, зависящие от уровня 2
CREATE TABLE test_dependencies.orders (
    id BIGINT,
    customer_id BIGINT,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(12,2),
    status VARCHAR(20) DEFAULT 'pending',
    shipping_address test_dependencies.base_address
);

CREATE TABLE test_dependencies.order_items (
    order_id BIGINT,
    product_id BIGINT,
    supplier_id BIGINT,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    discount DECIMAL(5,2) DEFAULT 0
);

-- === ФУНКЦИИ ===
-- Функции уровня 1 (используют уровень 1 таблицы)
CREATE OR REPLACE FUNCTION test_dependencies.calculate_order_total(p_order_id BIGINT)
RETURNS DECIMAL(12,2) AS $$
DECLARE
    v_total DECIMAL(12,2) := 0;
BEGIN
    SELECT SUM((quantity * unit_price) * (1 - discount/100))
    INTO v_total
    FROM test_dependencies.order_items
    WHERE order_id = p_order_id;

    UPDATE test_dependencies.orders
    SET total_amount = v_total
    WHERE id = p_order_id;

    RETURN v_total;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_dependencies.get_customer_orders(p_customer_id BIGINT)
RETURNS TABLE (
    order_id BIGINT,
    order_date TIMESTAMP,
    total_amount DECIMAL,
    item_count BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        o.id,
        o.order_date,
        o.total_amount,
        COUNT(oi.product_id)
    FROM test_dependencies.orders o
    LEFT JOIN test_dependencies.order_items oi ON o.id = oi.order_id
    WHERE o.customer_id = p_customer_id
    GROUP BY o.id, o.order_date, o.total_amount;
END;
$$ LANGUAGE plpgsql;

-- === ФУНКЦИИ УРОВЕНЯ 2 (используют функции уровня 1) ===
CREATE OR REPLACE FUNCTION test_dependencies.get_customer_total_spent(p_customer_id BIGINT)
RETURNS DECIMAL(12,2) AS $$
DECLARE
    v_total DECIMAL(12,2) := 0;
BEGIN
    SELECT SUM(total_amount)
    INTO v_total
    FROM test_dependencies.get_customer_orders(p_customer_id);

    RETURN COALESCE(v_total, 0);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_dependencies.update_order_status(p_order_id BIGINT, p_status VARCHAR)
RETURNS VOID AS $$
BEGIN
    UPDATE test_dependencies.orders
    SET status = p_status
    WHERE id = p_order_id;

    -- Пересчитываем итоговую сумму если статус изменился на 'completed'
    IF p_status = 'completed' THEN
        PERFORM test_dependencies.calculate_order_total(p_order_id);
    END IF;
END;
$$ LANGUAGE plpgsql;

-- === ПРЕДСТАВЛЕНИЯ ===
-- Представления уровня 1 (используют таблицы уровня 1)
CREATE VIEW test_dependencies.active_orders AS
SELECT
    o.id,
    c.id as customer_id,
    be.name as customer_name,
    o.order_date,
    o.total_amount,
    o.status,
    COUNT(oi.product_id) as item_count
FROM test_dependencies.orders o
JOIN test_dependencies.customers c ON o.customer_id = c.id
JOIN test_dependencies.base_entities be ON c.base_entity_id = be.id
LEFT JOIN test_dependencies.order_items oi ON o.id = oi.order_id
WHERE o.status IN ('pending', 'processing', 'shipped')
GROUP BY o.id, c.id, be.name, o.order_date, o.total_amount, o.status;

-- Представления уровня 2 (используют представления уровня 1)
CREATE VIEW test_dependencies.customer_order_summary AS
SELECT
    customer_id,
    customer_name,
    COUNT(*) as total_orders,
    SUM(total_amount) as total_spent,
    AVG(total_amount) as avg_order_value,
    MAX(order_date) as last_order_date
FROM test_dependencies.active_orders
GROUP BY customer_id, customer_name;

-- === ФУНКЦИИ УРОВЕНЯ 3 (используют представления) ===
CREATE OR REPLACE FUNCTION test_dependencies.get_top_customers(limit_count INTEGER DEFAULT 10)
RETURNS TABLE (
    customer_id BIGINT,
    customer_name VARCHAR,
    total_orders BIGINT,
    total_spent DECIMAL
) AS $$
BEGIN
    RETURN QUERY
    SELECT cos.customer_id, cos.customer_name, cos.total_orders, cos.total_spent
    FROM test_dependencies.customer_order_summary cos
    ORDER BY cos.total_spent DESC
    LIMIT limit_count;
END;
$$ LANGUAGE plpgsql;

-- === МАТЕРИАЛИЗОВАННЫЕ ПРЕДСТАВЛЕНИЯ ===
-- Материализованное представление уровня 3
CREATE MATERIALIZED VIEW test_dependencies.product_sales_stats AS
SELECT
    p.id as product_id,
    p.name as product_name,
    cat.name as category_name,
    COUNT(oi.order_id) as total_orders,
    SUM(oi.quantity) as total_quantity_sold,
    SUM(oi.quantity * oi.unit_price) as total_revenue,
    AVG(oi.unit_price) as avg_price
FROM test_dependencies.products p
JOIN test_dependencies.categories cat ON p.category_id = cat.id
LEFT JOIN test_dependencies.order_items oi ON p.id = oi.product_id
GROUP BY p.id, p.name, cat.name;

-- === ФУНКЦИИ УРОВЕНЯ 4 (используют материализованные представления) ===
CREATE OR REPLACE FUNCTION test_dependencies.get_best_selling_products(limit_count INTEGER DEFAULT 5)
RETURNS TABLE (
    product_id BIGINT,
    product_name VARCHAR,
    category_name VARCHAR,
    total_quantity_sold BIGINT,
    total_revenue DECIMAL
) AS $$
BEGIN
    RETURN QUERY
    SELECT pss.product_id, pss.product_name, pss.category_name,
           pss.total_quantity_sold, pss.total_revenue
    FROM test_dependencies.product_sales_stats pss
    ORDER BY pss.total_quantity_sold DESC
    LIMIT limit_count;
END;
$$ LANGUAGE plpgsql;

-- === ТРИГГЕРЫ ===
CREATE OR REPLACE FUNCTION test_dependencies.update_product_stock()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE test_dependencies.products
        SET stock_quantity = stock_quantity - NEW.quantity
        WHERE id = NEW.product_id;
    ELSIF TG_OP = 'UPDATE' THEN
        UPDATE test_dependencies.products
        SET stock_quantity = stock_quantity + OLD.quantity - NEW.quantity
        WHERE id = NEW.product_id;
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE test_dependencies.products
        SET stock_quantity = stock_quantity + OLD.quantity
        WHERE id = OLD.product_id;
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_product_stock
    AFTER INSERT OR UPDATE OR DELETE ON test_dependencies.order_items
    FOR EACH ROW EXECUTE FUNCTION test_dependencies.update_product_stock();

-- === ПРОЦЕДУРЫ ===
CREATE OR REPLACE PROCEDURE test_dependencies.create_order(
    p_customer_id BIGINT,
    p_product_id BIGINT,
    p_supplier_id BIGINT,
    p_quantity INTEGER,
    p_unit_price DECIMAL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_order_id BIGINT;
BEGIN
    -- Создаем заказ
    INSERT INTO test_dependencies.orders (customer_id, status)
    VALUES (p_customer_id, 'pending')
    RETURNING id INTO v_order_id;

    -- Добавляем позицию заказа
    INSERT INTO test_dependencies.order_items (order_id, product_id, supplier_id, quantity, unit_price)
    VALUES (v_order_id, p_product_id, p_supplier_id, p_quantity, p_unit_price);

    -- Пересчитываем сумму
    PERFORM test_dependencies.calculate_order_total(v_order_id);
END;
$$;

-- === ПОСЛЕДОВАТЕЛЬНОСТИ ===
CREATE SEQUENCE test_dependencies.global_id_seq;
CREATE SEQUENCE test_dependencies.product_id_seq;
CREATE SEQUENCE test_dependencies.order_id_seq;

-- === ТЕСТОВЫЕ ДАННЫЕ ===
-- Базовые сущности
INSERT INTO test_dependencies.base_entities (name, description, address, contact) VALUES
('TechCorp Ltd', 'Electronics manufacturer', ('123 Tech St', 'Tech City', '12345'), ('+1-555-0101', 'info@techcorp.com', 'www.techcorp.com')),
('BookWorld Inc', 'Book distributor', ('456 Book Ave', 'Read City', '67890'), ('+1-555-0202', 'sales@bookworld.com', 'www.bookworld.com')),
('FashionHub', 'Clothing retailer', ('789 Style Blvd', 'Fashion Town', '11223'), ('+1-555-0303', 'contact@fashionhub.com', 'www.fashionhub.com'));

-- Категории (сначала родительские, потом дочерние)
INSERT INTO test_dependencies.categories (name, category_type, parent_id) VALUES
('Electronics', 'electronics', NULL),
('Books', 'books', NULL),
('Clothing', 'clothing', NULL);

INSERT INTO test_dependencies.categories (name, category_type, parent_id) VALUES
('Computers', 'electronics', 1),
('Smartphones', 'electronics', 1),
('Fiction', 'books', 2),
('Non-fiction', 'books', 2);

-- Продукты
INSERT INTO test_dependencies.products (name, category_id, base_entity_id, price, stock_quantity, description) VALUES
('Laptop Pro', 4, 1, 1299.99, 50, 'High-performance laptop'),
('Smartphone X', 5, 1, 899.99, 100, 'Latest smartphone model'),
('Programming Book', 6, 2, 49.99, 200, 'Learn programming'),
('Fashion Jacket', 3, 3, 199.99, 75, 'Stylish winter jacket');

-- Поставщики
INSERT INTO test_dependencies.suppliers (name, base_entity_id, rating, is_active) VALUES
('Global Electronics', 1, 4.8, true),
('Book Distributors Inc', 2, 4.5, true);

-- Поставщики продуктов
INSERT INTO test_dependencies.supplier_products (supplier_id, product_id, supply_price, min_quantity) VALUES
(1, 1, 1100.00, 10),
(1, 2, 750.00, 20),
(2, 3, 35.00, 50);

-- Клиенты
INSERT INTO test_dependencies.customers (base_entity_id, loyalty_points, preferred_category, registration_date) VALUES
(1, 1500, 'electronics', '2023-01-15'),
(2, 800, 'books', '2023-03-20'),
(3, 2000, 'clothing', '2022-11-10');

-- Заказы и позиции заказов
INSERT INTO test_dependencies.orders (customer_id, order_date, status, shipping_address) VALUES
(1, '2024-01-15 10:00:00', 'completed', ('123 Main St', 'Anytown', '12345')),
(2, '2024-01-16 14:30:00', 'processing', ('456 Oak Ave', 'Somewhere', '67890')),
(3, '2024-01-17 09:15:00', 'shipped', ('789 Pine Rd', 'Elsewhere', '11223'));

INSERT INTO test_dependencies.order_items (order_id, product_id, supplier_id, quantity, unit_price, discount) VALUES
(1, 1, 1, 1, 1299.99, 5.0),
(1, 2, 1, 2, 899.99, 0.0),
(2, 3, 2, 3, 49.99, 10.0),
(3, 4, NULL, 1, 199.99, 0.0);

-- Обновляем итоговые суммы заказов
SELECT test_dependencies.calculate_order_total(1);
SELECT test_dependencies.calculate_order_total(2);
SELECT test_dependencies.calculate_order_total(3);

-- Обновляем материализованное представление
REFRESH MATERIALIZED VIEW test_dependencies.product_sales_stats;