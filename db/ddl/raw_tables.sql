CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    user_phone TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS drivers (
    driver_id INT PRIMARY KEY,
    driver_phone TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS stores (
    store_id INT PRIMARY KEY,
    store_name TEXT NOT NULL,
    store_city TEXT NOT NULL,
    store_address TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS items (
    item_id INT PRIMARY KEY,
    item_title TEXT NOT NULL,
    item_category TEXT NOT NULL,
    item_price INT NOT NULL
);
CREATE TABLE IF NOT EXISTS orders (
    order_id INT PRIMARY KEY,
    user_id INT NOT NULL,
    store_id INT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    paid_at TIMESTAMP,
    delivery_started_at TIMESTAMP,
    canceled_at TIMESTAMP,
    payment_type TEXT NOT NULL,
    order_cancellation_reason TEXT,
    order_discount INT NOT NULL,
    delivery_city TEXT NOT NULL,
    delivery_address TEXT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (store_id) REFERENCES stores(store_id)
);
CREATE TABLE IF NOT EXISTS order_drivers (
    order_id INT NOT NULL,
    driver_id INT NOT NULL,
    delivered_at TIMESTAMP,
    delivery_cost INT NOT NULL,
    PRIMARY KEY (order_id, driver_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE,
    FOREIGN KEY (driver_id) REFERENCES drivers(driver_id) ON DELETE
    CASCADE
);
CREATE TABLE IF NOT EXISTS order_items (
    id SERIAL NOT NULL PRIMARY KEY,
    order_id INT NOT NULL,
    item_id INT NOT NULL,
    item_quantity INT NOT NULL,
    item_canceled_quantity INT NOT NULL,
    item_discount INT,
    item_replaced_id INT,
    UNIQUE (order_id, item_id, item_discount, item_replaced_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE,
    FOREIGN KEY (item_id) REFERENCES items(item_id) ON DELETE CASCADE,
    FOREIGN KEY (item_replaced_id) REFERENCES items(item_id) ON DELETE
    CASCADE
);