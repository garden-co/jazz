CREATE TABLE cart_items (
    owner_id TEXT NOT NULL,
    product UUID REFERENCES products NOT NULL,
    quantity INTEGER NOT NULL
);
CREATE POLICY cart_items_select_policy ON cart_items FOR SELECT USING (owner_id = @session.user_id);
CREATE POLICY cart_items_insert_policy ON cart_items FOR INSERT WITH CHECK (owner_id = @session.user_id);
CREATE POLICY cart_items_update_policy ON cart_items FOR UPDATE USING (owner_id = @session.user_id) WITH CHECK (owner_id = @session.user_id);
CREATE POLICY cart_items_delete_policy ON cart_items FOR DELETE USING (owner_id = @session.user_id);

CREATE TABLE order_items (
    order UUID REFERENCES orders NOT NULL,
    product UUID REFERENCES products NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price_cents INTEGER NOT NULL
);
CREATE POLICY order_items_select_policy ON order_items FOR SELECT USING (TRUE);

CREATE TABLE orders (
    owner_id TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    total_cents INTEGER NOT NULL,
    item_count INTEGER NOT NULL
);
CREATE POLICY orders_select_policy ON orders FOR SELECT USING (owner_id = @session.user_id);

CREATE TABLE products (
    name TEXT NOT NULL,
    brand TEXT NOT NULL,
    category TEXT NOT NULL,
    description TEXT NOT NULL,
    image_url TEXT NOT NULL,
    price_cents INTEGER NOT NULL,
    rating REAL NOT NULL,
    in_stock INTEGER NOT NULL
);
CREATE POLICY products_select_policy ON products FOR SELECT USING (TRUE);