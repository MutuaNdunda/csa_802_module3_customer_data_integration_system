    #!/usr/bin/env python3
    # -*- coding: utf-8 -*-
    """
    populate_db.py
    Generates synthetic customers, products, orders, and order_items.
    Writes products to CSV and inserts customers, products, orders, order_items into Supabase PostgreSQL.
    """

    import os
    import csv
    import random
    from datetime import datetime, timedelta
    from faker import Faker
    import psycopg2
    from psycopg2.extras import execute_values

    # Import from your provided name_generator module
    from name_generator import generate_county_based_name, county_name_mapping

    from dotenv import load_dotenv
    load_dotenv()

    # ---------- Kenyan Estates ----------
    KENYAN_ESTATES = {
        "Nairobi": ["Kileleshwa", "Karen", "Umoja", "Embakasi", "South B", "Lavington", "Rongai"],
        "Mombasa": ["Kizingo", "Nyali", "Likoni", "Shanzu", "Bamburi"],
        "Kisumu": ["Nyalenda", "Milimani", "Manyatta", "Nyamasaria"],
        "Nakuru": ["Section 58", "Kiamunyi", "Naka", "Barut"],
        "Kiambu": ["Ruiru", "Thika Road", "Kahawa Sukari", "Githurai"],
        "Machakos": ["Kalulini", "Mlolongo", "Syokimau", "Athi River"],
        "Kajiado": ["Kitengela", "Ongata Rongai", "Ngong", "Isinya"],
        "Uasin Gishu": ["Elgon View", "Kapseret", "Pioneer"],
        "Kericho": ["Kapsoit", "Ainamoi", "Kipkelion"]
    }

    DEFAULT_ESTATES = ["CBD", "Main Street", "Market Area"]

    # ---------- Kenyan Postal Codes ----------
    POSTAL_CODES = [
        "00100", "20100", "40100", "80100", "30100",
        "90100", "70100", "50100", "60100"
    ]

    def generate_postal_code():
        return random.choice(POSTAL_CODES)

    # ---------- Kenyan Phone Numbers ----------
    def generate_kenyan_phone():
        prefix = random.choice(["07", "01"])
        rest = "".join(str(random.randint(0, 9)) for _ in range(8))
        return prefix + rest

    # ---------- Config ----------
    SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
    if not SUPABASE_DB_URL:
        raise RuntimeError("Please set SUPABASE_DB_URL environment variable.")

    PRODUCTS_CSV_FILE = "products_2000.csv"
    NUM_RECORDS = 2000
    BATCH_SIZE = 500

    fake = Faker("en_US")
    random.seed(42)

    # ---------- Product generator ----------
    PRODUCT_CATALOG_SEEDS = [
        ("Electronics", ["Smart TV 43\"", "Smartphone Model X", "Bluetooth Speaker", "Laptop Slim"]),
        ("Grocery", ["Maize Flour 2kg", "Basmati Rice 5kg", "Sugar 2kg", "Milk 1L"]),
        ("Furniture", ["Dining Table", "Office Chair", "Sofa 3-seater", "Coffee Table"]),
        ("Clothing", ["Kitenge Dress", "Safari Boot", "School Uniform", "Sports Jersey"]),
        ("Beverages", ["Coca-Cola 500ml", "Mineral Water 1L", "Tusker Lager 500ml", "Juice 1L"])
    ]

    SOURCE_SYSTEMS = ["CSV", "API"]

    def generate_product(product_id: int) -> dict:
        cat, names = random.choice(PRODUCT_CATALOG_SEEDS)
        base_name = random.choice(names)
        descriptor = random.choice(["Classic", "Pro", "2024 Edition", "Limited", "Value", "Deluxe", "Mini"])
        product_name = f"{base_name} {descriptor}"
        price = round(random.uniform(50, 10000), 2)
        stock_quantity = random.randint(0, 500)
        source_system = random.choice(SOURCE_SYSTEMS)
        created_at = datetime.utcnow()

        return {
            "product_id": product_id,
            "product_name": product_name,
            "price": price,
            "stock_quantity": stock_quantity,
            "source_system": source_system,
            "created_at": created_at
        }

    # ---------- Customer generator ----------
    def generate_customer(customer_id: int) -> dict:
        county = random.choice(list(county_name_mapping.keys()))
        person = generate_county_based_name(county)

        estates = KENYAN_ESTATES.get(county, DEFAULT_ESTATES)
        estate_name = random.choice(estates)

        email_local = person["first_name"].lower() + "." + person["last_name"].lower()
        email = f"{email_local}{customer_id}@example.com"

        phone = generate_kenyan_phone()
        postal_code = generate_postal_code()

        address_line1 = f"{estate_name}, {county}"
        address_line2 = f"P.O. Box {random.randint(100, 9999)}-{postal_code}, {county}"

        return {
            "customer_id": customer_id,
            "first_name": person["first_name"],
            "last_name": person["last_name"],
            "email": email,
            "phone": phone,
            "address_line1": address_line1,
            "address_line2": address_line2,
            "city": county,
            "country": "Kenya",
            "created_at": datetime.utcnow()
        }

    # ---------- Orders / Order Items ----------
    def generate_orders_and_items(customer_ids, product_ids, num_orders, num_items):
        orders = []
        order_items = []

        start_date = datetime(2022, 1, 1)
        end_date = datetime(2025, 10, 30)
        total_days = (end_date - start_date).days + 1

        for oid in range(1, num_orders + 1):
            customer_id = random.choice(customer_ids)
            rand_day = random.randint(0, total_days - 1)
            order_date = (start_date + timedelta(days=rand_day)).date()

            orders.append({
                "order_id": oid,
                "customer_id": customer_id,
                "order_date": order_date,
                "total_amount": None
            })

        # Weighted Grocery Selection
        weights = []
        for pid in product_ids:
            if pid <= 400:  # first 400 products assumed grocery-like
                weights.append(3)
            else:
                weights.append(1)

        item_id = 1
        for order in orders:
            product_id = random.choices(product_ids, weights=weights, k=1)[0]
            quantity = random.randint(1, 5)
            unit_price = round(random.uniform(50, 10000), 2)
            subtotal = round(unit_price * quantity, 2)

            order_items.append({
                "order_item_id": item_id,
                "order_id": order["order_id"],
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "subtotal": subtotal
            })
            item_id += 1

        remaining = max(0, num_items - len(order_items))
        for _ in range(remaining):
            order = random.choice(orders)
            product_id = random.choices(product_ids, weights=weights, k=1)[0]
            quantity = random.randint(1, 5)
            unit_price = round(random.uniform(50, 10000), 2)
            subtotal = round(unit_price * quantity, 2)

            order_items.append({
                "order_item_id": item_id,
                "order_id": order["order_id"],
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "subtotal": subtotal
            })
            item_id += 1

        totals = {}
        for oi in order_items:
            totals.setdefault(oi["order_id"], 0)
            totals[oi["order_id"]] += oi["subtotal"]

        for order in orders:
            order["total_amount"] = round(totals[order["order_id"]], 2)

        return orders, order_items

    # ---------- DB Helpers ----------
    def create_tables_if_not_exist(conn):
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INT PRIMARY KEY,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            email VARCHAR(200),
            phone VARCHAR(100),
            address_line1 VARCHAR(255),
            address_line2 VARCHAR(255),
            city VARCHAR(100),
            country VARCHAR(100),
            created_at TIMESTAMP
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id INT PRIMARY KEY,
            product_name VARCHAR(300),
            price DECIMAL(10,2),
            stock_quantity INT,
            source_system VARCHAR(50),
            created_at TIMESTAMP
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id INT PRIMARY KEY,
            customer_id INT,
            order_date DATE,
            total_amount DECIMAL(12,2),
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE CASCADE
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS order_items (
            order_item_id INT PRIMARY KEY,
            order_id INT,
            product_id INT,
            quantity INT,
            unit_price DECIMAL(10,2),
            subtotal DECIMAL(12,2),
            FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE,
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        );
        """)

        conn.commit()
        cur.close()

    def clear_tables(conn):
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE order_items, orders, customers, products RESTART IDENTITY CASCADE;")
        conn.commit()
        cur.close()

    def batch_insert(conn, table_name, columns, values):
        cur = conn.cursor()
        cols = ", ".join(columns)
        template = "(" + ", ".join(["%s"] * len(columns)) + ")"
        sql = f"INSERT INTO {table_name} ({cols}) VALUES %s ON CONFLICT DO NOTHING;"
        execute_values(cur, sql, values, template=template)
        conn.commit()
        cur.close()

    # ---------- Main ----------
    def main():
        print("Generating product records...")
        products = [generate_product(pid) for pid in range(1, NUM_RECORDS + 1)]
        product_ids = [p["product_id"] for p in products]

        print("Writing products CSV...")
        with open(PRODUCTS_CSV_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["product_id", "product_name", "price", "stock_quantity", "source_system", "created_at"])
            writer.writeheader()
            for p in products:
                row = p.copy()
                row["created_at"] = row["created_at"].isoformat()
                writer.writerow(row)

        print("Generating customers...")
        customers = [generate_customer(cid) for cid in range(1, NUM_RECORDS + 1)]
        customer_ids = [c["customer_id"] for c in customers]

        print("Generating orders and order items...")
        orders, order_items = generate_orders_and_items(customer_ids, product_ids, NUM_RECORDS, NUM_RECORDS)

        print("Connecting to Supabase database...")
        conn = psycopg2.connect(SUPABASE_DB_URL)

        try:
            create_tables_if_not_exist(conn)

            print("Clearing existing data...")
            clear_tables(conn)

            print("Inserting products...")
            batch_insert(
                conn,
                "products",
                ["product_id", "product_name", "price", "stock_quantity", "source_system", "created_at"],
                [(p["product_id"], p["product_name"], p["price"], p["stock_quantity"], p["source_system"], p["created_at"]) for p in products]
            )

            print("Inserting customers...")
            batch_insert(
                conn,
                "customers",
                ["customer_id", "first_name", "last_name", "email", "phone", "address_line1", "address_line2", "city", "country", "created_at"],
                [(c["customer_id"], c["first_name"], c["last_name"], c["email"], c["phone"], c["address_line1"], c["address_line2"], c["city"], c["country"], c["created_at"]) for c in customers]
            )

            print("Inserting orders...")
            batch_insert(
                conn,
                "orders",
                ["order_id", "customer_id", "order_date", "total_amount"],
                [(o["order_id"], o["customer_id"], o["order_date"], o["total_amount"]) for o in orders]
            )

            print("Inserting order items...")
            batch_insert(
                conn,
                "order_items",
                ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "subtotal"],
                [(oi["order_item_id"], oi["order_id"], oi["product_id"], oi["quantity"], oi["unit_price"], oi["subtotal"]) for oi in order_items]
            )

            print("SUCCESS: All synthetic tables created and populated with Kenyan data.")

        finally:
            conn.close()


    if __name__ == "__main__":
        main()
