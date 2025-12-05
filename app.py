import streamlit as st
import psycopg2
import pandas as pd
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

# ---------------------------------------------
# DB Connection Helper
# ---------------------------------------------
def run_query(query, params=None):
    conn = psycopg2.connect(SUPABASE_DB_URL)
    df = None
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            df = pd.DataFrame(cur.fetchall())
    finally:
        conn.close()
    return df


# Streamlit App
st.title("Customer–Product Data Integration System Demo")

st.write("""
This Streamlit app demonstrates the complete workflow for CSA 802:
- Database schema design  
- Integration of multiple datasets  
- SQL queries for insights  
- CSV vs Database integration  
""")

# ======================================================
# TAB LAYOUT
# ======================================================
tabs = st.tabs([
    "📚 ERD & Schema Link",
    "📁 Customers",
    "📦 Products (DB)",
    "📄 Products CSV",
    "🧾 Orders",
    "📑 Order Items",
    "🔍 Integrated Queries"
])

# ======================================================
# TAB 1 — ERD & Schema Link
# ======================================================
with tabs[0]:
    st.header("ERD & Schema Documentation")
    st.write("View the complete database schema on DB Docs:")

    st.markdown(
        "[➡️ Click here to view Schema (DBDocs)](https://dbdocs.io/gabrielndunda/Customer-Product-Data-Integration-System)"
    )

    st.write("Below is the DBML representation used to generate the ERD:")

    dbml = """
    Table customers {
      customer_id int [pk]
      first_name varchar
      last_name varchar
      email varchar
      phone varchar
      address_line1 varchar
      address_line2 varchar
      city varchar
      country varchar
      created_at timestamp
    }

    Table products {
      product_id int [pk]
      product_name varchar
      price decimal
      stock_quantity int
      source_system varchar
      created_at timestamp
    }

    Table orders {
      order_id int [pk]
      customer_id int [ref: > customers.customer_id]
      order_date date
      total_amount decimal
    }

    Table order_items {
      order_item_id int [pk]
      order_id int [ref: > orders.order_id]
      product_id int [ref: > products.product_id]
      quantity int
      unit_price decimal
      subtotal decimal
    }
    """
    st.code(dbml, language="text")

# ======================================================
# TAB 2 — Customers
# ======================================================
with tabs[1]:
    st.header("Customers Table")

    query = "SELECT * FROM customers LIMIT 200;"
    st.subheader("SQL Query:")
    st.code(query)

    df = run_query(query)
    st.dataframe(df)

# ======================================================
# TAB 3 — Products in DB
# ======================================================
with tabs[2]:
    st.header("Products Table (Database)")
    
    min_price = st.number_input("Min Price", min_value=0.0, value=0.0)
    max_price = st.number_input("Max Price", min_value=0.0, value=50000.0)

    query = """
    SELECT * FROM products
    WHERE price BETWEEN %s AND %s
    LIMIT 200;
    """

    st.subheader("SQL Query:")
    st.code(query)

    df = run_query(query, (min_price, max_price))
    st.dataframe(df)

# ======================================================
# TAB 4 — Products CSV Comparison
# ======================================================
with tabs[3]:
    st.header("Products CSV Upload + Compare")

    csv_file = st.file_uploader("Upload products_2000.csv", type=["csv"])

    if csv_file:
        df_csv = pd.read_csv(csv_file)
        st.write("### CSV Preview:")
        st.dataframe(df_csv.head(20))

        db_query = "SELECT * FROM products LIMIT 2000;"
        df_db = run_query(db_query)

        st.write("### Compare counts")
        st.write(f"CSV rows: {len(df_csv)}")
        st.write(f"DB rows: {len(df_db)}")

# ======================================================
# TAB 5 — Orders
# ======================================================
with tabs[4]:
    st.header("Orders Table")
    
    query = "SELECT * FROM orders LIMIT 200;"
    st.subheader("SQL Query:")
    st.code(query)

    df = run_query(query)
    st.dataframe(df)

# ======================================================
# TAB 6 — Order Items
# ======================================================
with tabs[5]:
    st.header("Order Items Table")

    query = "SELECT * FROM order_items LIMIT 200;"
    st.subheader("SQL Query:")
    st.code(query)

    df = run_query(query)
    st.dataframe(df)

# ======================================================
# TAB 7 — Integrated Queries
# ======================================================
with tabs[6]:
    st.header("Integrated Database Queries")

    st.subheader("1. Customer Orders with Product Details")

    q1 = """
    SELECT 
        o.order_id,
        c.first_name,
        c.last_name,
        p.product_name,
        oi.quantity,
        oi.unit_price,
        oi.subtotal
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON p.product_id = oi.product_id
    LIMIT 50;
    """
    st.code(q1)
    st.dataframe(run_query(q1))

    st.subheader("2. Total Value of Orders Per Customer")

    q2 = """
    SELECT 
        c.customer_id,
        c.first_name,
        c.last_name,
        SUM(o.total_amount) AS total_spent
    FROM customers c
    JOIN orders o ON o.customer_id = c.customer_id
    GROUP BY c.customer_id
    ORDER BY total_spent DESC
    LIMIT 50;
    """
    st.code(q2)
    st.dataframe(run_query(q2))

    st.subheader("3. Products Ordered vs Products in CSV")

    q3 = """
    SELECT 
        p.product_id,
        p.product_name,
        SUM(oi.quantity) AS units_sold
    FROM products p
    JOIN order_items oi ON oi.product_id = p.product_id
    GROUP BY p.product_id, p.product_name
    ORDER BY units_sold DESC
    LIMIT 50;
    """
    st.code(q3)
    st.dataframe(run_query(q3))
