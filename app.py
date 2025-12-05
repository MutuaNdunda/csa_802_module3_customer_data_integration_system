import streamlit as st
import psycopg2
import pandas as pd
from psycopg2.extras import RealDictCursor
import altair as alt
from dotenv import load_dotenv
import os

# -----------------------------------------------------
# Load Environment Variables
# -----------------------------------------------------
load_dotenv()
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

if not SUPABASE_DB_URL:
    st.error("SUPABASE_DB_URL missing in .env file.")
    st.stop()

# -----------------------------------------------------
# Database Query Helper
# -----------------------------------------------------
def run_query(query, params=None):
    conn = psycopg2.connect(SUPABASE_DB_URL)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            return pd.DataFrame(cur.fetchall())
    finally:
        conn.close()


# -----------------------------------------------------
# Streamlit Page Configuration
# -----------------------------------------------------
st.set_page_config(
    page_title="Customer–Product Data Integration System",
    layout="wide"
)

st.title("Customer–Product Data Integration System")

st.write("""
A demonstration of:
- Database schema design  
- Data integration from multiple sources  
- Analytical SQL queries  
- CSV and Database comparison  
- Reporting and visualization  
""")


# -----------------------------------------------------
# Tabs Layout (Icons Only in Titles)
# -----------------------------------------------------
tabs = st.tabs([
    "📘 Submission Details",
    "📚 ERD & Schema",
    "👥 Customers",
    "📦 Products (Database)",
    "📄 Products CSV",
    "🧾 Orders",
    "📑 Order Items",
    "📊 Analytics & Insights"
])


# -----------------------------------------------------
# TAB 1 — Submission Details
# -----------------------------------------------------
with tabs[0]:
    st.header("Submission Details")

    st.write("""
**University Name:** The Open University of Kenya  
**Unit:** CSA 802 — Systems and Data Integration  
**Student:** Gabriel Ndunda  
**Registration Number:** ST62/80168/2024  
            
This application demonstrates the full implementation of Mini Project 1,  
including data integration, SQL querying, schema design, and reporting.
""")


# -----------------------------------------------------
# TAB 2 — ERD & Schema Link
# -----------------------------------------------------
with tabs[1]:
    st.header("ERD & Database Schema")

    st.write("Schema hosted on DB Docs:")
    st.markdown("[Open Schema on DB Docs](https://dbdocs.io/gabrielndunda/Customer-Product-Data-Integration-System)")

    st.write("Entity Relationship Diagram:")

    erd_path = "static/erd_diagram.png"
    if os.path.exists(erd_path):
        st.image(erd_path, caption="ERD Diagram", use_column_width=True)
    else:
        st.warning("Place 'erd_diagram.png' inside the /static folder.")

    st.write("DBML Schema Used:")

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


# -----------------------------------------------------
# TAB 3 — Customers
# -----------------------------------------------------
with tabs[2]:
    st.header("Customers Table")

    limit = st.slider("Rows to display:", 10, 500, 200)

    query = f"SELECT * FROM customers LIMIT {limit};"
    st.subheader("SQL Query")
    st.code(query)

    st.dataframe(run_query(query), use_container_width=True)


# -----------------------------------------------------
# TAB 4 — Products (Database)
# -----------------------------------------------------
with tabs[3]:
    st.header("Products Table (Database)")

    col1, col2 = st.columns(2)
    with col1:
        min_price = st.number_input("Minimum Price", min_value=0.0, value=0.0)
    with col2:
        max_price = st.number_input("Maximum Price", min_value=0.0, value=50000.0)

    query = """
    SELECT * FROM products
    WHERE price BETWEEN %s AND %s
    ORDER BY price
    LIMIT 300;
    """
    st.subheader("SQL Query")
    st.code(query)

    df = run_query(query, (min_price, max_price))
    st.dataframe(df, use_container_width=True)


# -----------------------------------------------------
# TAB 5 — Products CSV (GitHub)
# -----------------------------------------------------
with tabs[4]:
    st.header("Products CSV (GitHub Source)")

    csv_url = "https://raw.githubusercontent.com/MutuaNdunda/csa_802_module3_customer_data_integration_system/refs/heads/main/products_2000.csv"
    st.write("CSV Source URL:")
    st.code(csv_url)

    df_csv = pd.read_csv(csv_url)
    st.write("CSV Preview")
    st.dataframe(df_csv.head(20))

    df_db = run_query("SELECT * FROM products LIMIT 2000;")

    st.write("Record Count Comparison")
    compare_df = pd.DataFrame({
        "Source": ["CSV File", "Database"],
        "Rows": [len(df_csv), len(df_db)]
    }).set_index("Source")

    st.bar_chart(compare_df)


# -----------------------------------------------------
# TAB 6 — Orders
# -----------------------------------------------------
with tabs[5]:
    st.header("Orders Table")

    query = "SELECT * FROM orders LIMIT 200;"
    st.subheader("SQL Query")
    st.code(query)

    st.dataframe(run_query(query), use_container_width=True)


# -----------------------------------------------------
# TAB 7 — Order Items
# -----------------------------------------------------
with tabs[6]:
    st.header("Order Items Table")

    query = "SELECT * FROM order_items LIMIT 200;"
    st.subheader("SQL Query")
    st.code(query)

    st.dataframe(run_query(query), use_container_width=True)


# -----------------------------------------------------
# TAB 8 — Analytics & Insights
# -----------------------------------------------------
with tabs[7]:
    st.header("Analytics & Insights")

    # ------------------------------  
    # Top Customers  
    # ------------------------------
    st.subheader("Top 10 Customers by Total Spending")

    q1 = """
    SELECT 
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
        SUM(o.total_amount) AS total_spent
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY customer_name
    ORDER BY total_spent DESC
    LIMIT 10;
    """

    df_cust = run_query(q1)
    st.bar_chart(df_cust.set_index("customer_name")["total_spent"])

    # ------------------------------  
    # Best-Selling Products  
    # ------------------------------
    st.subheader("Top 10 Best-Selling Products")

    q2 = """
    SELECT 
        p.product_name,
        SUM(oi.quantity) AS units_sold
    FROM products p
    JOIN order_items oi ON oi.product_id = p.product_id
    GROUP BY p.product_name
    ORDER BY units_sold DESC
    LIMIT 10;
    """

    df_prod = run_query(q2)
    st.bar_chart(df_prod.set_index("product_name")["units_sold"])

    # ------------------------------  
    # Daily Orders Trend  
    # ------------------------------
    st.subheader("Orders Per Day")

    q3 = """
    SELECT 
        order_date,
        COUNT(order_id) AS num_orders
    FROM orders
    GROUP BY order_date
    ORDER BY order_date;
    """

    df_orders = run_query(q3)
    st.line_chart(df_orders.set_index("order_date")["num_orders"])

    # ------------------------------  
    # Revenue Distribution Pie Chart  
    # ------------------------------
    st.subheader("Revenue Distribution (Top 10 Products)")

    q4 = """
    SELECT 
        p.product_name,
        SUM(oi.subtotal) AS revenue
    FROM products p
    JOIN order_items oi ON oi.product_id = p.product_id
    GROUP BY p.product_name
    ORDER BY revenue DESC
    LIMIT 10;
    """

    df_rev = run_query(q4)

    pie = alt.Chart(df_rev).mark_arc().encode(
        theta="revenue",
        color="product_name",
        tooltip=["product_name", "revenue"]
    )

    st.altair_chart(pie, use_container_width=True)
