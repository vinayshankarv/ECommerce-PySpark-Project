import streamlit as st
import charts
from utils import load_tables


# -------------------------------------------------
# Page Config
# -------------------------------------------------

st.set_page_config(
    page_title="Olist Ecommerce Analytics",
    page_icon="🛒",
    layout="wide"
)

st.title("🛒 Olist Ecommerce Analytics Dashboard")
st.caption("Big Data Analytics Pipeline with Predictive Insights (PySpark + Hive + ML)")


# -------------------------------------------------
# Load Tables
# -------------------------------------------------

try:
    (
        sales_summary,
        category_revenue,
        top_products,
        seller_performance,
        delivery_performance,
        monthly_sales,
        state_sales,
        payment_analysis,
        delay_summary,
        high_risk_sellers,
        category_delay
    ) = load_tables()

except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()


# -------------------------------------------------
# KPI Cards
# -------------------------------------------------

st.subheader("📊 Business Overview")

try:
    total_orders = int(sales_summary.iloc[0]["total_orders"])
    total_revenue = round(sales_summary.iloc[0]["total_revenue"], 2)
    avg_order_value = round(sales_summary.iloc[0]["avg_order_value"], 2)
    avg_rating = round(sales_summary.iloc[0]["avg_rating"], 2)

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("📦 Total Orders", f"{total_orders:,}")
    col2.metric("💰 Total Revenue", f"${total_revenue:,}")
    col3.metric("🛒 Avg Order Value", f"${avg_order_value}")
    col4.metric("⭐ Avg Rating", avg_rating)

except Exception as e:
    st.warning("KPI data not available")

st.divider()


# -------------------------------------------------
# Revenue Insights
# -------------------------------------------------

st.subheader("📈 Revenue Insights")

col1, col2 = st.columns(2)

with col1:
    try:
        st.plotly_chart(
            charts.monthly_sales_chart(monthly_sales),
            use_container_width=True
        )
    except:
        st.warning("Monthly sales data not available")

with col2:
    try:
        st.plotly_chart(
            charts.category_revenue_chart(category_revenue),
            use_container_width=True
        )
    except:
        st.warning("Category revenue data not available")


# -------------------------------------------------
# Product Analytics
# -------------------------------------------------

st.subheader("📦 Product Analytics")

col1, col2 = st.columns(2)

with col1:
    try:
        st.plotly_chart(
            charts.top_products_chart(top_products),
            use_container_width=True
        )
    except:
        st.warning("Top products data not available")

with col2:
    try:
        st.plotly_chart(
            charts.state_sales_chart(state_sales),
            use_container_width=True
        )
    except:
        st.warning("State sales data not available")


# -------------------------------------------------
# Payments & Sellers
# -------------------------------------------------

st.subheader("💳 Payments & Sellers")

col1, col2 = st.columns(2)

with col1:
    try:
        st.plotly_chart(
            charts.payment_chart(payment_analysis),
            use_container_width=True
        )
    except:
        st.warning("Payment data not available")

with col2:
    try:
        st.plotly_chart(
            charts.seller_performance_chart(seller_performance),
            use_container_width=True
        )
    except:
        st.warning("Seller performance data not available")


# -------------------------------------------------
# Logistics
# -------------------------------------------------

st.subheader("🚚 Logistics Performance")

try:
    st.plotly_chart(
        charts.delivery_chart(delivery_performance),
        use_container_width=True
    )
except:
    st.warning("Delivery performance data not available")

st.divider()


# =================================================
# 🔥 Predictive Insights (ML)
# =================================================

st.subheader("🤖 Predictive Insights (Machine Learning)")

# Debug (optional – remove later)
# st.write("Delay Summary Columns:", delay_summary.columns)

col1, col2 = st.columns(2)

# Delay Distribution
with col1:
    try:
        st.plotly_chart(
            charts.delay_distribution_chart(delay_summary),
            use_container_width=True
        )
    except Exception as e:
        st.warning(f"Delay distribution error: {e}")

# High-Risk Sellers
with col2:
    try:
        st.plotly_chart(
            charts.high_risk_sellers_chart(high_risk_sellers),
            use_container_width=True
        )
    except Exception as e:
        st.warning(f"High-risk sellers error: {e}")

# Category Delay
try:
    st.plotly_chart(
        charts.category_delay_chart(category_delay),
        use_container_width=True
    )
except Exception as e:
    st.warning(f"Category delay error: {e}")


# -------------------------------------------------
# Footer
# -------------------------------------------------

st.divider()

st.caption("Powered by PySpark • Hive • Machine Learning • Streamlit • Plotly")