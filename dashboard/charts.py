import plotly.express as px


# ---------------------------------------------------
# Monthly Revenue Trend
# ---------------------------------------------------

def monthly_sales_chart(df):

    fig = px.line(
        df,
        x="month",
        y="total_revenue",
        markers=True,
        title="Monthly Revenue Trend"
    )

    fig.update_layout(
        template="plotly_dark",
        title_x=0.5,
        xaxis_title="Month",
        yaxis_title="Revenue"
    )

    return fig


# ---------------------------------------------------
# Category Revenue
# ---------------------------------------------------

def category_revenue_chart(df):

    df = df.sort_values("revenue", ascending=False).head(10)

    fig = px.bar(
        df,
        x="revenue",
        y="product_category_name_english",
        orientation="h",
        title="Top Categories by Revenue",
        color="revenue",
        color_continuous_scale="Blues"
    )

    fig.update_layout(
        template="plotly_dark",
        yaxis=dict(autorange="reversed"),
        title_x=0.5
    )

    return fig


# ---------------------------------------------------
# Top Products
# ---------------------------------------------------

def top_products_chart(df):

    df = df.sort_values("total_sales", ascending=False).head(10)

    fig = px.bar(
        df,
        x="total_sales",
        y="product_id",
        orientation="h",
        title="Top Selling Products",
        color="total_sales",
        color_continuous_scale="Viridis"
    )

    fig.update_layout(
        template="plotly_dark",
        yaxis=dict(autorange="reversed"),
        title_x=0.5
    )

    return fig


# ---------------------------------------------------
# Revenue by State
# ---------------------------------------------------

def state_sales_chart(df):

    df = df.sort_values("revenue", ascending=False).head(10)

    fig = px.bar(
        df,
        x="customer_state",
        y="revenue",
        title="Revenue by State",
        color="revenue",
        color_continuous_scale="Teal"
    )

    fig.update_layout(
        template="plotly_dark",
        title_x=0.5,
        xaxis_title="State",
        yaxis_title="Revenue"
    )

    return fig


# ---------------------------------------------------
# Seller Performance
# ---------------------------------------------------

def seller_performance_chart(df):

    df = df.sort_values("revenue", ascending=False).head(10)

    fig = px.bar(
        df,
        x="revenue",
        y="seller_id",
        orientation="h",
        title="Top Sellers by Revenue",
        color="revenue",
        color_continuous_scale="Greens"
    )

    fig.update_layout(
        template="plotly_dark",
        yaxis=dict(autorange="reversed"),
        title_x=0.5
    )

    return fig


# ---------------------------------------------------
# Payment Distribution
# ---------------------------------------------------

def payment_chart(df):

    fig = px.pie(
        df,
        names="payment_type",
        values="total_orders",
        title="Payment Method Distribution",
        hole=0.4
    )

    fig.update_layout(
        template="plotly_dark",
        title_x=0.5
    )

    return fig


# ---------------------------------------------------
# Delivery Performance
# ---------------------------------------------------

def delivery_chart(df):

    df = df.sort_values("avg_delivery_days", ascending=False).head(10)

    fig = px.bar(
        df,
        x="avg_delivery_days",
        y="seller_city",
        orientation="h",
        title="Average Delivery Time by Seller City",
        color="avg_delivery_days",
        color_continuous_scale="Oranges"
    )

    fig.update_layout(
        template="plotly_dark",
        yaxis=dict(autorange="reversed"),
        title_x=0.5
    )

    return fig

# ---------------------------------------------------
# Delay Distribution 
# ---------------------------------------------------

def delay_distribution_chart(df):

    if df is None or df.empty:
        return None

    # 🔥 Handle column mismatch safely
    if "total_orders" in df.columns:
        y_col = "total_orders"
    elif "count" in df.columns:
        y_col = "count"
    else:
        raise ValueError(f"Expected 'total_orders' or 'count', got {df.columns}")

    fig = px.bar(
        df,
        x="prediction",
        y=y_col,
        title="Delay Prediction Distribution",
        color=y_col,
        color_continuous_scale="Reds"
    )

    fig.update_layout(
        template="plotly_dark",
        title_x=0.5,
        xaxis_title="Delay (0 = No, 1 = Yes)",
        yaxis_title="Number of Orders"
    )

    return fig

# ---------------------------------------------------
# High-Risk Sellers
# ---------------------------------------------------

def high_risk_sellers_chart(df):

    if df is None or df.empty:
        return None

    df = df.sort_values("delay_risk", ascending=False).head(10)

    fig = px.bar(
        df,
        x="delay_risk",
        y="seller_id",
        orientation="h",
        title="Top High-Risk Sellers",
        color="delay_risk",
        color_continuous_scale="Reds"
    )

    fig.update_layout(
        template="plotly_dark",
        yaxis=dict(autorange="reversed"),
        title_x=0.5
    )

    return fig

# ---------------------------------------------------
# Category Delay Rate
# ---------------------------------------------------

def category_delay_chart(df):

    if df is None or df.empty:
        return None

    df = df.sort_values("delay_rate", ascending=False).head(10)

    fig = px.bar(
        df,
        x="delay_rate",
        y="product_category_name_english",
        orientation="h",
        title="Category Delay Rate",
        color="delay_rate",
        color_continuous_scale="Oranges"
    )

    fig.update_layout(
        template="plotly_dark",
        yaxis=dict(autorange="reversed"),
        title_x=0.5
    )

    return fig