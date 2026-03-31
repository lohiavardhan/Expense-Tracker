import json
import os

import boto3
import pandas as pd
import streamlit as st
from botocore.exceptions import ClientError

S3_BUCKET = os.environ.get("S3_BUCKET", "expense-tracker-vardhan")
AWS_REGION = "ap-southeast-1"

st.set_page_config(page_title="Expense Tracker", layout="wide")
st.title("Expense Tracker Dashboard")


@st.cache_data(ttl=300)
def load_dashboard_data():
    s3 = boto3.client("s3", region_name=AWS_REGION)
    response = s3.get_object(Bucket=S3_BUCKET, Key="dashboard/latest.json")
    return json.loads(response["Body"].read())


try:
    data = load_dashboard_data()
except ClientError as e:
    if e.response["Error"]["Code"] == "NoSuchKey":
        st.warning("No dashboard data yet.")
    else:
        st.error(f"Could not load dashboard data from S3: {e}")
    st.stop()
except Exception as e:
    st.error(f"Could not load dashboard data from S3: {e}")
    st.stop()

st.caption(f"Last updated: {data['generated_at']}")

df_daily = pd.DataFrame(data.get("daily_spend", []))
df_monthly = pd.DataFrame(data.get("monthly_spend", []))
df_type = pd.DataFrame(data.get("spend_by_type", []))
df_merchants = pd.DataFrame(data.get("top_merchants", []))

today_str = pd.Timestamp.now().strftime("%Y-%m-%d")
current_month = pd.Timestamp.now().strftime("%Y-%m")

daily_spend_value = 0.0
if not df_daily.empty and {"date", "total"}.issubset(df_daily.columns):
    df_daily["date"] = pd.to_datetime(df_daily["date"], errors="coerce")
    match = df_daily.loc[df_daily["date"].dt.strftime("%Y-%m-%d") == today_str, "total"]
    if not match.empty:
        daily_spend_value = float(match.iloc[0])

monthly_spend_value = 0.0
transactions_this_month = 0
if not df_monthly.empty and {"month", "total"}.issubset(df_monthly.columns):
    month_total = df_monthly.loc[df_monthly["month"] == current_month, "total"]
    if not month_total.empty:
        monthly_spend_value = float(month_total.iloc[0])

if not df_monthly.empty and {"month", "count"}.issubset(df_monthly.columns):
    month_count = df_monthly.loc[df_monthly["month"] == current_month, "count"]
    if not month_count.empty:
        transactions_this_month = int(month_count.iloc[0])

card_total = 0.0
paynow_total = 0.0
if not df_type.empty and {"type", "total"}.issubset(df_type.columns):
    tmp = df_type.copy()
    tmp["type_norm"] = tmp["type"].astype(str).str.strip().str.lower()

    card_match = tmp.loc[tmp["type_norm"] == "card", "total"]
    paynow_match = tmp.loc[tmp["type_norm"] == "paynow", "total"]

    if not card_match.empty:
        card_total = float(card_match.iloc[0])
    if not paynow_match.empty:
        paynow_total = float(paynow_match.iloc[0])

top1, top2, top3, top4 = st.columns(4)
top1.metric("Daily Spend", f"${daily_spend_value:,.2f}")
top2.metric("Monthly Spend", f"${monthly_spend_value:,.2f}")
top3.metric("Total Transactions This Month", transactions_this_month)
top4.metric("Current Month Merchants", len(df_merchants) if not df_merchants.empty else 0)

st.divider()

c1, c2 = st.columns(2)
c1.metric("Card", f"${card_total:,.2f}")
c2.metric("PayNow", f"${paynow_total:,.2f}")

st.divider()

st.subheader("Top 5 Merchants This Month")
if not df_merchants.empty:
    table_df = df_merchants.rename(
        columns={
            "to_merchant": "Merchant",
            "count": "Transactions",
            "total": "Total (SGD)",
        }
    )
    st.dataframe(
        table_df[["Merchant", "Transactions", "Total (SGD)"]],
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("No data")

st.divider()

bottom_left, bottom_right = st.columns(2)

with bottom_left:
    st.subheader("Daily Spend Breakdown")
    if not df_daily.empty:
        daily_table = df_daily.copy()
        daily_table["date"] = pd.to_datetime(daily_table["date"], errors="coerce")
        daily_table = daily_table.dropna(subset=["date"]).sort_values("date", ascending=False)
        daily_table["date"] = daily_table["date"].dt.strftime("%Y-%m-%d")
        daily_table = daily_table.rename(columns={"date": "Date", "total": "Total (SGD)"})
        st.dataframe(daily_table[["Date", "Total (SGD)"]], use_container_width=True, hide_index=True)
    else:
        st.info("No data")

with bottom_right:
    st.subheader("Monthly Spend Breakdown")
    if not df_monthly.empty:
        monthly_table = df_monthly.copy().sort_values("month", ascending=False)
        monthly_table = monthly_table.rename(
            columns={
                "month": "Month",
                "count": "Transactions",
                "total": "Total (SGD)",
            }
        )
        cols = [c for c in ["Month", "Transactions", "Total (SGD)"] if c in monthly_table.columns]
        st.dataframe(monthly_table[cols], use_container_width=True, hide_index=True)
    else:
        st.info("No data")