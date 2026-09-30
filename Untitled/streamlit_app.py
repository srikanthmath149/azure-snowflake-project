# Import python packages
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
from snowflake.snowpark.context import get_active_session
import os

session = get_active_session()

database= 'SLOWBRIDGE_PROD_DB'

# Write directly to the app
st.title(f"SlowBridge Dashboard :balloon:")

if st.button("Refresh Data"):
    st.cache_data.clear()

st.divider()

col_filter1, col_filter2 = st.columns(2)
with col_filter1:
    date_type = st.radio("Filter By:",["Order Date","Ingestion Date"],horizontal=True)

with col_filter2:
    date_options = ["Today","This Week","Last 7 Days","Last 14 Days","Last 30 Days","Last 90 Days","YTD","Last Year","All Time"]
    selected_period=st.selectbox("Time Period:" , date_options,index=2)

today = datetime.now().date()
if selected_period == "Today":
    start_date = today
    end_date = today
elif selected_period == "This Week":
    start_date = timedelta(days=today.weekday())
    end_date = today
elif selected_period == "Last 7 Days":
    start_date = timedelta(days=7)
    end_date = today
elif selected_period == "Last 14 Days":
    start_date = timedelta(days=14)
    end_date = today
elif selected_period == "Last 30 Days":
    start_date = timedelta(days=30)
    end_date = today
elif selected_period == "Last 90 Days":
    start_date = timedelta(days=90)
    end_date = today
elif selected_period == "YTD":
    start_date = today.replace(month=1,day=1)
    end_date = today
elif selected_period == "Last Year":
    start_date = today - timedelta(days=365)
    end_date = today
else:
    start_date = None
    end_date = None

all_statuses = ["PENDING","PROCESSING","SHIPPED","IN TRANSIT","DELIVERED","CANCELLED"]

all_regions = ["NORTH AMERICA","EUROPE","ASIA PACIFIC"]

col_filter3,col_filter4=st.columns(2)
with col_filter3:
  status_mode = st.selectbox("Order Status",["All"]+all_statuses,index=0,key="status_mode")
  if status_mode == "All":
      selected_statuses = all_statuses
  else:
      extra_statuses = st.multiselect("Add More Statuses:",[s for s in all_statuses if s != status_mode],key="extra_status")
      selected_statuses = [status_mode] + extra_statuses

with col_filter4:
    region_mode = st.selectbox("Customer Region", ["All"] + all_regions, index=0, key="region_mode")
    if region_mode == "All":
        selected_regions = all_regions
    else:
        extra_regions = st.multiselect("Add more regions", [r for r in all_regions if r != region_mode], key="extra_region")
        selected_regions = [region_mode] + extra_regions

date_column = "ORDER_DATE" if date_type == "Order Date" else "INGESTION_DATE"

date_filter = ""
if start_date and end_date:
    date_filter = f"WHERE {date_column} BETWEEN  '{start_date}' AND '{end_date} 23:59:59'"

status_filter = ""
if status_mode != "All":
    status_list = ",".join([f"'{s}'" for s in selected_statuses])
    status_filter = f"{'AND' if date_filter else 'WHERE'} ORDER_STATUS IN ( {status_list} )"

region_filter = ""
if region_mode != "All":
    region_list = ",".join([f"'{r}'" for r in selected_regions])
    region_filter = f"{'AND' if date_filter or status_filter else 'WHERE'} CUSTOMER_REGION IN ({region_list})"

combined_filter = f"{date_filter} {status_filter} {region_filter}"

st.divider()


@st.cache_data(ttl=60)
def load_fulfillment (combined_filter):
    df = session. sql(f"""
        SELECT
            CUSTOMER_REGION,
            CUSTOMER_SEGMENT,
			COUNT(*) AS TOTAL_ORDERS,
			COUNT(CASE WHEN ORDER_STATUS = 'DELIVERED' THEN 1 END) AS DELIVERED_ORDERS,
			COUNT(CASE WHEN ORDER_STATUS = 'PENDING' THEN 1 END) AS PENDING_ORDERS,
			COUNT(CASE WHEN ORDER_STATUS = 'CANCELLED' THEN 1 END) AS CANCELLED_ORDERS,
			COUNT(CASE WHEN ORDER_STATUS = 'IN_TRANSIT' THEN 1 END) AS IN_TRANSIT_ORDERS,
			ROUND(COUNT(CASE WHEN ORDER_STATUS = 'DELIVERED' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS FULFILLMENT_RATE_PCT,
			SUM(TOTAL_AMOUNT) AS TOTAL_REVENUE,
			AVG(TOTAL_AMOUNT) AS AVG_ORDER_VALUE
		FROM {database}.GOLD_SCH.AGG_BASE
		{combined_filter}
		GROUP BY CUSTOMER_REGION, CUSTOMER_SEGMENT
	""").to_pandas()
    return df

@st.cache_data(ttl=60)
def load_suppliers(combined_filter):
	df = session.sql(f"""
		SELECT
			SUPPLIER_ID,
			SUPPLIER_NAME,
			SUPPLIER_COUNTRY,
			COUNT(*) AS TOTAL_ORDERS,
			AVG(PERFORMANCE_SCORE) AS AVG_PERFORMANCE_SCORE,
			AVG(LEAD_TIME_DAYS) AS AVG_LEAD_TIME_DAYS,
			SUM (TOTAL_AMOUNT) AS TOTAL_REVENUE,
			COUNT (CASE WHEN DELAY_DAYS = 0 THEN 1 END) AS ON_TIME_ORDERS,
			COUNT (CASE WHEN DELAY_DAYS > 0 THEN 1 END) AS DELAYED_ORDERS,
			ROUND(COUNT (CASE WHEN DELAY_DAYS = 0 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS ON_TIME_RATE_PCT,
			AVG (DELAY_DAYS) AS AVG_DELAY_DAYS
		FROM {database}.GOLD_SCH.AGG_BASE
		{combined_filter}
		GROUP BY SUPPLIER_ID, SUPPLIER_NAME, SUPPLIER_COUNTRY
	""").to_pandas()
	return df

@st.cache_data(ttl=60)
def load_inventory(combined_filter):
	df = session. sql (f"""
        SELECT
            WAREHOUSE_ID,
            WAREHOUSE_LOCATION,
            CATEGORY,
            COUNT(*) AS TOTAL_ORDERS,
            SUM(QUANTITY) AS TOTAL_QUANTITY_ORDERED,
            AVG(INVENTORY_LEVEL) AS AVG_INVENTORY_LEVEL,
            SUM(TOTAL_AMOUNT) AS TOTAL_REVENUE,
			ROUND(CASE WHEN AVG(INVENTORY_LEVEL) > 0 THEN-SUM (QUANTITY) / AVG(INVENTORY_LEVEL) ELSE 0 END, 2) AS INVENTORY_TURNOVER_RATIO
		FROM {database}.GOLD_SCH.AGG_BASE
		{combined_filter}
		GROUP BY WAREHOUSE_ID, WAREHOUSE_LOCATION, CATEGORY
	""").to_pandas()
	return df

@st.cache_data(ttl=60)
def load_shipments(combined_filter):
    df = session. sql(f"""
        SELECT
            CARRIER,
            CUSTOMER_REGION,
            COUNT(*) AS TOTAL_SHIPMENTS,
            COUNT(CASE WHEN DELAY_DAYS = 0 THEN 1 END) AS ON_TIME_SHIPMENTS,
            COUNT(CASE WHEN DELAY_DAYS > 0 THEN 1 END) AS DELAYED_SHIPMENTS,
            ROUND(COUNT(CASE WHEN DELAY_DAYS = 0 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS ON_TIME_RATE_PCT,
            AVG(DELAY_DAYS) AS AVG_DELAY_DAYS,
            SUM(TOTAL_AMOUNT) AS TOTAL_REVENUE
        FROM {database}.GOLD_SCH.AGG_BASE
        {combined_filter}
        {"AND" if combined_filter.strip() else "WHERE"} CARRIER IS NOT NULL AND CARRIER != 'UNKNOWN'
        GROUP BY CARRIER, CUSTOMER_REGION
    """).to_pandas()
    return df

@st.cache_data(ttl=60)
def load_kpis(combined_filter):
	df = session. sql (f"""
		SELECT
			COUNT(*) AS TOTAL_ORDERS,
			SUM(TOTAL_AMOUNT) AS TOTAL_REVENUE,
			ROUND(COUNT(CASE WHEN ORDER_STATUS = 'DELIVERED' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS FULFILLMENT_RATE,
			ROUND(COUNT(CASE WHEN DELAY_DAYS = 0 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS ON_TIME_RATE
		FROM {database}.GOLD_SCH.AGG_BASE
		{combined_filter}
	""" ).to_pandas()
	return df

@st.cache_data(ttl=60)
def load_time_series(combined_filter) :
	df = session. sql(f"""
		SELECT
			DATE_TRUNC('DAY', ORDER_DATE) AS ORDER_DAY,
			COUNT(ORDER_ID) AS DAILY_ORDERS,
			SUM(TOTAL_AMOUNT) AS DAILY_REVENUE,
			AVG(DELAY_DAYS) AS AVG_DELAY
		FROM {database}.GOLD_SCH.AGG_BASE
		{combined_filter}
		GROUP BY 1
		ORDER BY 1
	""").to_pandas()
	return df
	
df_f = load_fulfillment(combined_filter)
df_s = load_suppliers(combined_filter)
df_i = load_inventory(combined_filter)
df_d = load_shipments(combined_filter)
df_ts = load_time_series(combined_filter)
df_kpi = load_kpis(combined_filter)

if df_kpi.empty or df_kpi['TOTAL_ORDERS'].iloc[0] == 0:
	st.warning("No data available for the selected time period.")
	st.stop()
	
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Orders", f"{int(df_kpi['TOTAL_ORDERS'].iloc[0]):,}")
col2.metric("Total Revenue", f"${df_kpi['TOTAL_REVENUE'].iloc[0]:,.0f}")
col3.metric("Fulfillment Rate", f"{df_kpi['FULFILLMENT_RATE'].iloc[0]}%")
col4.metric("On-Time Rate", f"{df_kpi['ON_TIME_RATE'].iloc[0]}%")

st.divider ()

tab1, tab2, tab3 = st.tabs(["Trends", "Breakdown", "Suppliers"])

with tab1:
	st. subheader("Daily Orders")
	if not df_ts.empty:
		st.line_chart(df_ts, x="ORDER_DAY", y="DAILY_ORDERS")
		
	st.subheader("Daily Revenue") # Section header
	if not df_ts.empty: # Only render if data exists
		st.area_chart(df_ts, x="ORDER_DAY", y="DAILY_REVENUE")
	
	st.subheader("Avg Delay Days (Daily)")
	if not df_ts.empty:
		st.line_chart(df_ts, x="ORDER_DAY", y="AVG_DELAY")
		
with tab2:
	col_a, col_b = st.columns (2)
	
	with col_a:
		st.subheader("Orders by Region")
		if not df_f.empty:
			data = df_f.groupby ("CUSTOMER_REGION")["TOTAL_ORDERS"].sum().reset_index ()
			st.bar_chart(data, x="CUSTOMER_REGION", y="TOTAL_ORDERS")
	
	with col_b:
		st. subheader("Revenue by Carrier")
		if not df_d.empty:
			data = df_d.groupby("CARRIER")["TOTAL_REVENUE"].sum().reset_index()
			st.bar_chart(data, x="CARRIER", y="TOTAL_REVENUE")
	
	st.subheader("On-Time vs Delayed by Carrier")
	if not df_d.empty:
		data = df_d.groupby("CARRIER")[["ON_TIME_SHIPMENTS","DELAYED_SHIPMENTS"]].sum().reset_index()
		st.bar_chart(data, x="CARRIER", y=["ON_TIME_SHIPMENTS", "DELAYED_SHIPMENTS"])
		
	st. subheader ("Inventory Turnover by Category")
	if not df_i.empty:
		data = df_i. groupby ("CATEGORY")["INVENTORY_TURNOVER_RATIO"].mean().reset_index()
		st.bar_chart(data, x="CATEGORY", y="INVENTORY_TURNOVER_RATIO")

with tab3:
	st.header("Supplier Scorecard")
	if not df_s.empty:
		st.dataframe(
			df_s[["SUPPLIER_NAME", "SUPPLIER_COUNTRY", "TOTAL_ORDERS", "AVG_PERFORMANCE_SCORE", "ON_TIME_RATE_PCT", "AVG_LEAD_TIME_DAYS"]],
			use_container_width=True,
			hide_index=True
		)
		
		st. subheader ("Performance Score vs On-Time Rate")
		if not df_s.empty:
			st.bar_chart(
				df_s. set_index ("SUPPLIER_NAME" ) [["AVG_PERFORMANCE_SCORE", "ON_TIME_RATE_PCT"]]
			)

