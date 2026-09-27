--	Script	: 05_serving/serving.sql
--	Purpose	: Serving layer - Secure Views over Gold
--			  Dynamic Tables for governed access
--	Run as	: SYSADMIN
--	Note	: Run each step individually, not all at once
--	Pattern : Secure Views on Gold Dynamic Tables
--			  Analysts query views - never tables directly
--			  Gold Dynamic Tables handle refresh automatically
--			  No extra serving tables needed
--	===============================================================


--	===============================================================
--	ENVIRONMENT
--	===============================================================
use role sysadmin;
use warehouse slowbridge_analytics_wh;
use database slowbridge_dev_db;
use schema gold_sch;



--	===============================================================
--	STEP 1 - VW_ORDER_FULFILLMENT
--	Purpose	: Governed access to order fulfillment KPIs
--	Source	: GOLD_SCH. AGG_ORDER_FULFILLMENT (Dynamic Table)
--	Type	: Secure View - view definition hidden from users
--			  who don't own the view
--	Used by : Streamlit dashboard + Reader Account
--	===============================================================

create or replace secure view serving_sch.vw_order_fulfillment
	comment = 'Secure View for GOLD_SCH. AGG_ORDER_FULFILLMENT (Dynamic Table)'
as
select
	customer_region,
	customer_segment,
	total_orders,
	delivered_orders,
	pending_orders,
	cancelled_orders,
	in_transit_orders,
	fulfillment_rate_pct,
	total_revenue,
	avg_order_value,
	paid_orders,
	payment_pending_orders,
	overdue_orders
from GOLD_SCH.AGG_ORDER_FULFILLMENT;
select * from serving_sch.vw_order_fulfillment;
--	===============================================================
--	STEP 2 - VW_SUPPLIER_PERFORMANCE
--	Purpose	: Governed access to supplier performance KPIs
--	Source	: GOLD_SCH. AGG_SUPPLIER_PERFORMANCE (Dynamic Table)
--	Type	: Secure View
--	Used by	: Streamlit dashboard + Reader Account
--	===============================================================
create or replace secure view serving_sch.vw_supplier_performance
	comment = 'Secure View for GOLD_SCH.AGG_SUPPLIER_PERFORMANCE (Dynamic Table)'
as
select
	supplier_id,
	supplier_name,
	supplier_country,
	total_orders,
	avg_performance_score,
	avg_lead_time_days,
	total_reveune,
	avg_order_value,
	on_time_orders,
	delayed_orders,
	on_time_rate_pct,
	avg_delay_days
from GOLD_SCH.AGG_SUPPLIER_PERFORMANCE;

Select * from GOLD_SCH.AGG_SUPPLIER_PERFORMANCE;
--	===============================================================
--	STEP 3 - VW_INVENTORY_TURNOVER
--	Purpose	: Governed access to inventory KPIs
--	Source	: GOLD_SCH. AGG_INVENTORY_TURNOVER (Dynamic Table)
--	Type	: Secure View
--	Used by	: Streamlit dashboard + Reader Account
--	===============================================================
create or replace secure view serving_sch.vw_inventory_turnover
	comment = 'Secure View for GOLD_SCH.AGG_INVENTORY_TURNOVER (Dynamic Table)'
as
select
	warehouse_id,
	warehouse_location,
	category,
	total_orders,
	total_quantity_ordered,
	avg_inventory_level,
	min_inventory_level,
	max_inventory_level,
	total_revenue,
	inventory_turnover_ratio
from GOLD_SCH.AGG_INVENTORY_TURNOVER;


--	===============================================================
--	STEP 4 - VW_SHIPMENT_DELAYS
--	Purpose : Governed access to shipment delay KPIs
--
--	===============================================================
create or replace secure view serving_sch.vw_shipment_delays
	comment = 'Secure View for GOLD_SCH.AGG_SHIPMENT_DELAYS (Dynamic Table)'
as
select
	carrier,
	customer_region,
	total_shipments,
	on_time_shipments,
	on_time_rate_pct,
	avg_delay_days,
	max_delay_days,
	total_revenue
from GOLD_SCH.AGG_SHIPMENT_DELAYS;

--	===============================================================
--	STEP 5 - VERIFY
--	===============================================================

-- Chcek all view created
show views in schema serving_sch;


-- Preview all Views
Select * from GOLD_SCH.AGG_SHIPMENT_DELAYS;