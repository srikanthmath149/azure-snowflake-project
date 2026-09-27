--	===============================================================
--	DataToCrunch - Snowflake Supply Chain Project
--	Script		: 04_gold/gold. sql
--	Purpose		: Gold layer - Dynamic Tables
--				  AGG_BASE (base layer - joins silver dims + fact)
--				  AGG_ORDER_FULFILLMEN Ldownstream)
--				  AGG_SUPPLIER_PERFORMANCE (downstream)
--				  AGG_INVENTORY_TURNOVER (downstream)
--				  AGG_SHIPMENT_DELAYS (downstream)
--	Run as		: SYSADMIN
--	Note		: Run each step individually, not all at once
--	Dynamic Table pattern:
--	AGG_BASE	->	LAG = '1 MINUTE' (queries Silver)
--	All others	->	LAG = DOWNSTREAM (refresh when base refreshes)
--	Benefit		->	Silver queried once, all KPIs consistent
--	===============================================================

--	===============================================================
--	STEP 1 - AGG_BASE (Base Dynamic Table)
--	Purpose : Joins FACT_ORDERS with all 5 dimensions
--			  Single source of truth for all Gold KPIs
--			  All downstream tables query this - not Silver
--	LAG		: 1 MINUTE - controls refresh cadence for all
--			  downstream tables automatically
--	Why base: Silver layer queried only ONCE per refresh
--			  All downstream KPIs use same consistent snapshot
--			  Cost efficient - no duplicate Silver queries
--	joins	: LEFT JOIN DIM_SHIPMENT - order may not be shipped
--	===============================================================
use role sysadmin;
use warehouse slowbridge_pipeline_wh;
use database slowbridge_dev_db;
use schema gold_sch;

create or replace dynamic table gold_sch.agg_base
	lag = '1 minute'
	warehouse = slowbridge_pipeline_wh
	comment = 'Base Dynamic Table'
as
select
	-- - Order fields  --------------------
	f.ORDER_ID,
	f.ORDER_DATE,
	f.ORDER_STATUS,
	f.PAYMENT_STATUS,
	-- - Measures  --------------------
	f.QUANTITY,
	f.UNIT_PRICE,
	f.TOTAL_AMOUNT,
	f.DELAY_DAYS,
	f.INVENTORY_LEVEL,	
	-- - Pipeline metadata  ------------------
	f.INGESTED_AT,	
	-- - Customer fields  --------------------
	c.CUSTOMER_ID,
	c.CUSTOMER_NAME,
	c.CUSTOMER_REGION,
	c.CUSTOMER_SEGMENT,	
	-- - Product fields  --------------------
	p.PRODUCT_ID,
	p.PRODUCT_NAME,
	p.CATEGORY,	
	-- - Supplier fields  --------------------
	sp.SUPPLIER_ID,
	sp.SUPPLIER_NAME,
	sp.SUPPLIER_COUNTRY,
	sp.LEAD_TIME_DAYS,
	sp.PERFORMANCE_SCORE,	
	-- - Warehouse fields
	w.WAREHOUSE_ID,
	w.WAREHOUSE_LOCATION,
	-- - Shipment fields  --------------------
	-- LEFT JOIN - NULL if order not yet shipped
	sh.SHIPMENT_ID,
	sh.CARRIER,
	sh.SHIP_DATE,
	sh.ESTIMATED_DELIVERY
	from silver_sch.fact_orders as f
	JOIN SILVER_SCH.DIM_CUSTOMER		c	ON f.CUSTOMER_SK = c.CUSTOMER_SK
	JOIN SILVER_SCH.DIM_PRODUCT			p	ON f.PRODUCT_SK= p.PRODUCT_SK
	JOIN SILVER_SCH.DIM_SUPPLIER		sp	ON f.SUPPLIER_SK = sp.SUPPLIER_SK
	JOIN SILVER_SCH.DIM_WAREHOUSE		w	ON f.WARHOUSE_SK = w.WAREHOUSE_SK
	LEFT JOIN SILVER_SCH.DIM_SHIPMENT	sh	ON f.SHIPMENT_SK = sh.SHIPMENT_SK;


select * from gold_sch.agg_base;
--	===============================================================
--	STEP 2 - AGG_ORDER_FULFILLMENT (Downstream)
--	Purpose		: Order fulfillment KPIs by customer region
--	Source		: AGG_BASE (not Silver directly)
--	LAG			: DOWNSTREAM - refreshes when AGG_BASE refreshes
--	KPIS
--	total_orders		- total orders per region
--	delivered_orders	- orders with status DELIVERED
--	pending_orders		- orders with status PENDING/PROCESSING
--	cancelled_orders	- orders with status CANCELLED
--	fulfillment_rate	- delivered / total * 100
--	total_revenue		- sum of total_amount
--	avg_order_value		- average order amount
--	===============================================================
create or replace dynamic table gold_sch. agg_order_fulfillment
	lag = downstream
	warehouse = slowbridge_pipeline_wh
	comment = 'Order fulfillment KPIs by customer region. DOWNSTREAM from AGG_BASE.'
as
select
	customer_region,
	customer_segment,
	-- Order counts
	count(order_id) as total_orders,
	count(case when order_status = 'DELIVERED' then 1 end) as delivered_orders,
	count(case when order_status in ( 'PENDING', 'PROCESSING') then 1 end) as pending_orders,
	count(case when order_status = 'CANCELLED' then 1 end) as cancelled_orders,
	count(case when order_status = 'IN TRANSIT' then 1 end) as in_transit_orders,
	-- Fulfillment rate - delivered / total * 100
	round(count(case when order_status = 'DELIVERED' then 1 end) / nullif(count(order_id),0) * 100,2) as FULFILLMENT_RATE_PCT,
	-- Revenue KPIs
	round( sum(total_amount),2) as total_revenue,
	round(avg( total_amount),2) as avg_order_value,
	-- Payment breakdown
	count(case when payment_status = 'PAID' then 1 end) as paid_orders,
	count(case when payment_status = 'PENDING' then 1 end) as payment_pending_orders,
	count(case when payment_status = 'OVERDUE' then 1 end) as overdue_orders
from gold_sch.agg_base
group by customer_region,
		 customer_segment;

select * from gold_sch. agg_order_fulfillment;
--	===============================================================
--	STEP 3 - AGG_SUPPLIER_PERFORMANCE (Downstream)
--	Purpose	: Supplier performance KPIs
--	Source	: AGG_BASE (not Silver directly)
--	LAG		: DOWNSTREAM - refreshes when AGG_BASE refreshes
--	KPIs	:
--	total_orders			- orders per supplier
--	avg_performance_score	- average score
--	avg_lead_time_days		- average lead time
--	total_revenue			- revenue generated
--	on_time_orders			- orders with delay_days = 0
--	delayed_orders			- orders with delay_days > 0
--	on_time_rate			- on time / total * 100
--	avg_delay_days			- average delay when delayed
--	===============================================================
create or replace dynamic table gold_sch.agg_supplier_performance
	lag = downstream
	warehouse = slowbridge_pipeline_wh
	comment = 'Supplier performance KPIs. DOWNSTREAM from AGG_BASE.'
as
select
	supplier_id,
	supplier_name,
	supplier_country,
	-- Order volume
	count(order_id) as total_orders,
	-- Performance metrics
	round(avg(performance_score),2) as avg_performance_score,
	round(avg(lead_time_days),2) as avg_lead_time_days,
	-- Revenue
	round(sum(total_amount),2) as total_reveune,
	round(avg(total_amount),2) as avg_order_value,
	-- Delay metrics
	count(case when delay_days = 0 then 1 end) as on_time_orders,
	count(case when delay_days > 0 then 1 end) as delayed_orders,
	-- On time rate
	round(count(case when delay_days = 0 then 1 end)/nullif(count(order_id), 0)*100, 2) as on_time_rate_pct,
	-- Average delay when delayed
	round(avg(case when delay_days > 0 then delay_days end),2) as avg_delay_days
from gold_sch.agg_base
group by
	supplier_id,
	supplier_name,
	supplier_country;

select * from gold_sch.agg_supplier_performance;

--	===============================================================
--	STEP 4 - AGG_INVENTORY_TURNOVER (Downstream)
--	Purpose	: Inventory KPIs by warehouse and product
--	Source	: AGG_BASE (not Silver directly)
--	LAG		: DOWNSTREAM - refreshes when AGG_BASE refreshes
--	KPIs
--		total_orders		- orders processed per warehouse
--		total_quantity		- total units ordered
--		avg_inventory_level	- average inventory snapshot
--		min_inventory_level	- lowest inventory recorded
--		max_inventory_level	- highest inventory recorded
--		total_revenue		- revenue per warehouse
--		inventory_turnover	- total_quantity / avg_inventory
--	===============================================================
create or replace dynamic table gold_sch.agg_inventory_turnover
	lag = downstream
	warehouse = slowbridge_pipeline_wh
	comment = 'Inventory turnover KPIs by warehouse and category. DOWNSTREAM from AGG_BASE.'
as
select
	warehouse_id,
	warehouse_location,
	category,
	-- Order volume
	count(order_id) as total_orders,
	sum(quantity) as total_quantity_ordered,
	-- Inventory metrics
	round(avg(inventory_level),2) as avg_inventory_level,
	min(inventory_level) as min_inventory_level,
	max(inventory_level) as max_inventory_level,
	-- Revenue
	round(sum(total_amount),2) as total_revenue,
	-- Inventory turnover ratio
	-- Higher = faster moving inventory
	round(sum(quantity)/nullif(avg(inventory_level),0),2) as inventory_turnover_ratio
from gold_sch.agg_base
group by
	warehouse_id,
	warehouse_location,
	category;

select * from gold_sch.agg_inventory_turnover;
--	===============================================================
--	STEP 5 - AGG_SHIPMENT_DELAYS (Downstream)
--	Purpose		: Shipment delay KPIs by carrier
--	Source		: AGG_BASE (not Silver directly)
--	LAG			: DOWNSTREAM - refreshes when AGG_BASE refreshes
--	Filter		: Only shipped orders (SHIPMENT_ID IS NOT NULL
--				  AND SHIPMENT_ID != 'UNKNOWN')
--	KPIs		:
--	total_shipments		- shipments per carrier
--	on_time_shipments	- delay_days = 0
--	delayed_shipments	- delay_days > 0
--	on_time_rate		- on time / total * 100
--	avg_delay_days		- average delay across all shipments
--	max_delay_days		- worst delay
--	total_revenue		- revenue per carrier
--	===============================================================
CREATE OR REPLACE DYNAMIC TABLE GOLD_SCH. AGG_SHIPMENT_DELAYS
	LAG			= DOWNSTREAM
	WAREHOUSE	= slowbridge_pipeline_wh
	COMMENT		= 'Shipment delay KPIs by carrier. DOWNSTREAM from AGG_BASE. Only shipped orders included.'
AS
SELECT
	CARRIER,
	CUSTOMER_REGION,
	-- Shipment counts
	COUNT(ORDER_ID)									AS TOTAL_SHIPMENTS,
	COUNT(CASE WHEN DELAY_DAYS = 0 THEN 1 END)		AS ON_TIME_SHIPMENTS,
	COUNT(CASE WHEN DELAY_DAYS > 0 THEN 1 END)		AS DELAYED_SHIPMENTS,
	-- On time rate
	ROUND(
		COUNT(CASE WHEN DELAY_DAYS = 0 THEN 1 END)
		/ NULLIF(COUNT(ORDER_ID),0) * 100,
		2
	)												AS ON_TIME_RATE_PCT,
	-- Delay metrics
	ROUND(AVG(DELAY_DAYS), 2)						AS AVG_DELAY_DAYS,
	MAX(DELAY_DAYS)									AS MAX_DELAY_DAYS,
	-- Revenue
	ROUND(SUM(TOTAL_AMOUNT), 2)					AS TOTAL_REVENUE
FROM GOLD_SCH. AGG_BASE
-- Only include shipped orders
-- NULL SHIPMENT_ID = order not yet shipped
WHERE SHIPMENT_ID IS NOT NULL
GROUP BY
	CARRIER,
	CUSTOMER_REGION;

select * from  GOLD_SCH. AGG_SHIPMENT_DELAYS;
--	===============================================================
--	STEP 6 - VERIFY
--	===============================================================
--	Check all dynamic tables created
show dynamic tables in schema GOLD_SCH;


-- Check dynamic table refresh history
select * from table(information_schema.dynamic_table_refresh_history())
where schema_name='GOLD_SCH' AND  database_name='slowbridge_dev_db' order by refresh_start_time desc limit 10;