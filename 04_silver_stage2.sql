--	========================================================
--	DataToCrunch - Snowflake Supply Chain Project
--	Script	: 03_silver/silver_stage2.sql
--	Purpose	: Silver Stage 2 - Star Schema
--			DIM_CUSTOMER, DIM_PRODUCT, DIM_SUPPLIER,
--			FACT_ORDERS, Stream on STG_ORDERS
--			Stored Procedure. Task
--	Run as	: SYSADMIN
--	Note	: Run cach step individually, not all at once
--	String convention: all string columns stored as UPPERCASE
--	Surrogate keys: AUTOINCREMENT on all dimension tables
--	========================================================

--	========================================================
--	ENVIRONMENT
--	========================================================

use role sysadmin;
use database slowbridge_dev_db;
use schema silver_sch;
use warehouse slowbridge_pipeline_wh;

--	========================================================
--	STEP 1 - SEQUENCES
--	Purpose : Auto-increment surrogate keys for all tables
--	Must run BEFORE tables that reference them
--	Relative reference - no hardcoded database name
--	Works correctly in both DEV and PROD after clone
--	========================================================
create sequence if not exists silver_sch.dim_customer_sk_seq
    start=1
    increment=1;

create sequence if not exists silver_sch.dim_product_sk_seq
    start=1
    increment=1;

create sequence if not exists silver_sch.dim_supplier_sk_seq
    start=1
    increment=1;

create sequence if not exists silver_sch.dim_warehouse_sk_seq
    start=1
    increment=1;

create sequence if not exists silver_sch.dim_shipment_sk_seq
    start=1
    increment=1;

create sequence if not exists silver_sch.fact_orders_sk_seq
    start=1
    increment=1;

--	========================================================
--	STEP 2 - DIM_CUSTOMER
--	Purpose : Customer dimension table
--	Type: Permanent (time travel enabled)
--	Key: CUSTOMER_SK (surrogate) - CUSTOMER_ID (business)
--	Pattern : MERGE upsert - INSERT new. UPDATE changed
--	Soft delete: IS_ACTIVE flag - never hard delete
--	========================================================


create or replace table silver_sch.dim_customer (
	customer_sk number default dim_customer_sk_seq.nextval,
	customer_id string not null,
	customer_name string not null,
	customer_region string not null,
	customer_segment string not null,
	is_active BOOLEAN default true,
	created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
	updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) comment='Customer Dimension - star schema -surrogate key via sequence';

--	==========================================================
--	STEP 3 - DIM_PRODUCT
--	Purpose	: Product dimension table
--	Type	: Permanent (time travel enabled)
--	Key		: PRODUCT_SK (surrogate) - PRODUCT_ID (business)
--	Pattern	: MERGE upsert - INSERT new, UPDATE changed
--	Unit price updated on MERGE MATCHED - tracks latest price
--	==========================================================
create or replace table silver_sch.dim_product (
	product_sk number default dim_product_sk_seq.nextval,
	product_id string not null,
	product_name string not null,
	category string not null,
	unit_price float not null,
	is_active BOOLEAN default true,
	created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
	updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) comment='Product Dimension - star schema - surrogate key via sequence';


--	========================================================
--	STEP 4 - DIM_SUPPLIER
--	Purpose	: Supplier dimension table
--	Type	: Permanent (time travel enabled)
--	Key		: SUPPLIER_SK (surrogate) - SUPPLIER_ID (business)
--	Pattern	: MERGE upsert - INSERT new, UPDATE changed
--	Performance score + Lead time updated on MERGE MATCHED
--	========================================================
create or replace table silver_sch.dim_supplier (
	supplier_sk number default dim_supplier_sk_seq.nextval,
	supplier_id string not null,
	supplier_name string not null,
	supplier_country string not null,
	lead_time_days number not null,
	performance_score float not null,
	is_active BOOLEAN default true,
	created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
	updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) comment = 'Supplier Dimension - star schema - surrogate key via sequence';

--	========================================================
--	STEP 5 - DIM_WAREHOUSE
--	Purpose	: Warehouse dimension table
--	Type	: Permanent (time travel enabled)
--	Key		: WAREHOUSE_SK (surrogate) - WAREHOUSE_ID (business)
--	Pattern	: MERGE upsert - INSERT new. UPDATE changed
--	Soft delete: IS_ACTIVE flag - never hard delete
--	Why separate dim: warehouse_id + location reused
--	across many orders - classic dimension pattern
--	========================================================
create or replace table silver_sch.dim_warehouse (
	warehouse_sk number default dim_warehouse_sk_seq.nextval,
	warehouse_id string not null,
	warehouse_location string not null,
	is_active BOOLEAN default true,
	created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
	updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) comment = 'Warehouse Dimension - star schema - surrogate key via sequence';

--	========================================================
--	STEP 6 - DIM_SHIPMENT
--	Purpose		: Shipment dimension table
--	Type		: Permanent (time travel enabled)
--	Key			: SHIPMENT_SK (surrogate) - SHIPMENT_ID (business)
--	FK			: ORDER_ID FACT_ORDERS (supports 1-to-many)
--	Pattern		: MERGE upsert - INSERT new, UPDATE changed
--	Soft delete	: IS_ACTIVE flag - never hard delete
--	Why dimension not fact:
--		Shipment is primarily descriptive (carrier, dates, status)
--		DELAY_DAYS moved to FACT_ORDERS as a measure
--		Descriptive attributes + dimension
--	Supports 1-to-many via ORDER_ID FK:
--		One order can have many shipments in future
--		Split shipment, re-shipment scenarios handled
--	========================================================
create or replace table silver_sch.dim_shipment (
	shipment_sk number default dim_shipment_sk_seq.nextval,
	shipment_id string not null,
	carrier string not null,
	ship_date timestamp_ntz,
	estimated_delivery date,
	is_active BOOLEAN default true,
	created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
	updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) comment = 'Shipment Dimension - star schema - surrogate key via sequence';



--	=================================================================
--	STEP 7 - FACT_ORDERS
--	Purpose		: Central fact table - one row per order
--	Type		: Permanent (time travel enabled)
--	Key			: ORDER_SK (surrogate) - ORDER_ID (business)
--	FKs			: CUSTOMER_SK	->	DIM_CUSTOMER
--				  PRODUCT_SK	->	DIM_PRODUCT
--				  SUPPLIER_SK	->	DIM_SUPPLIER
--				  WAREHOUSE_SK	->	DIM_WAREHOUSE
--				  SHIPMENT_SK	->	DIM_SHIPMENT (-1 if not shipped)
--	Measures	: QUANTITY, UNIT_PRICE, TOTAL_AMOUNT,
--				  DELAY_DAYS, INVENTORY_LEVEL
--	=================================================================
create or replace table silver_sch. fact_orders (

	order_sk number default fact_orders_sk_seq. nextval,
	order_id string not null,

	customer_sk number not null,
	product_sk number not null,
	supplier_sk number not null,
	warhouse_sk number not null,
	shipment_sk number not null,

	order_date timestamp_ntz not null,
	order_status string not null,
	payment_status string not null,

	quantity number not null,
	unit_price float not null,
	total_amount float not null,
	delay_days number not null,
	inventory_level number not null,
	
	created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
	updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
	ingested_at timestamp_ntz
) comment = 'Fact Table - one row per order. 5 dim FKs & Measures';

--	============================================================
--	STEP 8 - STREAM ON STG_ORDERS
--	Purpose		: Captures INSERT + UPDATE from Stage 1 MERGE
--	Type		: Standard stream (no APPEND_ONLY)
--				  Stage 1 MERGE can insert AND update STG_ORDERS
--				  so we need full CDC - INSERT + UPDATE
--	Consumed by	: sp_silver_to_star stored procedure
--	=============================================================

create or replace stream silver_sch.stg_orders_stream
	on table silver_sch.stg_orders
	show_initial_rows = true
	comment = 'Stream on stg_orders';

show streams;

--	=============================================================
--	STEP 9 - STORED PROCEDURE: sp_silver_to_star
--	Purpose	: Builds star schema from STG_ORDERS
--			  Runs in strict order - dims first, fact last
--			  1. MERGE DIM_CUSTOMER
--			  2. MERGE - DIM_PRODUCT
--			  3. MERGE - DIM_SUPPLIER
--			  4. MERGE + DIM_WAREHOUSE
--			  5. MERGE DIM_SHIPMENT
--			  6. MERGE FACT_ORDERS (joins all 5 dims for SKs)
--	Why order matters:
--			  FACT_ORDERS needs all 5 dim SKs
--			  All dims must be populated first
--	Called by: silver_to_star_task (every 1 minute)
--	Test by	: CALL SILVER_SCH. sp_silver_to_star()
--	=============================================================
create or replace procedure silver_sch.silver_to_star()
	returns string
	language sql
as
begin
	-- Part 1 - TEMP TABLE
	create or replace temporary table silver_sch.tmp_stream_data as
	select * from silver_sch.raw_orders_stream
	where metadata$action = 'INSERT';
	
	-- PArt 2 - Merge into dim_customer ( SCD1 - Upsert)
	merge into silver_sch. dim_customer as tgt
	using (
		select
			upper (customer_id)			as customer_id,
			upper (customer_name)		as customer_name,
			upper (customer_region)		as customer_region,
			upper (customer_segment)	as customer_segment
			from silver_sch.tmp_stream_data
			qualify row_number() over (partition by upper(customer_id) order by order_date desc) = 1
		) as src
		on tgt.customer_id = src.customer_id
		when matched then update set
			tgt.customer_name		= src. customer_name,
			tgt.customer_region		= src.customer_region,
			tgt.customer_segment	= src.customer_segment,
			tgt.is_active			= true,
			tgt.updated_at			= current_timestamp()
		when not matched then insert (
			customer_id, customer_name, customer_region, customer_segment
		) values (
			src.customer_id, src.customer_name, src.customer_region, src.customer_segment
		);
		
		-- Part 3 - Merge into dim_product ( SCD1 - Upsert)
		merge into silver_sch. dim_product as tgt
		using (
			select
				upper (product_id)		as product_id,
				upper (product_name)	as product_name,
				upper (category)		as category,
				unit_price
			from silver_sch.tmp_stream_data
			qualify row_number() over (partition by upper (product_id) order by order_date desc) = 1
		) as src
		on tgt.product_id = src.product_id
		when matched then update set
			tgt.product_name	= src. product_name,
			tgt.category		= src.category,
			tgt.unit_price		= src.unit_price,
			tgt.is_active		= true,
			tgt.updated_at		= current_timestamp()
		when not matched then insert (
			product_id, product_name, category, unit_price
		) values (
			src.product_id, src.product_name, src.category, src.unit_price
		);
		
		-- Part 4 - Merge into dim_supplier ( SCD1 - Upsert)
		Merge into silver_sch.dim_supplier as tgt
		using (
			select
				upper(supplier_id)			as supplier_id,
				upper( supplier_name)		as supplier_name,
				upper(supplier_country)		as supplier_country,
				lead_time_days,
				per formance_score
			from silver_sch.tmp_stream_data
			qualify row_number() over (partition by upper (supplier_id) order by order_date desc) = 1
		) as src
		on tgt.supplier_id = src.supplier_id
		when matched then update set
			tgt.supplier_name		= src.supplier_name,
			tgt.supplier_country	= src.supplier_country,
			tgt.lead_time_days		= src.lead_time_days,
			tgt.performance_score	= src.performance_score,
			tgt.is_active			= true,
			tgt.updated_at			= current_timestamp()
		when not matched then insert (
			supplier_id, supplier_name, supplier_country, lead_time_days, performance_score
		) values (
			src. supplier_id, src.supplier_name, src.supplier_country, src. lead_time_days, src.performance_score
		);		
		
		-- Part 5 - Merge into dim_warehouse ( SCD1 - Upsert)
		merge into silver_sch.dim_warehouse as tgt
		using (
			select
				upper (warehouse_id) as warehouse_id,
				upper (warehouse_location) as warehouse_location
			from silver_sch.tmp_stream_data
			qualify row_number() over (partition by upper (warehouse_id) order by order_date desc) = 1
		) as src
		on tgt.warehouse_id = src. warehouse_id
		when matched then update set
			tgt.warehouse_location	= src.warehouse_location,
			tgt.is_active			= true,
			tgt.updated_at			= current_timestamp()
		when not matched then insert (
			warehouse_id, warehouse_location
		) values (
			src.warehouse_id, src.warehouse_location
		);
		
		--	Part 6 - Merge into dim_shipment ( SCD1 - Upsert)
		merge into silver_sch.dim_shipment as tgt
		using (
			select
				upper(shipment_id)	as shipment_id,
				upper(carrier)		as carrier,
				ship_date,
				estimated_delivery
			from silver_sch.tmp_stream_data
			where upper (shipment_id) != 'UNKNOWN'
			qualify row_number() over (partition by upper(shipment_id) order by order_date desc) = 1
		) as src
		on tgt. shipment_id = src. shipment_id
		when matched then update set
			tgt.carrier				= src.carrier,
			tgt.ship_date			= src.ship_date,
			tgt.estimated_delivery	= src.estimated_delivery,
			tgt.is_active			= true,
			tgt.updated_at			= current_timestamp()
		when not matched then insert (
			shipment_id, carrier, ship_date, estimated_delivery
		) values (
			src.shipment_id, src.carrier, src.ship_date, src.estimated_delivery
		);
		
		-- Part 7 - Merge into fact Orders
		merge into silver_sch. fact_orders as tgt
		using (
			select
				upper(s.order_id) as order_id,
				dc.customer_sk,
				dp.product_sk,
				ds.supplier_sk,
				dw.warehouse_sk,
				coalesce(dsh.shipment_sk, -1) as shipment_sk,
				s.order_date,
				upper(s.order_status) as order_status,
				upper(s.payment_status) as payment_status,
				s.quantity,
				s.unit_price,
				s.total_amount,
				s.delay_days,
				s.inventory_level,
				s.ingested_at
				from silver_sch.tmp_stream_data s
				inner join silver_sch.dim_customer dc on dc.customer_id = upper(s.customer_id)
				inner join silver_sch.dim_product dp on dp.product_id = upper(s.product_id)
				inner join silver_sch.dim_supplier ds on ds.supplier_id = upper(s.supplier_id)
				inner join silver_sch.dim_warehouse dw on dw.warehouse_id = upper(s.warehouse_id)
				left join silver_sch.dim_shipment dsh on dsh.shipment_id = upper(s.shipment_id)
				qualify row_number() over (partition by upper(s.order_id) order by s.order_date desc) = 1
		) as src
		on tgt.order_id = src.order_id
		when matched then update set
			tgt.customer_sk		= src.customer_sk,
			tgt.product_sk		= src.product_sk,
			tgt.supplier_sk		= src.supplier_sk,
			tgt.warehouse_sk	= src.warehouse_sk,
			tgt.shipment_sk		= src. shipment_sk,
			tgt.order_date		= src.order_date,
			tgt.order_status	= src.order_status,
			tgt.payment_status	= src.payment_status,
			tgt.quantity		= src.quantity,
			tgt.unit_price		= src.unit_price,
			tgt.total_amount	= src.total_amount,
			tgt.delay_days		= src.delay_days,
			tgt.inventory_level	= src. inventory_level,
			tgt.ingested_at		= src.ingested_at,
			tgt.updated_at		= current_timestamp()
		when not matched then insert (
			order_id, customer_sk, product_sk, supplier_sk, warehouse_sk, shipment_sk,
			order_date, order_status, payment_status, quantity, unit_price, total_amount,
			delay_days, inventory_level, ingested_at
		) values (
			src.order_id, src.customer_sk, src.product_sk, src.supplier_sk, src.warehouse_sk, src.shipment_sk,
			src.order_date, src.order_status, src.payment_status, src.quantity, src.unit_price, src.total_amount,
			src.delay_days, src.inventory_level, src.ingested_at
		);
		
		-- cleanup temp table - no longer needed after all MERGEs
		drop table if exists silver_sch.tmp_stream_data;

		return 'sp_silver_to_star completed successfully';
end

--	=============================================================
--	STEP 10 - TASK: silver_to_star_task
--	Purpose		: Orchestrates sp_silver_to_star every 1 minute
--	Schedule	: 1 MINUTE
--	WHEN		: SYSTEM$STREAM_HAS_DATA - no data = no run = no cost
--	Note		: Tasks created SUSPENDED by default
--				  Run ALTER TASK ... RESUME to activate
--	=============================================================
create or replace task silver_sch.silver_to_star_task
	warehouse = slowbridge_pipeline_wh
	schedule = '1 minute'
	when system$stream_has_data('silver_sch.stg_orders_stream')
as
	call silver_sch.sp_silver_to_star();

--	resume the task (tasks are SUSPENDED by default)
--	alter task silver_sch.silver_to_star_task resume;
--	=============================================================
--	STEP 11 - RESUME TASK
--	=============================================================
alter task silver_sch.BRONZE_TO_SILVER_TASK suspend;

show tasks;

--	=============================================================
--	STEP 12 - VERIFY
--	=============================================================
--	Check stream has data
select system$stream_has_data('SLOWBRIDGE_DEV_DB.SILVER_SCH.STG_ORDERS_STREAM' );


-- Check all table counts
select 'DIM_CUSTOMER', count(*) from silver_sch.dim_customer
union all
select 'DIM_PRODUCT', count(*) from silver_sch. dim_product
union all
select 'DIM_SUPPLIER', count(*) from silver_sch.dim_supplier
union all
select 'DIM_WAREHOUSE', count(*) from silver_sch.dim_warehouse
union all
select 'DIM_SHIPMENT', count(*) from silver_sch.dim_shipment
union all
select 'FACT_ORDERS', count(*) from silver_sch. fact_orders;


select * from silver_sch.dim_customer;

select * from silver_sch.dim_supplier;

select * from silver_sch.dim_product;

select * from silver_sch.dim_warehouse;

select * from silver_sch.dim_shipment;

select * from silver_sch.fact_orders;