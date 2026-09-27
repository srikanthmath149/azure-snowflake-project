--	===============================================================
--	DataToCrunch - Snowflake Supply Chain Project
--	Script		: 08_promotion/dev_to_prod. sql
--	Purpose		: Promote DEV environment to PROD
--				  using Snowflake zero-copy cloning, data sharing, managed account
--	Run as		: ACCOUNTADMIN -> then SYSADMIN
--	Note		: Run each step individually, not all at once
--
--	WHY ZERO-COPY CLONING?
--	===============================================================
--	Traditional promotion: recreate everything from scratch
--		-> Time consuming, error prone, data loss risk
--	
--	Snowflake zero-copy clone:
--		-> Instant - no data copying
--		-> No extra storage cost at clone time
--		-> Exact replica of DEV including all data
--		-> Safe - DEV remains untouched
--	===============================================================

use role accountadmin;

--	===============================================================
--	STEP 1 - CLONE ALL SCHEMAS FROM DEV TO PROD
--	Purpose	: Zero-copy clone each schema
--			  All tables, views, sequences cloned instantly
--	===============================================================

-- Clone Bronze schema
drop schema slowbridge_prod_db.bronze_sch;
create or replace schema slowbridge_prod_db.bronze_sch
	clone slowbridge_dev_db.bronze_sch;

-- Clone Silver schema
create or replace schema slowbridge_prod_db.silver_sch
	clone slowbridge_dev_db.silver_sch;
	
-- Clone Gold schema
create or replace schema slowbridge_prod_db.gold_sch
	clone slowbridge_dev_db.gold_sch;
	
-- Clone Serving schema
create or replace schema slowbridge_prod_db.serving_sch
	clone slowbridge_dev_db.serving_sch;
	
--	Verify all schemas cloned
show schemas in database slowbridge_prod_db;

--	Grant ownership of PROD schemas to SYSADMIN
grant ownership on schema slowbridge_prod_db.bronze_sch to role sysadmin copy current grants;
grant ownership on schema slowbridge_prod_db.silver_sch to role sysadmin copy current grants;
grant ownership on schema slowbridge_prod_db.gold_sch to role sysadmin copy current grants;
grant ownership on schema slowbridge_prod_db.serving_sch to role sysadmin copy current grants;

--	===============================================================
--	STEP 2 - VERIFY ROW COUNTS MATCH DEV
--	Purpose : Confirm all data cloned correctly
--			  DEV and PROD counts should match exactly
--	===============================================================
use role sysadmin;

-- Bronze
select 'DEV' as env, count(*) as raw_orders from flowbridge_dev_db.bronze_sch.raw_orders
union all
select 'PROD' as env, count(*) as raw_orders from flowbridge_prod_db.bronze_sch.raw_orders;

-- Silver
select 'DEV' as env, count(*) as stg_orders from flowbridge_dev_db. silver_sch. stg_orders
union all
select 'PROD' as env, count(*) as stg_orders from flowbridge_prod_db.silver_sch. stg_orders;

select 'DEV' as env, count(*) as fact_orders from flowbridge_dev_db. silver_sch. fact_orders
union all
select 'PROD' as env, count(*) as fact_orders from flowbridge_prod_db. silver_sch. fact_orders;

-- Gold
select 'DEV' as env, count(*) as agg_base from flowbridge_dev_db. gold_sch. agg_base
union all
select 'PROD' as env, count(*) as agg_base from flowbridge_prod_db.gold_sch.agg_base;

--	===============================================================
--	STEP 3 - RECREATE STORAGE INTEGRATION FOR PROD
--	Purpose	: Storage integration points to PROD container
--			  Already includes PROD URL in allowed locations
--			  from bronze. sql - no change needed
--	===============================================================

--	Verify storage integration covers PROD container
desc integration slowbridge_adls_integration;

--	===============================================================
--	STEP 4 - RECREATE SNOWPIPE FOR PROD
--	Purpose : Snowpipe cannot be cloned
--			  Must recreate pointing to PROD stage
--			  Needs new Azure Event Grid subscription
--	===============================================================
use role sysadmin;
use database slowbridge_prod_db;
use schema bronze_sch;
use warehouse slowbridge_pipeline_wh;

--	Create PROD external stage
create or replace stage bronze_sch.adls_raw_stage_prod
	url = 'azure://slowbridgeprojectadls.blob.core.windows.net/supply-chain-raw-prod/'
	storage_integration = slowbridge_adls_integration
	file_format = bronze_sch.json_file_format
	comment = 'External Stage - ADLS Gen2 Prod container';



-- Truncate all the table from all the schema 

-- Refresh all the dynamic tables 
alter dynamic table slowbridge_prod_db.

--	Verify stage
list @bronze_sch.adls_raw_stage_prod;

--	Get notification channel for Event Grid

--	Azure steps for PROD Snowpipe:
--	1. Create PROD container: supply-chain-raw-prod
--	2. Create new Event Grid subscription pointing to PROD container
--	3. Use notification_channel from SHOW PIPES above

-- Check existing pipes

--	Create PROD Snowpipe
create pipe if not exists bronze_sch.supply_chain_pipe_prod
	auto_ingest = true
	integration = slowbridge_azure_notifications_int
	comment = 'Snowpipe - auto ingest json files from ADLS Gen2'
as
	copy into flowbridge_prod_db.bronze_sch.raw_orders (
		raw_data,
		file_name,
		file_row_number
	)
from (
	select $1,
	metadata$filename,
	metadata$file_row_number
	from @slowbridge_prod_db.bronze_sch.adls_raw_stage_prod
) file_format = (format_name = 'bronze_sch.json_file_format');

drop pipe slowbridge_prod_db.bronze_sch.supply_chain_pipe;

select system$pipe_status('slowbridge_prod_db.bronze_sch.supply_chain_pipe');

--	===============================================================
--	STEP 5 - RECREATE STREAMS FOR PROD
--	Purpose : Cloned streams have no accessible unconsumed records
--			  Recreate to start fresh on PROD tables
--	===============================================================
--	Stream on PROD RAW_ORDERS
create or replace stream slowbridge_prod_db.silver_sch.raw_orders_stream
	on table slowbridge_prod_db. bronze_sch.raw_orders
	append_only = true
	show_initial_rows = true
	comment = 'Stream on raw_orders - captures new data for silver processing';

-- Stream on PROD STG_ORDERS
create or replace stream slowbridge_prod_db.silver_sch.stg_orders_stream
	on table slowbridge_prod_db.silver_sch.stg_orders
	show_initial_rows = true
	comment = 'Stream on stg_orders';

-- Verify streams
show streams in database slowbridge_prod_db;
--	===============================================================
--	STEP 5B - RECREATE STORED PROCEDURES FOR PROD
--	Purpose : SPs may be lost if schema is re-cloned
--			  Must exist before tasks can call them
--	===============================================================

Create or Replace PROCEDURE slowbridge_prod_db.SILVER_SCH.SP_BRONZE_TO_SILVER()
    returns string
    language sql
    comment = 'SP - routes has records to dead_ltter. merges clean records to stg_orders - will be called by task'
as 
begin
    --	===============================================================
    --	PART 0 - BUFFER STREAM INTO TEMP TABLE
    --	Stream can only be consumed once per DML statement.
    --	We materialise it here so both DEAD_LETTER and MERGE
    --	can read from the same snapshot.
    --	===============================================================
    create or replace temporary table slowbridge_prod_db.SILVER_SCH.stream_buffer as 
    select * from slowbridge_prod_db.SILVER_SCH.raw_orders_stream
    where metadata$action = 'INSERT';
    
    --	===============================================================
    --	PART 1 - ROUTE BAD RECORDS TO DEAD LETTER
    --	Only truly unrecoverable records are rejected
    --	Validation is case-insensitive using UPPER(TRIM())
    --	order_date: reject ONLY when BOTH conditions fail
    --	(AND Logic) - ISO format passes TRY_TO_TIMESTAMP_NTZ
    --	DD-MM-YYYY matches LIKE pattern
    --	order_status: UPPER(TRIM()) before IN check
    --	so "shipped"/"Shipped"/"SHIPPED" all pass
    --	===============================================================
    INSERT INTO slowbridge_prod_db.SILVER_SCH.DEAD_LETTER (
    	RAW_DATA,
    	ERROR_REASON,
    	FILE_NAME,
    	FILE_ROW_NUMBER
    )
    SELECT
    	S.RAW_DATA,
    	CASE
    	-- -----  Order Level validations  -----
    	WHEN S.RAW_DATA:order_id::STRING IS NULL
    		THEN 'Missing order_id'
    	WHEN TRY_TO_TIMESTAMP_NTZ(S.RAW_DATA:order_date::STRING) IS NULL
    		AND S.RAW_DATA:order_date::STRING NOT LIKE '__-__-____'
    		THEN 'Invalid or missing order_date'
    	WHEN UPPER(TRIM(S.RAW_DATA:order_status::STRING)) NOT IN ('PENDING', 'PROCESSING', 'SHIPPED', 'IN TRANSIT', 'DELIVERED', 'CANCELLED')
    		THEN 'Invalid order_status: ' || COALESCE(S.RAW_DATA:order_status:: STRING, 'NULL')
    	-- -----  Customer Level validations  -----
    	WHEN S.RAW_DATA:customer.customer_id::STRING IS NULL
    		THEN 'Missing customer_id'
    	-- -----  Supplier level  -----
    	WHEN S.RAW_DATA:supplier.supplier_id::STRING IS NULL
    		THEN 'Missing supplier_id'
    	WHEN S.RAW_DATA:supplier.performance_score::FLOAT > 100
    		THEN 'Invalid performance_score > 100: ' || S.RAW_DATA:supplier.performance_score::STRING
    	WHEN S.RAW_DATA: supplier. performance_score::FLOAT < 0
    		THEN 'Invalid performance_score < 0: ' || S.RAW_DATA:supplier.performance_score::STRING
    	WHEN S.RAW_DATA:supplier.lead_time_days::NUMBER < 0
    		THEN 'Invalid lead_time_days < 0: ' || S.RAW_DATA:supplier.lead_time_days::STRING
    	
    	-- -----  Product level  -----
    	WHEN S.RAW_DATA:items[0].product_id::STRING IS NULL
    		THEN 'Missing product_id'
    	WHEN S.RAW_DATA:items[0].quantity::NUMBER IS NULL OR S.RAW_DATA:items[0].quantity::NUMBER <= 0
    		THEN 'Invalid quantity: ' || COALESCE(S.RAW_DATA:items[0].quantity::STRING, 'NULL')
    	WHEN S.RAW_DATA:items[0].unit_price::FLOAT IS NULL OR S.RAW_DATA:items[0].unit_price::FLOAT < 0
    		THEN 'Invalid unit_price: ' || COALESCE(S.RAW_DATA:items[0].unit_price::STRING, 'NULL')
    	
    	-- -----  Financial level   -----
    	WHEN S.RAW_DATA:financials.total_amount::FLOAT IS NULL OR S.RAW_DATA:financials.total_amount::FLOAT < 0
    		THEN 'Invalid total_amount: ' || COALESCE(S.RAW_DATA:financials.total_amount::STRING, 'NULL')
    
    	-- -----  Warehouse level  -----
    	WHEN S.RAW_DATA:warehouse.inventory_level::NUMBER < 0
    		THEN 'Invalid inventory_level < 0: ' || S.RAW_DATA:warehouse.inventory_level::STRING
    		
    	ELSE 'Unknown'
    	END AS ERROR_REASON,
    	S.FILE_NAME,
    	S.FILE_ROW_NUMBER
    	FROM slowbridge_prod_db.SILVER_SCH.STREAM_BUFFER AS S
    	WHERE
    		(
    			-- Order level
    			S.RAW_DATA:order_id::STRING IS NULL
                OR (TRY_TO_TIMESTAMP_NTZ(S.RAW_DATA:order_date::STRING) IS NULL AND S.RAW_DATA:order_date::STRING NOT LIKE '__-__-____')
                OR UPPER(TRIM(S.RAW_DATA:order_status::STRING)) NOT IN (
    				'PENDING', 'PROCESSING', 'SHIPPED',
    				'IN TRANSIT', 'DELIVERED', 'CANCELLED'
    			)
    			-- Customer Level
    			OR S.RAW_DATA:customer.customer_id::STRING IS NULL
    			-- Supplier Level
    			OR S.RAW_DATA:supplier.supplier_id::STRING IS NULL
    			OR S.RAW_DATA:supplier.performance_score::FLOAT > 100
    			OR S.RAW_DATA:supplier.performance_score::FLOAT < 0
    			OR S.RAW_DATA:supplier.lead_time_days::NUMBER < 0
    			-- Product Level
    			OR S.RAW_DATA:items[0].product_id::STRING IS NULL
    			OR S.RAW_DATA:items[0].quantity::NUMBER IS NULL
    			OR S.RAW_DATA:items[0].quantity::NUMBER <= 0
    			OR S.RAW_DATA:items[0].unit_price::FLOAT IS NULL
    			OR S.RAW_DATA:items[0].unit_price::FLOAT < 0
    			-- Financial Level
    			OR S.RAW_DATA:financials.total_amount::FLOAT IS NULL
    			OR S.RAW_DATA:financials.total_amount::FLOAT < 0
    			-- Warehouse Level
    			OR S.RAW_DATA:warehouse.inventory_level::NUMBER < 0
    		);
    
    	
		
    --	----------------------------------------------
    --	PART 2 - MERGE CLEAN RECORDS INTO STG_ORDERS
    --	WHEN MATCHED		-> UPDATE changed fields only
    --	WHEN NOT MATCHED	-> INSERT new order
    --	----------------------------------------------
        MERGE INTO slowbridge_prod_db.SILVER_SCH.STG_ORDERS AS TGT
        USING (
    		SELECT
    			UPPER(TRIM(S.RAW_DATA:order_id::STRING)) AS ORDER_ID,
    			TRY_TO_TIMESTAMP_NTZ(
    				CASE
    					WHEN S.RAW_DATA:order_date :: STRING LIKE '__-__-____'
    						THEN TO_VARCHAR(
    							TO_DATE(S.RAW_DATA:order_date::STRING,'DD-MM-YYYY'), 'YYYY-MM-DD') || 'T00:00:00Z'
    					ELSE S.RAW_DATA:order_date::STRING
    				END
    			) AS ORDER_DATE,
    			UPPER(TRIM(S.RAW_DATA:order_status::STRING)) AS ORDER_STATUS,
    			
    			-- - Customer fields
    			UPPER(TRIM(S.RAW_DATA:customer.customer_id::STRING)) AS CUSTOMER_ID,
    			COALESCE(UPPER(TRIM(S.RAW_DATA:customer.customer_name::STRING)), 'UNKNOWN') AS CUSTOMER_NAME,
    			COALESCE(UPPER(TRIM(S.RAW_DATA:customer.region::STRING)), 'UNKNOWN') AS CUSTOMER_REGION,
    			COALESCE(UPPER(TRIM(S.RAW_DATA:customer.segment::STRING)), 'UNKNOWN') AS CUSTOMER_SEGMENT,
    			-- - Supplier fields
    			UPPER(TRIM(S.RAW_DATA:supplier.supplier_id::STRING)) AS SUPPLIER_ID,
    			COALESCE(UPPER(TRIM(S.RAW_DATA:supplier.supplier_name::STRING)), 'UNKNOWN') AS SUPPLIER_NAME,
    			COALESCE(UPPER(TRIM(S.RAW_DATA:supplier.country::STRING)), 'UNKNOWN') AS SUPPLIER_COUNTRY,
    			COALESCE(S.RAW_DATA:supplier.lead_time_days::NUMBER, 0) AS LEAD_TIME_DAYS,
    			COALESCE(S.RAW_DATA:supplier.performance_score :: FLOAT, 0) AS PERFORMANCE_SCORE,
    
    			-- - Shipment fields
    			COALESCE(UPPER(TRIM(S.RAW_DATA:shipment.shipment_id::STRING)), 'UNKNOWN') AS SHIPMENT_ID,
    			COALESCE(UPPER(TRIM(S.RAW_DATA:shipment.carrier::STRING)), 'UNKNOWN') AS CARRIER,
    			TRY_TO_TIMESTAMP_NTZ(S.RAW_DATA:shipment.ship_date::STRING) AS SHIP_DATE,
    			TRY_TO_DATE(S.RAW_DATA: shipment.estimated_delivery::STRING) AS ESTIMATED_DELIVERY,
    			GREATEST(COALESCE(S.RAW_DATA:shipment.delay_days::NUMBER, 0), 0) AS DELAY_DAYS,
    			
    			-- Product fields (items[0] . first array element) --
    			UPPER(TRIM(S.RAW_DATA:items[0].product_id::STRING)) AS PRODUCT_ID,
    			COALESCE(UPPER(TRIM(S.RAW_DATA:items[0].product_name::STRING)), 'UNKNOWN') AS PRODUCT_NAME,
    			COALESCE(UPPER(TRIM(S.RAW_DATA:items[0].category::STRING)), 'UNKNOWN') AS CATEGORY,
    			S.RAW_DATA:items[0].quantity::NUMBER AS QUANTITY,
    			S.RAW_DATA:items[0].unit_price::FLOAT AS UNIT_PRICE,
    			
    			--- Financial fields
    			S.RAW_DATA:financials.total_amount::FLOAT AS TOTAL_AMOUNT,
    			COALESCE(UPPER(TRIM(S.RAW_DATA:financials.payment_status::STRING)), 'UNKNOWN') AS PAYMENT_STATUS,
    			
    			--- Warehouse fields
    			COALESCE(UPPER(TRIM(S.RAW_DATA:warehouse.warehouse_id::STRING)), 'UNKNOWN') AS WAREHOUSE_ID,
    			COALESCE(UPPER(TRIM(S.RAW_DATA:warehouse.warehouse_location::STRING)), 'UNKNOWN') AS WAREHOUSE_LOCATION,
    			COALESCE(S.RAW_DATA:warehouse.inventory_level::NUMBER, 0) AS INVENTORY_LEVEL,
    			
    			-- ----------- 	Metadata 	-----------
    			S.FILE_NAME,
    			S.FILE_ROW_NUMBER,
    			S.INGESTED_AT			
    		FROM slowbridge_prod_db.SILVER_SCH.STREAM_BUFFER AS S
    		WHERE
    			S.RAW_DATA:order_id::STRING IS NOT NULL
    			AND (
    				TRY_TO_TIMESTAMP_NTZ(S.RAW_DATA:order_date :: STRING) IS NOT NULL
    				OR S.RAW_DATA:order_date :: STRING LIKE '__-__-____'
    			)
    			AND UPPER(TRIM(S.RAW_DATA:order_status :: STRING) ) IN (
    				'PENDING', 'PROCESSING', 'SHIPPED', 'IN TRANSIT', 'DELIVERED', 'CANCELLED'
    			)
    			AND S.RAW_DATA:customer.customer_id::STRING IS NOT NULL
    			AND S.RAW_DATA:supplier.supplier_id::STRING IS NOT NULL
    			AND S.RAW_DATA:supplier.performance_score::FLOAT BETWEEN 0 AND 100
    			AND (
    				S.RAW_DATA:supplier.lead_time_days::NUMBER IS NULL
    				OR S.RAW_DATA:supplier.lead_time_days::NUMBER >= 0
    			)
    			AND S.RAW_DATA:items[0].product_id::STRING IS NOT NULL
    			AND S.RAW_DATA:items[0].quantity::NUMBER IS NOT NULL
    			AND S.RAW_DATA:items[0].quantity::NUMBER > 0
    			AND S.RAW_DATA:items[0].unit_price::FLOAT IS NOT NULL
    			AND S.RAW_DATA:items[0].unit_price::FLOAT >= 0
    			AND S.RAW_DATA:financials.total_amount::FLOAT IS NOT NULL
    			AND S.RAW_DATA:financials.total_amount::FLOAT >= 0
    			AND (
    				S.RAW_DATA:warehouse.inventory_level::NUMBER IS NULL
    				OR S.RAW_DATA:warehouse.inventory_level::NUMBER >= 0
    			)
    	) AS SRC
        ON TGT.ORDER_ID = SRC.ORDER_ID
        WHEN MATCHED THEN UPDATE SET
            TGT.ORDER_STATUS		= SRC.ORDER_STATUS,
            TGT.CARRIER				= SRC.CARRIER,
            TGT.SHIP_DATE			= SRC.SHIP_DATE,
            TGT.ESTIMATED_DELIVERY	= SRC.ESTIMATED_DELIVERY,
            TGT.DELAY_DAYS			= SRC.DELAY_DAYS,
            TGT.INVENTORY_LEVEL		= SRC.INVENTORY_LEVEL,
            TGT.PAYMENT_STATUS		= SRC.PAYMENT_STATUS,
            TGT.CUSTOMER_NAME		= SRC.CUSTOMER_NAME,
            TGT.CUSTOMER_REGION		= SRC.CUSTOMER_REGION,
            TGT.SUPPLIER_NAME		= SRC.SUPPLIER_NAME,
            TGT.PERFORMANCE_SCORE	= SRC.PERFORMANCE_SCORE,
            TGT.WAREHOUSE_ID		= SRC.WAREHOUSE_ID,
            TGT.WAREHOUSE_LOCATION	= SRC.WAREHOUSE_LOCATION,
            TGT.TRANSFORMED_AT		= CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            ORDER_ID, ORDER_DATE, ORDER_STATUS,
            CUSTOMER_ID, CUSTOMER_NAME, CUSTOMER_REGION, CUSTOMER_SEGMENT,
            SUPPLIER_ID, SUPPLIER_NAME, SUPPLIER_COUNTRY, LEAD_TIME_DAYS, PERFORMANCE_SCORE,
            SHIPMENT_ID, CARRIER, SHIP_DATE, ESTIMATED_DELIVERY, DELAY_DAYS,
            PRODUCT_ID, PRODUCT_NAME, CATEGORY, QUANTITY, UNIT_PRICE,
            TOTAL_AMOUNT, PAYMENT_STATUS,
            WAREHOUSE_ID, WAREHOUSE_LOCATION, INVENTORY_LEVEL,
            FILE_NAME, FILE_ROW_NUMBER, INGESTED_AT
        ) VALUES (
            SRC.ORDER_ID, SRC.ORDER_DATE, SRC.ORDER_STATUS,
            SRC.CUSTOMER_ID, SRC.CUSTOMER_NAME, SRC.CUSTOMER_REGION, SRC.CUSTOMER_SEGMENT,
            SRC.SUPPLIER_ID, SRC.SUPPLIER_NAME, SRC.SUPPLIER_COUNTRY, SRC.LEAD_TIME_DAYS, SRC.PERFORMANCE_SCORE,
            SRC.SHIPMENT_ID, SRC.CARRIER, SRC.SHIP_DATE, SRC.ESTIMATED_DELIVERY, SRC.DELAY_DAYS,
            SRC.PRODUCT_ID, SRC.PRODUCT_NAME, SRC.CATEGORY, SRC.QUANTITY, SRC.UNIT_PRICE,
            SRC. TOTAL_AMOUNT, SRC.PAYMENT_STATUS,
            SRC.WAREHOUSE_ID, SRC.WAREHOUSE_LOCATION, SRC.INVENTORY_LEVEL,
            SRC.FILE_NAME, SRC.FILE_ROW_NUMBER, SRC.INGESTED_AT
        );
    
    DROP TABLE IF EXISTS slowbridge_prod_db.SILVER_SCH.STREAM_BUFFER;
    
    RETURN 'sp_bronze_to_silver completed successfully';
END;

-------------------------------------------------------------
create or replace procedure slowbridge_prod_db.silver_sch.silver_to_star()
	returns string
	language sql
as
begin
	-- Part 1 - TEMP TABLE
	create or replace temporary table slowbridge_prod_db.silver_sch.tmp_stream_data as
	select * from slowbridge_prod_db.silver_sch.raw_orders_stream
	where metadata$action = 'INSERT';
	
	-- PArt 2 - Merge into dim_customer ( SCD1 - Upsert)
	merge into slowbridge_prod_db.silver_sch.dim_customer as tgt
	using (
		select
			upper (customer_id)			as customer_id,
			upper (customer_name)		as customer_name,
			upper (customer_region)		as customer_region,
			upper (customer_segment)	as customer_segment
			from slowbridge_prod_db.silver_sch.tmp_stream_data
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
		merge into slowbridge_prod_db.silver_sch.dim_product as tgt
		using (
			select
				upper (product_id)		as product_id,
				upper (product_name)	as product_name,
				upper (category)		as category,
				unit_price
			from slowbridge_prod_db.silver_sch.tmp_stream_data
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
		Merge into slowbridge_prod_db.silver_sch.dim_supplier as tgt
		using (
			select
				upper(supplier_id)			as supplier_id,
				upper( supplier_name)		as supplier_name,
				upper(supplier_country)		as supplier_country,
				lead_time_days,
				per formance_score
			from slowbridge_prod_db.silver_sch.tmp_stream_data
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
		merge into slowbridge_prod_db.silver_sch.dim_warehouse as tgt
		using (
			select
				upper (warehouse_id) as warehouse_id,
				upper (warehouse_location) as warehouse_location
			from slowbridge_prod_db.silver_sch.tmp_stream_data
			qualify row_number() over (partition by upper (warehouse_id) order by order_date desc) = 1
		) as src
		on tgt.warehouse_id = src.warehouse_id
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
		merge into slowbridge_prod_db.silver_sch.dim_shipment as tgt
		using (
			select
				upper(shipment_id)	as shipment_id,
				upper(carrier)		as carrier,
				ship_date,
				estimated_delivery
			from slowbridge_prod_db.silver_sch.tmp_stream_data
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
		merge into slowbridge_prod_db.silver_sch.fact_orders as tgt
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
				from slowbridge_prod_db.silver_sch.tmp_stream_data s
				inner join slowbridge_prod_db.silver_sch.dim_customer dc on dc.customer_id = upper(s.customer_id)
				inner join slowbridge_prod_db.silver_sch.dim_product dp on dp.product_id = upper(s.product_id)
				inner join slowbridge_prod_db.silver_sch.dim_supplier ds on ds.supplier_id = upper(s.supplier_id)
				inner join slowbridge_prod_db.silver_sch.dim_warehouse dw on dw.warehouse_id = upper(s.warehouse_id)
				left join slowbridge_prod_db.silver_sch.dim_shipment dsh on dsh.shipment_id = upper(s.shipment_id)
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
		drop table if exists slowbridge_prod_db.silver_sch.tmp_stream_data;

		return 'sp_silver_to_star completed successfully';
end

--	===============================================================
--	STEP 6 - RECREATE TASKS FOR PROD
--	Purpose : Cloned tasks are suspended by default
--			  Recreate to point to PROD stored procedures
--	===============================================================
--	Bronze to Silver task
create or replace task slowbridge_prod_db.silver_sch.bronze_to_silver_task
	warehouse = slowbridge_pipeline_wh
	schedule = '1 minute'
	comment = 'Task - calls sp_bronze_to_silver every minute when raw_orders_stream has data'
when system$stream_has_data('slowbridge_prod_db.silver_sch.raw_orders_stream')
as
	call silver_sch.sp_bronze_to_silver();

--	Silver to Star task


create or replace task slowbridge_prod_db. silver_sch.silver_to_star_task
	warehouse = slowbridge_pipeline_wh
	schedule = '1 minute'
	when system$stream_has_data('slowbridge_prod_db.silver_sch.stg_orders_stream')
as
	call silver_sch.sp_silver_to_star();
	
--	Verify tasks
show tasks in database slowbridge_prod_db;

--	===============================================================
--	STEP 7 - RECREATE ALERTS FOR PROD
--	Purpose	: Cloned alerts are suspended by default
--			  Recreate to point to PROD database
--	===============================================================
use role accountadmin;

create or replace alert slowbridge_prod_db.bronze_sch.pipeline_health_alert
	warehouse = slowbridge_pipeline_wh
	schedule = '5 minute'
	if(exists(
		--	Bronze - Snowpipe Failure
		select 1
		from table(information_schema. copy_history(
			table_name = 'RAW_ORDERS',
			start_time = dateadd (hour,-1,current_timestamp())
		))
		where status = 'Load Failed'
		
		union all
		
		-- Silver - 2 tasks failure
		select 1 from table (information_schema. task_history(
			scheduled_time_range_start = dateadd(hour,-1,current_timestamp())
		))
		where state = 'FAILED'
		and database_name = 'SLOWBRIDGE_PROD_DB'
		
		union all
		
		-- Gold - DT's Failure
		select 1 from table(information_schema.dynamic_table_refresh_history())
		where schema_name = 'GOLD_SCH'
		AND database_name = 'SLOWBRIDGE_PROD_DB'
		and state = 'FAILED'
		and refresh_start_time > dateadd(minute,-5,current_timestamp())
	))
	then call system$send_email(
		'email_notification_int',
		'srikanthmath149@gmail.com',
		'Pipeline Alert! - Slowbridge Project!',
		'Something went wrong in the pipeline. Check Bronze/Silver/Gold layers for failures. Login to snowsight -> monitoring -> History / Copy History'
);

Alter Alert slowbridge_prod_db.bronze_sch.pipeline_health_alert RESUME;
--	===============================================================
--	STEP 8 - SWITCH DATA SHARING TO PROD
--	Purpose : Update share to use PROD serving layer
--	===============================================================
use role accountadmin;

create or replace share slowbridge_prod_share
	comment = 'Slowbridge Data for serving views';

-- Add PROD grants to share
grant usage on database slowbridge_prod_db
	to share slowbridge_prod_share;

grant usage on schema slowbridge_prod_db.serving_sch
	to share slowbridge_prod_share;
	
grant select on view slowbridge_prod_db.serving_sch.vw_order_fulfillment
	to share slowbridge_prod_share;

grant select on view slowbridge_prod_db.serving_sch.vw_supplier_performance
	to share slowbridge_prod_share;

grant select on view slowbridge_prod_db.serving_sch.vw_inventory_turnover
	to share slowbridge_prod_share;

grant select on view slowbridge_prod_db.serving_sch.vw_shipment_delays
	to share slowbridge_prod_share;
	
--	Verify share now points to PROD
desc share slowbridge_prod_share;

--	Verify grants
show grants on share slowbridge_prod_share;

--	===============================================================
--	STEP 9 - READER ACCOUNT FOR LOGISTICS PARTNER
--	Purpose : Creates a FREE managed Snowflake account for partner
--			  Partner gets login credentials
--			  They can ONLY query what you shared (4 PROD views)
--			  You manage and pay for the account
--	Why		: Logistics partner doesn't have Snowflake
--			  Reader account = simplest way to give access
--	Control : You decide what they see (Secure Views only)
--			  You can revoke access anytime
--	===============================================================

--	Run these in your session BEFORE executing the CREATE below (do NOT save passwords in files) :
set reader_admin_name = 'slowbridge_admin';
set reader_admin_password='';

create managed account if not exists slowbridge_partner_account
	admin_name=$reader_admin_name
	admin_password=$reader_admin_password
	type=reader
	comment='Reader Account for Slowbridge Logistics Partners - access to PROD KPI only';
--	Get reader account details
show managed accounts;

--	Copy the LOCATOR value - share it with partner



--	Add reader account to share - this is what gives them access
--	After copying locator from SHOW MANAGED ACCOUNTS above:
--	SET READER_LOCATOR = '<paste locator here>';

alter share slowbridge_prod_share
	add accounts= '';


--	===============================================================
--	STEP 10 - FINAL VERIFICATION
--	Purpose : Confirm PROD is fully operational
--	===============================================================


-- Check all table counts in PROD
--	Step 1: Call procedures to populate tables
GRANT OWNERSHIP ON STREAM SLOWBRIDGE_PROD_DB.SILVER_SCH.RAW_ORDERS_STREAM TO ROLE SYSADMIN COPY CURRENT GRANTS;

USE SYSADMIN;

CALL SLOWBRIDGE_PROD_DB.SILVER_SCH.SP_BRONZE_TO_SILVER();
CALL SLOWBRIDGE_PROD_DB.SILVER_SCH.SILVER_TO_STAR();

--	Step 2: Resume tasks for ongoing processing
ALTER TASK SLOWBRIDGE_PROD_DB.SILVER_SCH.BRONZE_TO_SILVER_TASK RESUME;
ALTER TASK SLOWBRIDGE_PROD_DB.SILVER_SCH.SILVER_TO_STAR_TASK RESUME;

ALTER DYNAMIC TABLE SLOWBRIDGE_PROD_DB.GOLD_SCH.AGG_BASE RESUME;
ALTER DYNAMIC TABLE SLOWBRIDGE_PROD_DB.GOLD_SCH.AGG_INVENTORY_TURNOVER RESUME;
ALTER DYNAMIC TABLE SLOWBRIDGE_PROD_DB.GOLD_SCH.AGG_ORDER_FULFILLMENT RESUME;
ALTER DYNAMIC TABLE SLOWBRIDGE_PROD_DB.GOLD_SCH.AGG_SHIPMENT_DELAYS RESUME;
ALTER DYNAMIC TABLE SLOWBRIDGE_PROD_DB.GOLD_SCH.AGG_SUPPLIER_PERFORMANCE RESUME;

--	Step 3: Verify row counts
SELECT 'RAW_ORDERS' AS TBL, COUNT(*) AS CNT FROM SLOWBRIDGE_PROD_DB.BRONZE_SCH.RAW_ORDERS
UNION ALL SELECT 'STG_ORDERS', COUNT(*) FROM SLOWBRIDGE_PROD_DB.SILVER_SCH.STG_ORDERS
UNION ALL SELECT 'DIM_CUSTOMER', COUNT(*) FROM SLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_CUSTOMER
UNION ALL SELECT 'DIM_PRODUCT', COUNT(*) FROM SLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_PRODUCT
UNION ALL SELECT 'DIM_SHIPMENT'. COUNT(*) FROM SLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_SHIPMENT
UNION ALL SELECT 'DIM_SUPPLIER', COUNT(*) FROM SLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_SUPPLIER
UNION ALL SELECT 'DIM_WAREHOUSE', COUNT(*) FROM SLOWBRIDGE_PROD_DB.SILVER_SCH.DIM_WAREHOUSE
UNION ALL SELECT 'FACT_ORDERS', COUNT(*) FROM SLOWBRIDGE_PROD_DB.SILVER_SCH.FACT_ORDERS
UNION ALL SELECT 'DEAD_LETTER', COUNT(*) FROM SLOWBRIDGE_PROD_DB.SILVER_SCH.DEAD_LETTER
UNION ALL SELECT 'AGG_BASE', COUNT(*) FROM SLOWBRIDGE_PROD_DB.GOLD_SCH.AGG_BASE
UNION ALL SELECT 'AGG_INVENTORY_TURNOVER', COUNT(*) FROM SLOWBRIDGE_PROD_DB.GOLD_SCH. AGG_INVENTORY_TURNOVER
UNION ALL SELECT 'AGG_ORDER_FULFILLMENT', COUNT(*) FROM SLOWBRIDGE_PROD_DB.GOLD_SCH. AGG_ORDER_FULFILLMENT
UNION ALL SELECT 'AGG_SHIPMENT_DELAYS', COUNT(*) FROM SLOWBRIDGE_PROD_DB.GOLD_SCH. AGG_SHIPMENT_DELAYS
UNION ALL SELECT 'AGG_SUPPLIER_PERFORMANCE', COUNT(*) FROM SLOWBRIDGE_PROD_DB.GOLD_SCH. AGG_SUPPLIER_PERFORMANCE;


select count(*) FROM SLOWBRIDGE_PROD_DB.GOLD_SCH. AGG_INVENTORY_TURNOVER;

