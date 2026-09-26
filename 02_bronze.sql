-- ===========================================================
-- DataToCrunch - Snowflake Supply Chain Project
-- Script
-- Purpose  : Bronze layer - storage integration, file format.
--            external stage, RAW_ORDERS table, Snowpipe
-- Run as   : ACCOUNTADMIN > then SYSADMIN
-- ============================================================


-----------------------  ENVIRONMENT  -------------------------
use database slowbridge_dev_db;

-----------------------  STORAGE INTEGRATION -- Run as ACCOUNTADMIN  -------------------------------
use role accountadmin;

CREATE OR REPLACE storage integration slowbridge_adls_integration
    type = external_stage
    storage_provider='azure'
    enabled=true
    azure_tenant_id='d67cc2ef-ceb5-4b17-9fc5-946a74cd2ea2'
    storage_allowed_locations=(
        'azure://slowbridgeadls.blob.core.windows.net/supply-chain-raw-dev/',
        'azure://slowbridgeadls.blob.core.windows.net/supply-chain-raw-prod/'
    );

desc integration slowbridge_adls_integration;

-- A STEP 1 - Run this & open AZURE_CONSENT_URL in browser & Assign Storage Blob Data Reader Role
-- Azure Portal
-- Storage Account
-- Access Control [IAM)]
-- Add + Add role assignment
-- Role: Storage Blob Data Reader
-- Members + Select members
-- Search: (your AZURE_MULTI_TENANT_APP_NAME value)
-- Select + Review + assign

-- Grant to SYSADMIN
grant usage on integration slowbridge_adls_integration to role sysadmin;

------------------------------  NOTIFICATION INTEGRATION (Event Grid + Storage Queue)  ---------------

-- Step 1 - Create Storage Queue
-- Azure Portal
-- Storage Account (slowbridgeadls)
-- Left menu scroll down
-- Data storage
-- Queues
-- + Queue
-- Name: supply-chain-queue-v2
-- OK

-- Step 2 - Create Event Grid Subscription
-- Azure Portal
-- Storage Account (slowbridgeadls)
-- Left menu
-- Events
-- + + Event Subscription
-- Fill in:
-- Name             : supply-chain-snowpipe-sub
-- Event Schema     : Event Grid Schema
-- Event Types      : Blob Created ONLY (uncheck everything else)

-- Endpoint Type    : Storage Queues
-- Endpoint         : + Select an endpoint
--                    + Select storage account: datatocrunchproject8adls
--                    + Select queue: supply-chain-queue-v2
--                    + Confirm selection
-- Click Create

-- Step 3 - Verify Event Grid Is Working
-- Azure Portal
-- > Storage Account
-- > Events
-- You should see 2 event subscriptions listed for dev & prod with filer enabled which starts with /blobServices/default/containers/supply-chain-raw/ and /blobServices/default/containers/supply-chain-raw-prod/

-- Step 4 - Come Back to Snowflake
-- Once queue and Event Grid are set up, run the Notification Integration with the correct queue URL:
-- sqlAZURE_STORAGE_QUEUE_PRIMARY_URI =
-- 'https://datatocrunchproject8adls.queue.core.windows.net/supply-chain-queue-v2'

create or replace notification integration slowbridge_azure_notification_int
    enabled=true
    type=queue
    notification_provider=azure_storage_queue
    azure_storage_queue_primary_uri='https://slowbridgeadls.queue.core.windows.net/slowbridge-supply-chain-queue'
    azure_tenant_id='d67cc2ef-ceb5-4b17-9fc5-946a74cd2ea2';

-- A Run this + copy AZURE_CONSENT_URL + open in browser + Accept
-- copy AZURE_MULTI_TENANT_APP_NAME => assign Storage Queue Data Contributor in Azure IAM
desc integration slowbridge_azure_notification_int;

-- Grant to SYSADMIN
grant usage on integration slowbridge_azure_notification_int to role sysadmin;

----------------------  SWITCH TO SYSADMIN  ---------------------------------------------
use role sysadmin;
use database slowbridge_dev_db;
use schema slowbridge_dev_db.bronze_sch;
use warehouse slowbridge_pipeline_wh;

-----------------------  FILE FORMAT  -----------------------
CREATE FILE FORMAT IF NOT EXISTS bronze_sch.json_file_format
type='json'
strip_outer_array=true
comment='JSON File Format for slowbridge Project';

describe file format  bronze_sch.json_file_format;

-----------------------  EXTERNAL STAGE  -----------------------

SLOWBRIDGE_DEV_DB.BRONZE_SCH.ADLS_RAW_STAGECREATE stage if not exists bronze_sch.adls_raw_stage
    url='azure://slowbridgeadls.blob.core.windows.net/supply-chain-raw-dev/'
    storage_integration = slowbridge_adls_integration
    file_format = bronze_sch.json_file_format
    comment = 'External Stage - ADLS  Gen2 Dev Container';
    
list @bronze_sch.adls_raw_stage;
-- ======================================================
-- RAW_ORDERS TABLE
-- Transient - no fail-safe, cost efficient, reloadable
-- Metadata columns added at ingestion time by Snowpipe
-- ======================================================
CREATE OR REPLACE transient table bronze_sch.raw_orders (
    raw_data variant,
    ingested_at timestamp_ntz default current_timestamp(),
    file_name string,
    file_row_number number,
    load_id string default uuid_string()
) comment='Bronze Layer - raw JSON supply chain order for SlowBridge';

-- ======================================================
-- SNOWPIPE
-- Auto-ingest triggered by Azure Event Grid on file arrival
-- ======================================================
CREATE PIPE IF NOT EXISTS bronze_sch.supply_chain_pipe
    auto_ingest = true
    integration = slowbridge_azure_notification_int
    comment = 'Snowpipe - auto ingest json files from ADLS Gen2'
as 
    copy into bronze_sch.raw_orders(
        raw_data,
        file_name,
        file_row_number
    )
from (
    select $1,
            metadata$filename,
            metadata$file_row_number
    from @bronze_sch.adls_raw_stage
) file_format = (format_name = 'bronze_sch.json_file_format');


-- ======================================================
-- STEP 8 - VERIFY
-- ======================================================
-- Check pipe created
show pipes;

-- Check Snowpipe status
SELECT system$pipe_status('bronze_sch.supply_chain_pipe');

-- After files land check data loaded
-- Since your 3 files Landed before Snowpipe was created, Event Grid missed them. You need to manually refresh:
alter pipe bronze_sch.supply_chain_pipe refresh;

SELECT * FROM bronze_sch.raw_orders;

-- Check ingestion history 
SELECT * FROM table(information_schema.copy_history(
    table_name => 'raw_orders',
    start_time => dateadd(hours,-1,current_timestamp())
));