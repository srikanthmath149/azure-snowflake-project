use role accountadmin;

----------------------  Databases  ---------------------------
create database if not exists slowbridge_dev_db
comment ='Slowbridge supply chain - Developlment Database';

create database if not exists slowbridge_prod_db
comment ='Slowbridge supply chain - Production Database';


---------------------  SCHEMA - DEV  ----------------------------
CREATE SCHEMA if NOT EXISTS SLOWBRIDGE_DEV_DB.BRONZE_SCH
COMMENT='Raw Ingestation Layer';

CREATE SCHEMA if NOT EXISTS SLOWBRIDGE_DEV_DB.SILVER_SCH
COMMENT='Transformation Layer';

CREATE SCHEMA if NOT EXISTS SLOWBRIDGE_DEV_DB.GOLD_SCH
COMMENT='Aggregation Layer';

CREATE SCHEMA if NOT EXISTS SLOWBRIDGE_DEV_DB.SERVING_SCH
COMMENT='Serving Layer';


---------------------  SCHEMA - PROD  ----------------------------
CREATE SCHEMA if NOT EXISTS SLOWBRIDGE_PROD_DB.BRONZE_SCH
COMMENT='Raw Ingestation Layer';

CREATE SCHEMA if NOT EXISTS SLOWBRIDGE_PROD_DB.SILVER_SCH
COMMENT='Transformation Layer';

CREATE SCHEMA if NOT EXISTS SLOWBRIDGE_PROD_DB.GOLD_SCH
COMMENT='Aggregation Layer';

CREATE SCHEMA if NOT EXISTS SLOWBRIDGE_PROD_DB.SERVING_SCH
COMMENT='Serving Layer';

---------------------  Warhouses  ---------------------------------

-- Pieline WH - Snowpipe + Streams + Tasks + Dynamic Tables
CREATE WAREHOUSE IF NOT EXISTS SLOWBRIDGE_PIPELINE_WH
    warehouse_size='x-small'
    auto_suspend=60
    auto_resume=true
    comment='Pipeline workloads - imgestion + transformation';

-- ANALTYICS WH - STREAMLIT - SECURE VIEWS + READER ACCOUNT
CREATE  OR REPLACE WAREHOUSE SLOWBRIDGE_ANALYTICS_WH
    warehouse_size='x-small'
    auto_suspend=60
    auto_resume=true
    comment='Pipeline workloads - STREAMLIT + DATA SHARING';

------------------  Resource Monitors  -------------------------
--Pipeline warehouse monitor
CREATE OR REPLACE resource monitor slowbridge_pipeline_rm
    with credit_quota = 20
    frequency = monthly
    start_timestamp = immediately
    triggers
        on 75 percent do notify
        on 90 percent do notify
        on 100 percent do suspend;

-- Analytics warehouse monitor
CREATE OR REPLACE resource monitor slowbridge_analytics_rm
    with credit_quota = 20
    frequency = monthly
    start_timestamp = immediately
    triggers
        on 75 percent do notify
        on 90 percent do notify
        on 100 percent do suspend;

---- Assign monitors to warehouses
alter warehouse SLOWBRIDGE_PIPELINE_WH set resource_monitor=slowbridge_pipeline_rm;
alter warehouse SLOWBRIDGE_ANALYTICS_WH set resource_monitor=slowbridge_analytics_rm;

---------------------  Grant PRIVILEGES TO SYSADMIN  ----------------------------
Use role accountadmin;

grant execute task on account to role sysadmin;

grant usage on warehouse SLOWBRIDGE_PIPELINE_WH to role sysadmin;
grant usage on warehouse SLOWBRIDGE_ANALYTICS_WH to role sysadmin;

grant all privileges on database slowbridge_dev_db to role sysadmin;
grant all privileges on database slowbridge_prod_db to role sysadmin


grant all privileges on all schemas 
    in database slowbridge_dev_db to role sysadmin;

grant all privileges on all schemas 
    in database slowbridge_prod_db to role sysadmin;

----------------  VERIFY -------------------------
-- RUN these to confirm everything was created correctly
show databases like 'slowbridge_%';


show warehouses like 'slowbridge_%';

show resource monitors like 'slowbridge_%';