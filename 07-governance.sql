--	===============================================================
--	DataToCrunch - Snowflake Supply Chain Project
--	Script: 07_governance/governance. sql
--	Purpose: Governance - Email Notification Integration, Pipeline Health Alerts
--	Run as: ACCOUNTADMIN
--	Note: Run each step individually, not all at once
--	
--	WHY GOVERNANCE?
--	---------------
--	Without governance:
--	. Pipeline breaks at 2am + nobody knows until morning
--	. Partner needs data > manual file exports + stale data
--	. No control over who sees what
--	
--	With governance:
--	. Pipeline alert fires immediately + fix before business starts
--	. Partner queries live data directly > no CSV exports
--	. Full control over data access
--	===============================================================
use role accountadmin;
use database slowbridge_dev_db;
use warehouse slowbridge_pipeline_wh;

--	===============================================================
--	STEP 1 - EMAIL NOTIFICATION INTEGRATION
--	===============================================================
--	What		: Connects Snowflake to an email server
--	Why			: Required before any alert can send emails
--			  	  Without this + alerts run but no email sent
--	Real life	: Every enterprise monitoring system needs
--				  a notification channel - email is simplest
--	===============================================================

create notification integration if not exists email_notification_int
	type = email
	enabled = true
	comment = 'Email Notification Integration for pipeline health alerts';
	
--	Test email - verify integration is working
SELECT SYSTEM$START_USER_EMAIL_VERIFICATION('srikanthmath149');

call system$send_email(
	'email_notification_int',
	'srikanthmath149@gmail.com',
	'Slowbridge Project - DataToCrunch',
	'DataToCrunch - Alerts are ready!'
);
--	Verify integration created
show integrations;

--	===============================================================
--	STEP 2 - PIPELINE HEALTH ALERT
--	===============================================================	
--	What	: Single alert that monitors ALL 3 pipeline layers
--			  Bronze (Snowpipe) + Silver (Tasks) + Gold (Dynamic Tables)
--	Why		: Real companies run pipelines 24/7
--			  Nobody manually checks logs every minute
--			  Alert detects failures automatically and notifies instantly
--	Schedule: Every 5 minutes -good balance of cost vs speed
--	Condition: If ANY failure found in Bronze/Silver/Gold
--			   -> send email immediately
--	Real life: On-call engineers get paged at 2am
--	They fix the issue before business starts
--	Business never knows there was a problem
--	===============================================================	
create or replace alert bronze_sch.pipeline_health_alert
	warehouse = slowbridge_pipeline_wh
	schedule = '5 minute'
	if(exists(
		-- ----------- BRONZE - 1 Snowpipe Failure  ------------
		select 1
		from table(information_schema.copy_history(
			table_name = 'RAW_ORDERS',
			start_time = dateadd(hour,-1,current_timestamp())
		))
		where status = 'Load Failed'
	
		union all
		
		-- ----------- SILVER - 2 Tasks Failure  ------------
		select 1
		from table(information_schema.task_history(
			schedule_time_range_start = dateadd(hour,-1,current_timestamp())
		))
		where state = 'FAILED'
		and database_name = 'slowbridge_dev_db'
		
		union all
		
		-- ----------- GOLD - 3 DT's Failure  ------------
		select 1
		from table(information_schema.dynamic_table_refresh_history())
		where schema_name = 'GOLD_SCH'
		and database_name = 'slowbridge_dev_db'
		and state = 'FAILED'
		and refresh_start_time > dateadd(minute,-5,current_timestamp())
		
	))
	then call system$send_email(
		'email_notification_int',
		'srikanthmath149@gmail.com',
		'Pipeline Alert! - Slowbridge Project!',
		'Something went wrong in the pipeline. Check Bronze/Sliver/Gold Layer for failures. Login to snowsight -> monitoring -> Query History / Copy History'
	);
-- Activate alert - alerts are SUSPENDED by default
alter alert bronze_sch.pipeline_health_alert resume;

-- Verify alert is active
show alerts in database slowbridge_dev_db;

-- Check alert History
select * from table(information_schema.alert_history(
	scheduled_time_range_start => dateadd(hour,-1,current_timestamp())
))
order by scheduled_time desc
limit 10;


--	===============================================================	
--	STEP 3 - VERIFY GOVERNANCE OBJECTS
--	===============================================================	
--	Note: Data Sharing & Reader Account are PROD-only
--		  See 08_dev_to_prod.sql (Step 8) for share setup
--	===============================================================	

-- All alerts
show alerts in database slowbridge_dev_db;

-- Resource monitors (created in account_setup. sql)
show resource monitors;