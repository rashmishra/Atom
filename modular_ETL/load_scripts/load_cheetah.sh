#!/bin/bash

## Script Name: load_cheetah.sh
## Purpose: Modular ETL flow of Atom.


                # Parameters: 
                #             #### $1: Data Object. ####
                #             #### $2: Cloud Bucket Name. ####
                #             #### $3: Data Load directory. ####
                #             #### $4: Metadata Dataset name. ####
                #             #### $5: Dataset Name. ####
                #             #### $6: ETL Home Directory. ####
                #             #### $7: Respective incremental epoch. ####




taskStartTime=`date`
v_task_start_epoch=`date +%s`
v_task_start_ts=`echo $(date -d "@$v_task_start_epoch" +"%Y-%m-%d %r %Z")`;
v_task_datetime=`echo $(date -d "@$v_task_start_epoch" +"%Y-%m-%d_%H:%M_%Z")`;

# Extract date for Cheetah data
v_extract_date=$(date --date="-2 days" +%Y%m%d)

## Initializing required variables
v_etl_task='load'
stg_schemaFileName=schema_stg_cheetah.json
schemaFileName=schema_cheetah.json
maxBadRecords=50

v_data_object=$1;
tableName=$1;
v_fileName="cheetah_data_$v_extract_date.csv.gz";
v_cloud_storage_path=$2;
v_load_dir=$3;
v_metadataset_name=$4;
v_dataset_name=$5;
v_schema_filepath=$6/schema_files;
v_logs_dir=$6/logs;
v_temp_dir=$6/temp;
v_arch_dir=$6/arch;
v_transform_dir=$6/data/transform;
v_incremental_epoch=$7;

# We always pull 2 days ago data with 1 day interval. So setting the date as 2 days ago.
# If today is T, then we pull From: (T-2) To: (T-1)
# T=3 Jan, 2016. T-2= 1 Jan, 2016. T-1: 2 Jan, 2016
v_start_date=$(date --date="-2 days" +%Y-%m-%d)
v_tbl_date=$(date +%Y%m%d -d"$v_start_date")

v_task_status='Not set yet';
v_log_obj_txt+=`echo "\n------------------------------------------------------------"`;
v_log_obj_txt+=`echo "\n------------------------------------------------------------"`;
v_log_obj_txt+=`echo "\n$v_etl_task process started for $v_data_object at $v_task_start_ts"`;


echo "In cheetah Loading script";

## Function to check task status and exit if error occurs.
p_exit_upon_error(){

    ## Parameters
    # $1: Task Status (passed/ failed)
    # $2: Sub Task (e.g. Extraction of data, Cloud upload, metadata table creation, final table population)

    v_task_status="$1";
    v_subtask="$2";


    if [ $v_task_status == "failed" ] ; then
        v_log_obj_txt+=`echo "\n$(date) $(date) Task ($v_subtask) failed for $v_data_object. Hence exiting."`;

        taskEndTime=`date`;

        v_task_end_epoch=`date +%s`
        v_task_end_ts=`echo $(date -d "@$v_task_end_epoch" +"%Y-%m-%d %r %Z")`;

        v_bq_log_tbl_row='';

        ## Writing (appending) the CSV log table row into respective file
        v_bq_log_tbl_row="$v_data_object,$v_etl_task,$v_task_start_epoch,$v_task_start_ts,$v_task_status,$v_task_end_epoch,$v_task_end_ts";
        echo -e  "CSV Row for Load: \n$v_bq_log_tbl_row"
        echo $v_bq_log_tbl_row >> $v_logs_dir/log_ETL_tasks.csv
        
        v_log_obj_txt+=`echo "\n$(date) Log Table Row: \n$v_bq_log_tbl_row"`;

        ## Writing the status (success/ failure) into proper file

        echo $v_task_status > $v_temp_dir/${v_data_object}_load_status.txt
        chmod 0777 $v_temp_dir/${v_data_object}_load_status.txt;

        ## Writing the log of this task to files

        v_log_obj_txt+=`echo "\n$v_etl_task process ended for $v_data_object at $v_task_end_ts"`;
        v_log_obj_txt+=`echo "\n------------------------------------------------------------"`;
        v_log_obj_txt+=`echo "\n------------------------------------------------------------"`;

        # Maintaining the log of this run in a separate file in arch folder
        echo -e "$v_log_obj_txt" > $v_arch_dir/logs/"$v_data_object""_load_"$v_task_datetime.log


        # Creating new file for cheetah's ETL run. Content will be appended in further tasks of T and L.
        echo -e "$v_log_obj_txt" >> $v_temp_dir/"$v_data_object"_log.log

        chmod 0777 $v_temp_dir/"$v_data_object"_log.log;

        

        exit 1;

    fi

}

# Fetching the data file from Transform Directory to Load Directory
cd $v_transform_dir;

mv $v_fileName $v_load_dir

cd $v_load_dir;
echo "In Load directory $v_load_dir";
v_log_obj_txt+=`echo "\n$(date) In Load directory $v_load_dir"`;

########################################################################################
                             ## Loading into gcloud ##
########################################################################################
gsutil cp $v_fileName $v_cloud_storage_path 2> "$v_data_object"_cloud_result.txt &
v_pid=$!

if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

v_cloud_result=`cat "$v_data_object"_cloud_result.txt`;
echo "Loaded $v_fileName into $v_cloud_storage_path";
v_log_obj_txt+=`echo "\n$(date) Cloud Load of $v_fileName into $v_cloud_storage_path result: \n$v_cloud_result"`;
#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#
                     ## Completed: Loading into Google Cloud ## 
#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#

########################################################################################
## Checking if the Cloud Upload process has failed. If Failed, then exit this task (script). ##

v_subtask="Cloud Upload";
p_exit_upon_error "$v_task_status" "$v_subtask"

rm "$v_data_object"_cloud_result.txt

#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#
                     ## Completed: Checking for Process Failure ##
#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#

#v_incr_table_result=`echo $(bq load --quiet  --source_format=NEWLINE_DELIMITED_JSON --replace --ignore_unknown_values=1 --max_bad_records=$maxBadRecords $v_metadataset_name.incremental_$tableName $v_cloud_storage_path/$v_fileName $v_schema_filepath/$schemaFileName 2>&1)`
echo "Etl Home is $6."
echo "Schema File path is: $v_schema_filepath"

# Loading the data into staging table
v_destination_tbl="$v_metadataset_name.stg_${tableName}";
bq load --quiet --field_delimiter=',' --source_format=CSV --skip_leading_rows=0 --max_bad_records=0  --allow_jagged_rows=1 --allow_quoted_newlines=1 --ignore_unknown_values=1 $v_destination_tbl $v_cloud_storage_path/$v_fileName $v_schema_filepath/$stg_schemaFileName &
#2> "$v_data_object"_final_table_result.txt 
v_pid=$!

wait $v_pid

if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

v_log_obj_txt+=`echo "\n$(date) $v_task_status is the task status for $v_subtask"`;

########################################################################################
## Checking if the Incremental Table Load process has failed. If Failed, then exit this task (script). ##

v_subtask="Staging Table load";
p_exit_upon_error "$v_task_status" "$v_subtask"

#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#
                     ## Completed: Checking for Process Failure ##
#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#


## Bulk Mail Send Events
v_cheetah_query_1="SELECT 'Bulk Mail Send Events' as event_type,
       Event_Type_ID as event_type_id, 
       timestamp as timestamp,
       issueid as issueid, 
       uid as uid, 
       resultcode_ip as resultcode, 
       '<Not Applicable>' as ip,
       mimetype_part as mimetype,
       '<Not Applicable>' as part,
       email_redirect as email,
       '<Not Applicable>' as redirect,
       '<Not Applicable>' as sendtime,
       '<Not Applicable>' as url,
       '<Not Applicable>' as eid,
       '<Not Applicable>' as clicktime,
       '<Not Applicable>' as txid,
       '<Not Applicable>' as amount,
       '<Not Applicable>' as items_val2,
       '<Not Applicable>' as val3,
       '<Not Applicable>' as val4 
FROM [metadata.stg_cheetah] 
WHERE Event_Type_ID = 1";

## Event Based Mail Send Events
v_cheetah_query_2="SELECT 'Event Based Mail Send Events' as event_type,
       Event_Type_ID as event_type_id, 
       timestamp as timestamp,
       issueid as issueid, 
       uid as uid, 
       resultcode_ip as resultcode, 
       '<Not Applicable>' as ip,
       mimetype_part as mimetype,
       '<Not Applicable>' as part,
       email_redirect as email,
       '<Not Applicable>' as redirect,
       sendtime_URL as sendtime,
       '<Not Applicable>' as url,
       eid_clicktime as eid,
       '<Not Applicable>' as clicktime,
       '<Not Applicable>' as txid,
       '<Not Applicable>' as amount,
       '<Not Applicable>' as items_val2,
       '<Not Applicable>' as val3,
       '<Not Applicable>' as val4 
FROM [metadata.stg_cheetah] 
WHERE Event_Type_ID = 2";

## Bulk Mail Bounce Events
v_cheetah_query_3="SELECT 'Bulk Mail Bounce Events' as event_type,
       Event_Type_ID as event_type_id, 
       timestamp as timestamp,
       issueid as issueid, 
       uid as uid, 
       resultcode_ip as resultcode, 
       '<Not Applicable>' as ip,
       '<Not Applicable>' as mimetype,
       '<Not Applicable>' as part,
       '<Not Applicable>' as email,
       '<Not Applicable>' as redirect,
       '<Not Applicable>' as sendtime,
       '<Not Applicable>' as url,
       '<Not Applicable>' as eid,
       '<Not Applicable>' as clicktime,
       '<Not Applicable>' as txid,
       '<Not Applicable>' as amount,
       '<Not Applicable>' as items_val2,
       '<Not Applicable>' as val3,
       '<Not Applicable>' as val4 
FROM [metadata.stg_cheetah] 
WHERE Event_Type_ID = 3";

## Open Events
v_cheetah_query_10="SELECT 'Open Events' as event_type,
       Event_Type_ID as event_type_id, 
       timestamp as timestamp,
       issueid as issueid, 
       uid as uid, 
       '<Not Applicable>' as resultcode, 
       resultcode_ip as ip,
       '<Not Applicable>' as mimetype,
       mimetype_part as part,
       '<Not Applicable>' as email,
       '<Not Applicable>' as redirect,
       '<Not Applicable>' as sendtime,
       '<Not Applicable>' as url,
       '<Not Applicable>' as eid,
       '<Not Applicable>' as clicktime,
       '<Not Applicable>' as txid,
       '<Not Applicable>' as amount,
       '<Not Applicable>' as items_val2,
       '<Not Applicable>' as val3,
       '<Not Applicable>' as val4 
FROM [metadata.stg_cheetah] 
WHERE Event_Type_ID = 10";

## Click Events
v_cheetah_query_20="SELECT 'Click Events' as event_type,
       Event_Type_ID as event_type_id, 
       timestamp as timestamp,
       issueid as issueid, 
       uid as uid, 
       '<Not Applicable>' as resultcode, 
       resultcode_ip as ip,
       '<Not Applicable>' as mimetype,
       mimetype_part as part,
       '<Not Applicable>' as email,
       email_redirect as redirect,
       '<Not Applicable>' as sendtime,
       sendtime_URL as url,
       '<Not Applicable>' as eid,
       '<Not Applicable>' as clicktime,
       '<Not Applicable>' as txid,
       '<Not Applicable>' as amount,
       '<Not Applicable>' as items_val2,
       '<Not Applicable>' as val3,
       '<Not Applicable>' as val4 
FROM [metadata.stg_cheetah] 
WHERE Event_Type_ID = 20";

## Transaction Events
v_cheetah_query_30="SELECT 'Transaction Events' as event_type,
       Event_Type_ID as event_type_id, 
       timestamp as timestamp,
       issueid as issueid, 
       uid as uid, 
       '<Not Applicable>' as resultcode, 
       resultcode_ip as ip,
       '<Not Applicable>' as mimetype,
       mimetype_part as part,
       '<Not Applicable>' as email,
       email_redirect as redirect,
       '<Not Applicable>' as sendtime,
       sendtime_URL as url,
       '<Not Applicable>' as eid,
       eid_clicktime as clicktime,
       txid as txid,
       amount as amount,
       items_val2 as items_val2,
       val3 as val3,
       val4 as val4 
FROM [metadata.stg_cheetah] 
WHERE Event_Type_ID = 30";


## Unsubscribe Events
v_cheetah_query_50="SELECT 'Unsubscribe Events' as event_type,
       Event_Type_ID as event_type_id, 
       timestamp as timestamp,
       issueid as issueid, 
       uid as uid, 
       '<Not Applicable>' as resultcode, 
       '<Not Applicable>' as ip,
       '<Not Applicable>' as mimetype,
       '<Not Applicable>' as part,
       '<Not Applicable>' as email,
       '<Not Applicable>' as redirect,
       '<Not Applicable>' as sendtime,
       '<Not Applicable>' as url,
       '<Not Applicable>' as eid,
       '<Not Applicable>' as clicktime,
       '<Not Applicable>' as txid,
       '<Not Applicable>' as amount,
       '<Not Applicable>' as items_val2,
       '<Not Applicable>' as val3,
       '<Not Applicable>' as val4 
FROM [metadata.stg_cheetah] 
WHERE Event_Type_ID = 50";

# Removing existing table with the day's data being loaded.
bq rm "$v_dataset_name.${tableName}_$v_extract_date";

bq query  --append=1 --allow_large_results=1 --maximum_billing_tier 5 --quiet --destination_table=$v_dataset_name.${tableName}_$v_extract_date $v_cheetah_query_50 & 
v_pid=$!

wait $v_pid

if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

v_log_obj_txt+=`echo "\n$(date) $v_task_status is the task status for $v_subtask"`;

## Checking if the Table Load process has failed. If Failed, then exit this task (script). ##
v_subtask="Unsubscribe Events data load in Final Table";
p_exit_upon_error "$v_task_status" "$v_subtask"

 
bq query  --append=1 --allow_large_results=1 --maximum_billing_tier 5 --quiet --destination_table=$v_dataset_name.${tableName}_$v_extract_date $v_cheetah_query_30 & 
v_pid=$!

wait $v_pid

if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

v_log_obj_txt+=`echo "\n$(date) $v_task_status is the task status for $v_subtask"`;

## Checking if the Table Load process has failed. If Failed, then exit this task (script). ##
v_subtask="Transaction Events data load in Final Table";
p_exit_upon_error "$v_task_status" "$v_subtask"
 
bq query  --append=1 --allow_large_results=1 --maximum_billing_tier 5 --quiet --destination_table=$v_dataset_name.${tableName}_$v_extract_date $v_cheetah_query_20 & 
v_pid=$!

wait $v_pid

if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

v_log_obj_txt+=`echo "\n$(date) $v_task_status is the task status for $v_subtask"`;

## Checking if the Table Load process has failed. If Failed, then exit this task (script). ##
v_subtask="Click Events data load in Final Table";
p_exit_upon_error "$v_task_status" "$v_subtask"
 
bq query  --append=1 --allow_large_results=1 --maximum_billing_tier 5 --quiet --destination_table=$v_dataset_name.${tableName}_$v_extract_date $v_cheetah_query_10 & 
v_pid=$!

wait $v_pid

if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

v_log_obj_txt+=`echo "\n$(date) $v_task_status is the task status for $v_subtask"`;

## Checking if the Table Load process has failed. If Failed, then exit this task (script). ##
v_subtask="Open Mail Events data load in Final Table";
p_exit_upon_error "$v_task_status" "$v_subtask"
 
bq query  --append=1 --allow_large_results=1 --maximum_billing_tier 5 --quiet --destination_table=$v_dataset_name.${tableName}_$v_extract_date $v_cheetah_query_3 & 
v_pid=$!

wait $v_pid

if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

v_log_obj_txt+=`echo "\n$(date) $v_task_status is the task status for $v_subtask"`;

## Checking if the Table Load process has failed. If Failed, then exit this task (script). ##
v_subtask="Bulk Mail Bounce Events data load in Final Table";
p_exit_upon_error "$v_task_status" "$v_subtask"
 
bq query  --append=1 --allow_large_results=1 --maximum_billing_tier 5 --quiet --destination_table=$v_dataset_name.${tableName}_$v_extract_date $v_cheetah_query_2 & 
v_pid=$!

wait $v_pid

if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

v_log_obj_txt+=`echo "\n$(date) $v_task_status is the task status for $v_subtask"`;

## Checking if the Table Load process has failed. If Failed, then exit this task (script). ##
v_subtask="Event Mail Send Events data load in Final Table";
p_exit_upon_error "$v_task_status" "$v_subtask"
 
bq query  --append=1 --allow_large_results=1 --maximum_billing_tier 5 --quiet --destination_table=$v_dataset_name.${tableName}_$v_extract_date $v_cheetah_query_1 & 
v_pid=$!

wait $v_pid

if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

v_log_obj_txt+=`echo "\n$(date) $v_task_status is the task status for $v_subtask"`;

## Checking if the Table Load process has failed. If Failed, then exit this task (script). ##
v_subtask="Bulk Mail Send Events data load in Final Table";
p_exit_upon_error "$v_task_status" "$v_subtask"


###################################################################################
## Storing the status (success/failed) into respective text file. This will be in 
## consumed by the main script to determine the status of entire Extract activity
## and this object's ETL flow
###################################################################################

echo $v_task_status > $v_temp_dir/${v_data_object}_load_status.txt

chmod 0777 $v_temp_dir/${v_data_object}_load_status.txt;

#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#



###################################################################################
## Storing the log for BigQuery table logging in a common CSV file for all tasks ##
###################################################################################

taskEndTime=`date`;

v_task_end_epoch=`date +%s`
v_task_end_ts=`echo $(date -d "@$v_task_end_epoch" +"%Y-%m-%d %r %Z")`;

v_bq_log_tbl_row='';

## Structure of Log row for Object section:
# data_object,etl_task,data_object_task_start_time,data_object_task_start_ts,data_object_task_status,data_object_task_end_epoch,data_object_task_end_ts

v_bq_log_tbl_row="$v_data_object,$v_etl_task,$v_task_start_epoch,$v_task_start_ts,$v_task_status,$v_task_end_epoch,$v_task_end_ts";
echo -e  "CSV Row for Load: \n$v_bq_log_tbl_row"

## Appending the log-table row portion specific to this activity to the main tasks CSV.
echo $v_bq_log_tbl_row >> $v_logs_dir/log_ETL_tasks.csv

#Adding the same row to task's log
v_log_obj_txt+=`echo "\n$(date) Log Table Row: \n$v_bq_log_tbl_row"`;
#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#


v_log_obj_txt+=`echo "\n$v_etl_task process ended for $v_data_object at $v_task_end_ts"`;
v_log_obj_txt+=`echo "\n------------------------------------------------------------"`;
v_log_obj_txt+=`echo "\n------------------------------------------------------------"`;


##############################################################################
## Storing the log text in data object specific log file in temp folder     ##
##############################################################################

# Maintaining the log of this run in a separate file in arch folder
echo -e "$v_log_obj_txt" > $v_arch_dir/logs/"$v_data_object""_load_"$v_task_datetime.log


# Creating new file for cheetah's ETL run. Content will be appended in further tasks of T and L.
echo -e "$v_log_obj_txt" >> $v_temp_dir/"$v_data_object"_log.log

chmod 0777 $v_temp_dir/"$v_data_object"_log.log;
#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#

echo -e "Log text is: \n"
echo -e "$v_log_obj_txt";

exit 0