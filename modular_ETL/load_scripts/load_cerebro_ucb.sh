#!/bin/bash

## Script Name: load_cerebro_ucb.sh
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

## Initializing required variables
v_etl_task='load'

schemaFileName=schema_cerebro_ucb.json
maxBadRecords=0

v_data_object=$1;
tableName=ucb;
v_fileName="$1.json.gz";
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

v_task_status='Not set yet';
v_log_obj_txt+=`echo "\n------------------------------------------------------------"`;
v_log_obj_txt+=`echo "\n------------------------------------------------------------"`;
v_log_obj_txt+=`echo "\n$v_etl_task process started for $v_data_object at $v_task_start_ts"`;


echo "In cerebro_ucb Loading script";

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
        echo " $v_task_status > $v_temp_dir/${v_data_object}_load_status.txt"
        echo $v_task_status > $v_temp_dir/${v_data_object}_load_status.txt
        chmod 0777 $v_temp_dir/${v_data_object}_load_status.txt;

        ## Writing the log of this task to files

        v_log_obj_txt+=`echo "\n$v_etl_task process ended for $v_data_object at $v_task_end_ts"`;
        v_log_obj_txt+=`echo "\n------------------------------------------------------------"`;
        v_log_obj_txt+=`echo "\n------------------------------------------------------------"`;

        # Maintaining the log of this run in a separate file in arch folder
        echo -e "$v_log_obj_txt" > $v_arch_dir/logs/"$v_data_object""_load_"$v_task_datetime.log


        # Creating new file for cerebro_ucb's ETL run. Content will be appended in further tasks of T and L.
        echo -e "$v_log_obj_txt" >> $v_temp_dir/"$v_data_object"_log.log

        chmod 0777 $v_temp_dir/"$v_data_object"_log.log;

        

        exit 1;

    fi

}

# Fetching the data file from Transform Directory to Load Directory
cd $v_transform_dir;
pwd
mv "$v_data_object".json.gz $v_load_dir

cd $v_load_dir;
echo "In Load directory, from cerebro_ucb script $v_load_dir";
v_log_obj_txt+=`echo "\n$(date) In Load directory $v_load_dir"`;

########################################################################################
                             ## Loading into gcloud ##
########################################################################################
gsutil cp $v_fileName $v_cloud_storage_path 2> "$v_data_object"_cloud_result.txt &
v_pid=$!

wait $v_pid

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

# rm "$v_data_object"_cloud_result.txt

#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#
                     ## Completed: Checking for Process Failure ##
#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#

## Load into metadata

## Make the diff table, with the uploaded file (for incremental load)

#v_incr_table_result=`echo $(bq load --quiet  --source_format=NEWLINE_DELIMITED_JSON --replace --ignore_unknown_values=1 --max_bad_records=$maxBadRecords $v_metadataset_name.incremental_$tableName $v_cloud_storage_path/$v_fileName $v_schema_filepath/$schemaFileName 2>&1)`
echo "Etl Home is $6."
echo "Schema File path is: $v_schema_filepath"

v_destination_tbl="$v_metadataset_name.incremental_$tableName";
bq load --quiet --source_format=NEWLINE_DELIMITED_JSON --replace --ignore_unknown_values=1 --max_bad_records=$maxBadRecords $v_destination_tbl $v_cloud_storage_path/$v_fileName $v_schema_filepath/$schemaFileName &
# 2> "$v_data_object"_inc_table_result.txt
v_pid=$!

wait $v_pid

if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi


v_log_obj_txt+=`echo "\n$(date) $v_task_status is the task status"`;

########################################################################################
## Checking if the prior process has failed. If Failed, then exit this task (script). ##

v_subtask="Incremental Table load";
p_exit_upon_error "$v_task_status" "$v_subtask"

#rm "$v_data_object"_inc_table_result.txt

#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#
                     ## Completed: Checking for Process Failure ##
#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#

if [[ "`bq ls --max_results=10000 $v_dataset_name | awk '{print $1}' | grep \"\b$tableName\b\"`" == "$tableName" ]] ;
    then 
        ## Make another table with prior (till last run) data 
        # v_query="SELECT base.*
        #         FROM $v_dataset_name.$tableName  base 
        #         LEFT OUTER JOIN  $v_metadataset_name.incremental_$tableName inc
        #             ON inc.customerId = base.customerId
        #             AND inc.time = base.time
        #         WHERE inc.time IS NULL";

        v_query="SELECT * FROM $v_dataset_name.$tableName 
                 WHERE cid NOT IN (SELECT COALESCE(cid, 0) FROM $v_metadataset_name.incremental_$tableName)";
        v_destination_tbl="$v_metadataset_name.prior_$tableName";
        echo "Destination table is $v_destination_tbl and Query is $v_query"
        bq query  --maximum_billing_tier 150 --allow_large_results=1 -n 1 --noflatten_results --replace --destination_table=$v_destination_tbl "$v_query" 2> "$v_data_object"_prior_table_result.txt &
        v_pid=$!

wait $v_pid

if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

v_log_obj_txt+=`echo "\n$(date) $v_task_status is the task status"`;

        ########################################################################################
        ## Checking if the prior process has failed. If Failed, then exit this task (script). ##

        v_subtask="Prior Data table creation";
        p_exit_upon_error "$v_task_status" "$v_subtask"

        rm "$v_data_object"_prior_table_result.txt

        #-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#
                             ## Completed: Checking for Process Failure ##
        #-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#
    else echo "Table $v_dataset_name.$tableName missing"; 
fi

## Drop Final (e.g. Atom) table
#bq query --append=1 --flatten_results=0 --allow_large_results=1 --destination_table=$METADATA_DATASET_NAME.final_customer 'select * from '"$METADATA_DATASET_NAME"'.customer_incremental' > /dev/null

v_destination_tbl="$v_metadataset_name.prior_$tableName";
v_query="SELECT * FROM $v_metadataset_name.incremental_$tableName";
bq query --maximum_billing_tier 150 --append=1 --flatten_results=0 --allow_large_results=1 -n 1 --destination_table=$v_destination_tbl "$v_query" 2> "$v_data_object"_table_union_result.txt &
v_pid=$!

wait $v_pid

if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi


v_table_union_result=`cat "$v_data_object"_table_union_result.txt`;

v_log_obj_txt+=`echo "\n$(date) Prior data table creation log: "`;
v_log_obj_txt+=`echo "\n$(date) $v_table_union_result \n$v_task_status is the task status"`;

########################################################################################
## Checking if the prior process has failed. If Failed, then exit this task (script). ##

v_subtask="Prior n Incr. Table Union";
p_exit_upon_error "$v_task_status" "$v_subtask"

#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#
                     ## Completed: Checking for Process Failure ##
#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#-X-#

## Load prior (till last run) data + diff table into final table

bq rm -f $v_dataset_name.$tableName
bq cp $v_metadataset_name.prior_$tableName $v_dataset_name.$tableName
# Removing Prior and Incremental tables
bq rm -f $v_metadataset_name.prior_$tableName
bq rm -f $v_metadataset_name.incremental_$tableName



###################################################################################
## Storing the status (success/failed) into respective text file. This will be in 
## consumed by the main script to determine the status of entire Extract activity
## and this object's ETL flow
###################################################################################
echo "cerebro_ucb's Load status is: $v_task_status" 
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


# Creating new file for cerebro_ucb's ETL run. Content will be appended in further tasks of T and L.
echo -e "$v_log_obj_txt" >> $v_temp_dir/"$v_data_object"_log.log

chmod 0777 $v_temp_dir/"$v_data_object"_log.log;
#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#

echo -e "Log text is: \n"
echo -e "$v_log_obj_txt";


exit 0
