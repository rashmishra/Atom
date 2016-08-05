#!/bin/bash

## Script Name: transform_payment_gateways.sh
## This scripts splits data Payment Gateway data based on PG flags


# Parameters: 
#             #### $1: Data Object. ####
#             #### $2: Data dump directory. ####
#             #### $3: Data Transform directory. ####
#             #### $4: Data Load directory. ####
#             #### $5: ETL Home Directory. ####


. /home/ubuntu/modular_ETL/config/masterenv.sh

# Variables' initialization
v_etl_task='transform';
v_data_object=$1;
v_data_dump_dir=$2
v_transform_dir=$3;
v_load_dir=$4;
v_temp_dir=$5/temp;
v_logs_dir=$5/logs;
v_arch_dir=$5/arch;

v_filename=$v_data_object.json

taskStartTime=`date`
v_task_start_epoch=`date +%s`
v_task_start_ts=`echo $(date -d "@$v_task_start_epoch" +"%Y-%m-%d %r %Z")`;

v_task_datetime=`echo $(date -d "@$v_task_start_epoch" +"%Y-%m-%d_%H:%M_%Z")`;


v_task_status='Not set yet';
v_log_obj_txt+=`echo "\n-----------------------------------------------------------------------"`;
v_log_obj_txt+=`echo "\n-----------------------------------------------------------------------"`;
v_log_obj_txt+=`echo "\n$(date)\n$v_etl_task process started for $v_data_object at $v_task_start_ts"`;


## Function to check task status and exit if error occurs.
p_exit_upon_error(){

    ## Parameters
    # $1: Task Status (passed/ failed)
    # $2: Sub Task (e.g. Extraction of data, Cloud upload, metadata table creation, final table population)

    v_task_status="$1";
    v_subtask="$2";


    if [ $v_task_status == "failed" ] ; 
    then
        v_log_obj_txt+=`echo "\n$(date) $(date) Task ($v_subtask) failed for $v_data_object. Hence exiting."`;

        taskEndTime=`date`;
        v_task_end_epoch=`date +%s`
        v_task_end_ts=`echo $(date -d "@$v_task_end_epoch" +"%Y-%m-%d %r %Z")`;
        v_bq_log_tbl_row='';

        ## Writing (appending) the CSV log table row into respective file
        v_bq_log_tbl_row="$v_data_object,$v_etl_task,$v_task_start_epoch,$v_task_start_ts,$v_task_status,$v_task_end_epoch,$v_task_end_ts";
        echo -e  "CSV Row for Transform: \n$v_bq_log_tbl_row"
        echo $v_bq_log_tbl_row >> $v_logs_dir/log_ETL_tasks.csv
        
        v_log_obj_txt+=`echo "\n$(date) Log Table Row: \n$v_bq_log_tbl_row"`;

        ## Writing the status (success/ failure) into proper file
        echo $v_task_status > $v_temp_dir/${v_data_object}_transform_status.txt
        chmod 0777 $v_temp_dir/${v_data_object}_transform_status.txt;

        ## Writing the log of this task to files
        v_log_obj_txt+=`echo "\n$v_etl_task process ended for $v_data_object at $v_task_end_ts"`;
        v_log_obj_txt+=`echo "\n------------------------------------------------------------"`;
        v_log_obj_txt+=`echo "\n------------------------------------------------------------"`;

        # Maintaining the log of this run in a separate file in arch folder
        echo -e "$v_log_obj_txt" > $v_arch_dir/logs/"$v_data_object""_transform_"$v_task_datetime.log

        # Creating new file for payment_gateways's ETL run. Content will be appended in further tasks of T and L.
        echo -e "$v_log_obj_txt" > $v_temp_dir/"$v_data_object"_log.log
        chmod 0777 $v_temp_dir/"$v_data_object"_log.log;

        #Exiting with an error-code
        exit 1;
    fi


}


echo "Extract data directory: $v_data_dump_dir";
echo "Transformation data directory: $v_transform_dir";
echo "PG filename being transformed: $v_filename.gz";

#Copying file from Extract folder to Transform folder
cd $v_data_dump_dir
ls 
echo "Transformation mein move Command: mv $v_filename.gz $v_transform_dir";
mv $v_filename.gz $v_transform_dir

cd $v_transform_dir

# Unzipping the extract for transformation
gzip -d $v_filename.gz
ls 

echo "cat $v_filename | grep \"paymentMode\" \: \"1\" > $PAYU_FILE_NAME"

# Transformation commands
v_transform_pids="";
# PayU 
cat $v_filename | grep "paymentMode\" \: \"1" > $PAYU_FILE_NAME &
v_transform_pids+=" $!";

# PayTM
cat $v_filename | grep "paymentMode\" \: \"2" > $PAYTM_FILE_NAME &
v_transform_pids+=" $!";

# Mobikwik
cat $v_filename | grep "paymentMode\" \: \"3" > $MOBIKWIK_FILE_NAME &
v_transform_pids+=" $!";


# Waiting for the process to complete
if wait $v_transform_pids; then
    echo "Process $v_transform_pids Status: success";
    v_task_status="success";
else 
    echo "Process $v_transform_pids Status: failed";
    v_task_status="failed";
fi

v_subtask="Splitting Payment Gateways data";
p_exit_upon_error "$v_task_status" "$v_subtask"

# gzip -f $v_filename;
# Removing the Raw PG Transactions data file as it is no longer required
rm $v_filename;

# Compressing individual PG files

gzip -f $PAYU_FILE_NAME;
gzip -f $PAYTM_FILE_NAME; 
gzip -f $MOBIKWIK_FILE_NAME;

###################################################################################
## Storing the status (success/failed) into respective text file. This will be in 
## consumed by the main script to determine the status of entire Transform activity
## and this object's ETL flow
###################################################################################

echo $v_task_status > $v_temp_dir/${v_data_object}_transform_status.txt
chmod 0777 $v_temp_dir/${v_data_object}_transform_status.txt;

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

## Appending the log-table row portion specific to this activity to the main tasks CSV.
echo $v_bq_log_tbl_row >> $v_logs_dir/log_ETL_tasks.csv

#Adding the same row to task's log
v_log_obj_txt+=`echo "\n$(date) Log Table Row: \n$v_bq_log_tbl_row"`;
#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#



v_log_obj_txt+=`echo "\n$(date)\n$v_etl_task process ended for $v_data_object at $v_task_end_ts"`;
v_log_obj_txt+=`echo "\n-----------------------------------------------------------------------"`;
v_log_obj_txt+=`echo "\n-----------------------------------------------------------------------"`;

exit 0