#!/bin/bash

## Script Name: export_playstore.sh
## Purpose: Modular ETL flow of Atom.

##### $1: Data Object. ####
##### $2: Data dump location. ####
##### $3: Mongo cp directory. ####
##### $4: Incremental Epoch. ####
##### $5: ETL Home directory. ####

. /home/ubuntu/modular_ETL/config/masterenv.sh

taskStartTime=`date`

v_task_start_epoch=`date +%s`
v_task_start_ts=`echo $(date -d "@$v_task_start_epoch" +"%Y-%m-%d %r %Z")`

v_task_datetime=`echo $(date -d "@$v_task_start_epoch" +"%Y-%m-%d_%H:%M_%Z")`

## Initializing required variables
v_etl_task='extract'
v_data_object=$1;
v_data_dump_dir=$2
v_mondo_dir=$3;
#v_incremental_epoch=$4;
v_incremental_epoch=$(($4*1000));
v_temp_dir=$5/temp;
v_logs_dir=$5/logs;

v_task_status='Not set yet';
v_log_obj_txt+=`echo "\n------------------------------------------------------------"`;
v_log_obj_txt+=`echo "\n------------------------------------------------------------"`;
v_log_obj_txt+=`echo "\n$v_etl_task process started for $v_data_object at $v_task_start_ts"`;

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
        echo -e  "CSV Row for extract: \n$v_bq_log_tbl_row"
        echo $v_bq_log_tbl_row >> $v_logs_dir/log_ETL_tasks.csv
        
        v_log_obj_txt+=`echo "\n$(date) Log Table Row: \n$v_bq_log_tbl_row"`;

        ## Writing the status (success/ failure) into proper file

        echo $v_task_status > $v_temp_dir/${v_data_object}_extract_status.txt
        chmod 0777 $v_temp_dir/${v_data_object}_extract_status.txt;

        ## Writing the log of this task to files

        v_log_obj_txt+=`echo "\n$v_etl_task process ended for $v_data_object at $v_task_end_ts"`;
        v_log_obj_txt+=`echo "\n------------------------------------------------------------"`;
        v_log_obj_txt+=`echo "\n------------------------------------------------------------"`;

        # Maintaining the log of this run in a separate file in arch folder
        echo -e "$v_log_obj_txt" > $v_arch_dir/logs/"$v_data_object""_extract_"$v_task_datetime.log


        # Creating new file for playstore's ETL run. Content will be appended in further tasks of T and L.
        echo -e "$v_log_obj_txt" > $v_temp_dir/"$v_data_object"_log.log

        chmod 0777 $v_temp_dir/"$v_data_object"_log.log;

        

        exit 1;
    fi


}
## Place your extraction code here 

cd $v_data_dump_dir;

v_subtask="Playstore Detailed Reports"

## Detailed Reports Download
#v_report_month=$(date +%Y%m);

v_report_month=$(date -d '- 0days' +%Y%m);
v_prev_report_month=$(date -d '- 1 month' +%Y%m);

v_detailed_rep_pids=""
for i in $PLAY_DETAILED_REPORT_NAMES; do
gsutil -m cp ${PLAY_BUCKET}/${i}_${v_report_month}.csv ./ 
gsutil -m cp ${PLAY_BUCKET}/${i}_${v_prev_report_month}.csv ./ &
v_pid=$!
v_detailed_rep_pids+=" $v_pid"
done

if wait $v_detailed_rep_pids; 
    then echo "Playstore Detailed Reports Processes $v_detailed_rep_pids Status: success";
    v_task_status="success";
else 
    echo "Playstore Detailed Reports Processes $v_detailed_rep_pids Status: failed";
    v_task_status="failed";
fi
p_exit_upon_error "$v_task_status" "$v_subtask"

## Aggregate Reports
## Crashes
v_subtask="Playstore Aggregated Reports"

v_agg_crash_rep_pids="";
for i in $PLAY_AGGREGATED_CRASHES_REPORT_NAMES; do 
     echo $i;
     sleep 2;
    gsutil -m cp ${PLAY_AGGREGATED_CRASHES_REPORT_FOLDER}/${PLAY_AGGREGATED_CRASHES_REPORT_PREFIX}_${v_report_month}_${i}.csv  ./
    gsutil -m cp ${PLAY_AGGREGATED_CRASHES_REPORT_FOLDER}/${PLAY_AGGREGATED_CRASHES_REPORT_PREFIX}_${v_prev_report_month}_${i}.csv ./ &
    v_pid=$!
    echo "PID for $i is : $v_pid"
    v_agg_crash_rep_pids+=" $v_pid";
    sleep 1;
done

if wait $v_agg_crash_rep_pids; 
    then echo "Playstore Aggregate Crashes Reports Processes $v_agg_crash_rep_pids Status: success";
    v_task_status="success";
else 
    echo "Playstore Aggregate Crashes Reports Processes $v_agg_crash_rep_pids Status: failed";
    v_task_status="failed";
fi
p_exit_upon_error "$v_task_status" "$v_subtask"


## GCM
v_agg_gcm_rep_pids="";
for i in $PLAY_AGGREGATED_GCM_REPORT_NAMES; do
    gsutil -m cp ${PLAY_AGGREGATED_GCM_REPORT_FOLDER}/${PLAY_AGGREGATED_GCM_REPORT_PREFIX}_${v_report_month}_${i}.csv  ./
    gsutil -m cp ${PLAY_AGGREGATED_GCM_REPORT_FOLDER}/${PLAY_AGGREGATED_GCM_REPORT_PREFIX}_${v_prev_report_month}_${i}.csv ./ &
    v_pid=$!
    v_agg_gcm_rep_pids+=" $v_pid"
done

if wait $v_agg_gcm_rep_pids; 
    then echo "Playstore Aggregate GCM Reports Processes $v_agg_gcm_rep_pids Status: success";
    v_task_status="success";
else 
    echo "Playstore Aggregate GCM Reports Processes $v_agg_gcm_rep_pids Status: failed";
    v_task_status="failed";
fi
p_exit_upon_error "$v_task_status" "$v_subtask"

## Installs
for i in $PLAY_AGGREGATED_INSTALLS_REPORT_NAMES; do
    gsutil -m cp ${PLAY_AGGREGATED_INSTALLS_REPORT_FOLDER}/${PLAY_AGGREGATED_INSTALLS_REPORT_PREFIX}_${v_report_month}_${i}.csv ./ 
    gsutil -m cp ${PLAY_AGGREGATED_INSTALLS_REPORT_FOLDER}/${PLAY_AGGREGATED_INSTALLS_REPORT_PREFIX}_${v_prev_report_month}_${i}.csv ./ &
    v_pid=$!
    v_agg_installs_rep_pids+=" $v_pid"
done

if wait $v_agg_installs_rep_pids; 
    then echo "Playstore Aggregate Installs Reports Processes $v_agg_gcm_rep_pids Status: success";
    v_task_status="success";
else 
    echo "Playstore Aggregate Installs Reports Processes $v_agg_installs_rep_pids Status: failed";
    v_task_status="failed";
fi
p_exit_upon_error "$v_task_status" "$v_subtask"

## Ratings
for i in $PLAY_AGGREGATED_RATINGS_REPORT_NAMES; do
    gsutil -m cp ${PLAY_AGGREGATED_RATINGS_REPORT_FOLDER}/${PLAY_AGGREGATED_RATINGS_REPORT_PREFIX}_${v_report_month}_${i}.csv ./
    gsutil -m cp ${PLAY_AGGREGATED_RATINGS_REPORT_FOLDER}/${PLAY_AGGREGATED_RATINGS_REPORT_PREFIX}_${v_prev_report_month}_${i}.csv ./ &
    v_pid=$!
    v_agg_ratings_rep_pids+=" $v_pid"
done

if wait $v_agg_ratings_rep_pids; 
    then echo "Playstore Aggregate Ratings Reports Processes $v_agg_ratings_rep_pids Status: success";
    v_task_status="success";
else 
    echo "Playstore Aggregate Ratings Reports Processes $v_agg_ratings_rep_pids Status: failed";
    v_task_status="failed";
fi
p_exit_upon_error "$v_task_status" "$v_subtask"

## Completed: Place your extraction code here 


# Init end time for logging purpose
taskEndTime=`date`;

v_task_end_epoch=`date +%s`
v_task_end_ts=`echo $(date -d "@$v_task_end_epoch" +"%Y-%m-%d %r %Z")`;

###################################################################################
## Storing the log for BigQuery table logging in a common CSV file for all tasks ##

v_bq_log_tbl_row='';

## Structure of Log row for Object section:
# data_object,etl_task,data_object_task_start_time,data_object_task_start_ts,data_object_task_status,data_object_task_end_epoch,data_object_task_end_ts

v_bq_log_tbl_row="$v_data_object,$v_etl_task,$v_task_start_epoch,$v_task_start_ts,$v_task_status,$v_task_end_epoch,$v_task_end_ts";

## Appending the log-table row portion specific to this activity to the main tasks CSV.
echo $v_bq_log_tbl_row >> $v_logs_dir/log_ETL_tasks.csv

#Adding the same row to task's log
v_log_obj_txt+=`echo "\n Log Table Row: \n$v_bq_log_tbl_row"`;
###################################################################################

###################################################################################
## Storing the status (success/failed) into respective text file. This will be in 
## consumed by the main script to determine the status of entire Extract activity
## and this object's ETL flow


echo $v_task_status > $v_temp_dir/${v_data_object}_extract_status.txt

###################################################################################

taskEndTime=`date`;

v_task_end_epoch=`date +%s`
v_task_end_ts=`echo $(date -d "@$v_task_end_epoch" +"%Y-%m-%d %r %Z")`;

v_log_obj_txt+=`echo "\n$v_etl_task process ended for $v_data_object at $v_task_end_ts"`;
v_log_obj_txt+=`echo "\n------------------------------------------------------------"`;
v_log_obj_txt+=`echo "\n------------------------------------------------------------"`;


##############################################################################
## Storing the log text in data object specific log file                    ##

# Maintaining the log of this run in a separate file.
echo -e "$v_log_obj_txt" > $v_temp_dir/"$v_data_object""_extract_"$v_task_datetime.log

# Removing the previous run's file from the directory
v_log_obj_txt+=`rm $v_logs_dir/"$v_data_object"_log.log`;

# Creating new file for playstore's ETL run. Content will be appended in further tasks of T and L.
echo -e "$v_log_obj_txt" > $v_logs_dir/"$v_data_object"_log.log
##############################################################################


echo -e "Log text is: \n"
echo -e "$v_log_obj_txt";

echo "playstore data export end time is : $taskEndTime "
exit 0