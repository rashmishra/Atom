#!/bin/bash

## Script Name: transform_playstore.sh


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

        # Creating new file for Google Playstore's ETL run. Content will be appended in further tasks of T and L.
        echo -e "$v_log_obj_txt" > $v_temp_dir/"$v_data_object"_log.log
        chmod 0777 $v_temp_dir/"$v_data_object"_log.log;

        #Exiting with an error-code
        exit 1;
    fi


}


#Copying file from Extract folder to Transform folder
cd $v_transform_dir

echo "Transformation Directory: $v_transform_dir";
echo "Data Extract dump Directory: $v_data_dump_dir";

v_subtask="Playstore Detailed Reports"

## Detailed Reports Conversion
v_report_month=$(date +%Y%m);
v_detailed_rep_pids=""
for i in $PLAY_TRANSFORM_DETAILED_REPORT_NAMES; do

echo "Command used: iconv -f UTF-16 -t UTF-8 ${v_data_dump_dir}/${i}_${v_report_month}.csv -o $v_transform_dir/${i}_${v_report_month}.csv &";
ls -lrth  ${v_data_dump_dir}/${i}_${v_report_month}.csv
iconv -f UTF-16 -t UTF-8 ${v_data_dump_dir}/${i}_${v_report_month}.csv  -o  ${v_transform_dir}/${i}_${v_report_month}.csv &
v_pid=$!
v_detailed_rep_pids+=" $v_pid"
ls -lrth  ${v_transform_dir}/${i}_${v_report_month}.csv
done

if wait $v_detailed_rep_pids; 
    then echo "Playstore Detailed Reports Processes $v_detailed_rep_pids Status: success";
    v_task_status="success";
else 
    echo "Playstore Detailed Reports Processes $v_detailed_rep_pids Status: failed";
    v_task_status="failed";
fi
p_exit_upon_error "$v_task_status" "$v_subtask"

echo "Completed Detailed Reports Conversion"


## Aggregate Reports
## Crashes
v_subtask="Playstore Aggregated Reports"

v_agg_crash_rep_pids="";
for i in $PLAY_AGGREGATED_CRASHES_REPORT_NAMES; do
    echo "Command used: iconv -f UTF-16 -t UTF-8 ${v_data_dump_dir}/${PLAY_AGGREGATED_CRASHES_REPORT_PREFIX}_${v_report_month}_${i}.csv -o $v_transform_dir/${PLAY_AGGREGATED_CRASHES_REPORT_PREFIX}_${v_report_month}_${i}.csv &";
    iconv -f UTF-16 -t UTF-8 ${v_data_dump_dir}/${PLAY_AGGREGATED_CRASHES_REPORT_PREFIX}_${v_report_month}_${i}.csv -o $v_transform_dir/${PLAY_AGGREGATED_CRASHES_REPORT_PREFIX}_${v_report_month}_${i}.csv &
    v_pid=$!
    v_agg_crash_rep_pids+=" $v_pid"
done

if wait $v_agg_crash_rep_pids; 
    then echo "Playstore Aggregate Crashes Reports Processes $v_agg_crash_rep_pids Status: success";
    v_task_status="success";
else 
    echo "Playstore Aggregate Crashes Reports Processes $v_agg_crash_rep_pids Status: failed";
    v_task_status="failed";
fi
p_exit_upon_error "$v_task_status" "$v_subtask"

echo "Completed AGgregated Reports Conversion: Crashes"

## GCM
v_agg_gcm_rep_pids="";
for i in $PLAY_AGGREGATED_GCM_REPORT_NAMES; do
    iconv -f UTF-16 -t UTF-8 ${v_data_dump_dir}/${PLAY_AGGREGATED_GCM_REPORT_PREFIX}_${v_report_month}_${i}.csv -o $v_transform_dir/${PLAY_AGGREGATED_GCM_REPORT_PREFIX}_${v_report_month}_${i}.csv &
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

echo "Completed AGgregated Reports Conversion: GCM"

## Installs
v_agg_installs_rep_pids="";
for i in $PLAY_AGGREGATED_INSTALLS_REPORT_NAMES; do
    iconv -f UTF-16 -t UTF-8 ${v_data_dump_dir}/${PLAY_AGGREGATED_INSTALLS_REPORT_PREFIX}_${v_report_month}_${i}.csv -o $v_transform_dir/${PLAY_AGGREGATED_INSTALLS_REPORT_PREFIX}_${v_report_month}_${i}.csv &
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

echo "Completed AGgregated Reports Conversion: Installs"

## Ratings
v_agg_ratings_rep_pids="";
for i in $PLAY_AGGREGATED_RATINGS_REPORT_NAMES; do
    iconv -f UTF-16 -t UTF-8 ${v_data_dump_dir}/${PLAY_AGGREGATED_RATINGS_REPORT_PREFIX}_${v_report_month}_${i}.csv -o $v_transform_dir/${PLAY_AGGREGATED_RATINGS_REPORT_PREFIX}_${v_report_month}_${i}.csv &
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


## Adding the Dimension name in the AGgregated Report names
# Crashes
v_agg_crash_rep_pids="";
for i in $PLAY_AGGREGATED_CRASHES_REPORT_NAMES; do
    echo "Command used: sed -i -e \"s/^/$i,/\" $v_transform_dir/${PLAY_AGGREGATED_CRASHES_REPORT_PREFIX}_${v_report_month}_${i}.csv &";
    sed -i -e "s/^/$i,/" $v_transform_dir/${PLAY_AGGREGATED_CRASHES_REPORT_PREFIX}_${v_report_month}_${i}.csv 
    v_pid=$!
    v_agg_crash_rep_pids+=" $v_pid"
done

if wait $v_agg_crash_rep_pids; 
    then echo "Playstore Aggregate Crashes Reports Dimension name addition Processes $v_agg_crash_rep_pids Status: success";
    v_task_status="success";
else 
    echo "Playstore Aggregate Crashes Reports Dimension name addition Processes $v_agg_crash_rep_pids Status: failed";
    v_task_status="failed";
fi
p_exit_upon_error "$v_task_status" "$v_subtask"

echo "Completed AGgregated Reports Dimension Name ADdition: Crashes"

## GCM
v_agg_gcm_rep_pids="";
for i in $PLAY_AGGREGATED_GCM_REPORT_NAMES; do
    echo "Command used: sed -i -e \"s/^/$i,/\" $v_transform_dir/${PLAY_AGGREGATED_GCM_REPORT_PREFIX}_${v_report_month}_${i}.csv  &";
    sed -i -e "s/^/$i,/" $v_transform_dir/${PLAY_AGGREGATED_GCM_REPORT_PREFIX}_${v_report_month}_${i}.csv &
    v_pid=$!
    v_agg_gcm_rep_pids+=" $v_pid"
done

if wait $v_agg_gcm_rep_pids; 
    then echo "Playstore Aggregate GCM Reports Dimension name addition Processes $v_agg_gcm_rep_pids Status: success";
    v_task_status="success";
else 
    echo "Playstore Aggregate GCM Reports Dimension name addition Processes $v_agg_gcm_rep_pids Status: failed";
    v_task_status="failed";
fi
p_exit_upon_error "$v_task_status" "$v_subtask"

echo "Completed AGgregated Reports Dimension Name ADdition: GCM"

## Installs
v_agg_installs_rep_pids="";
for i in $PLAY_AGGREGATED_INSTALLS_REPORT_NAMES; do
    echo "Command used: sed -i -e "s/^/$i,/" $v_transform_dir/${PLAY_AGGREGATED_INSTALLS_REPORT_PREFIX}_${v_report_month}_${i}.csv  &"
    sed -i -e "s/^/$i,/" $v_transform_dir/${PLAY_AGGREGATED_INSTALLS_REPORT_PREFIX}_${v_report_month}_${i}.csv  &
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

echo "Completed AGgregated Reports Conversion: Installs"

## Ratings
v_agg_ratings_rep_pids="";
for i in $PLAY_AGGREGATED_RATINGS_REPORT_NAMES; do
    echo "Command used: sed -i -e \"s/^/$i,/\" $v_transform_dir/${PLAY_AGGREGATED_RATINGS_REPORT_PREFIX}_${v_report_month}_${i}.csv  &"
    sed -i -e "s/^/$i,/" $v_transform_dir/${PLAY_AGGREGATED_RATINGS_REPORT_PREFIX}_${v_report_month}_${i}.csv  &
    
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





## Removing Downloaded files from extract folder
# Detailed Reports
v_report_month=$(date +%Y%m);
v_detailed_rep_pids=""
for i in $PLAY_TRANSFORM_DETAILED_REPORT_NAMES; do
    rm ${v_data_dump_dir}/${i}_${v_report_month}.csv 
done



## Aggregate Reports
## Crashes
v_subtask="Playstore Aggregated Reports"
v_agg_crash_rep_pids="";
for i in $PLAY_AGGREGATED_CRASHES_REPORT_NAMES; do
    rm ${v_data_dump_dir}/${PLAY_AGGREGATED_CRASHES_REPORT_PREFIX}_${v_report_month}_${i}.csv 
done


## GCM
v_agg_gcm_rep_pids="";
for i in $PLAY_AGGREGATED_GCM_REPORT_NAMES; do
    rm ${v_data_dump_dir}/${PLAY_AGGREGATED_GCM_REPORT_PREFIX}_${v_report_month}_${i}.csv 
done


## Installs
v_agg_installs_rep_pids=""
for i in $PLAY_AGGREGATED_INSTALLS_REPORT_NAMES; do
    rm ${v_data_dump_dir}/${PLAY_AGGREGATED_INSTALLS_REPORT_PREFIX}_${v_report_month}_${i}.csv 
done


## Ratings
v_agg_ratings_rep_pids=""
for i in $PLAY_AGGREGATED_RATINGS_REPORT_NAMES; do
    rm ${v_data_dump_dir}/${PLAY_AGGREGATED_RATINGS_REPORT_PREFIX}_${v_report_month}_${i}.csv 
done
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
