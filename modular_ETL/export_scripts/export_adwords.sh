#!/bin/bash


## Script Name: export_adwords.sh
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
v_mongo_dir=$3;
v_incremental_epoch=$4;
v_temp_dir=$5/temp;
v_logs_dir=$5/logs;
v_arch_dir=$5/arch;
v_scripts_dir=$5/export_scripts

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

        # Creating new file for adwords's ETL run. Content will be appended in further tasks of T and L.
        echo -e "$v_log_obj_txt" > $v_temp_dir/"$v_data_object"_log.log
        chmod 0777 $v_temp_dir/"$v_data_object"_log.log;

        #Exiting with an error-code
        exit 1;
    fi


}

cd $v_scripts_dir

# python extract_adwords.py "$v_data_dump_dir" &

for i in ${ADWORDS_YAML_FILENAMES[@]} ; do 
   cp $CONFIG_DIR/$i $HOME_DIR/googleads.yaml
    
    python extract_adwords.py &
    v_extract_pid=$!

    if wait $v_extract_pid; then
        echo "Process $v_extract_pid Status: success";
        v_task_status="success";
    else 
        echo "Process $v_extract_pid Status: failed";
        v_task_status="failed";
    fi

    echo "Completed downloading Reports for account associated with $i file"

done



v_log_obj_txt+=`echo "\n$(date) $v_task_status is the task status. \n"`;

v_subtask="API export";
p_exit_upon_error "$v_task_status" "$v_subtask"

# Zipping the exported data file
declare -a v_arr_reportnames=("account_performance_report" "adgroup_performance_report" "ad_performance_report" "campaign_performance_report" "keywords_performance_report");
## Looping for each Google ADwords report
for i in "${v_arr_reportnames[@]}"
do

cpulimit -l 80 gzip -f $v_data_dump_dir/"$i".csv	
done 


###################################################################################
## Storing the status (success/failed) into respective text file. This will be in 
## consumed by the main script to determine the status of entire Extract activity
## and this object's ETL flow
###################################################################################

echo $v_task_status > $v_temp_dir/${v_data_object}_extract_status.txt
chmod 0777 $v_temp_dir/${v_data_object}_extract_status.txt;

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

##############################################################################
## Storing the log text in data object specific log file in temp folder     ##
##############################################################################

# Maintaining the log of this run in a separate file in arch folder
echo -e "$v_log_obj_txt" > $v_arch_dir/logs/"$v_data_object""_extract_"$v_task_datetime.log


# Removing the previous run's file from the directory
rm $v_temp_dir/"$v_data_object"_log.log

# Creating new file for adwords's ETL run. Content will be appended in further tasks of T and L.
echo -e "$v_log_obj_txt" > $v_temp_dir/"$v_data_object"_log.log
chmod 0777 $v_temp_dir/"$v_data_object"_log.log
#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#

echo -e "Log text is: \n"
echo -e "$v_log_obj_txt";


#  Removing the Command's (mongoexport) output stored in a file
rm $v_temp_dir/"$v_data_object"_extract_command_output.txt

#echo "adwords mongo export end time is : $taskEndTime "

exit 0