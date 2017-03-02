#!/bin/bash

###############################################################################
## Script Name: atom_etl_wrapper.sh
## Author: Ranganath
## Date: 01-Mar-2017
## Purpose: Call the scripts of ETL and then BOM transformation for backward compatible orderline

###############################################################################

v_run_date=`date "+%Y-%m-%d"`

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


        # Creating new file for Order Header's ETL run. Content will be appended in further tasks of T and L.
        echo -e "$v_log_obj_txt" >> $v_temp_dir/"$v_data_object"_log.log

        chmod 0777 $v_temp_dir/"$v_data_object"_log.log;

        

        exit 1;

    fi

}


bash /home/ubuntu/modular_ETL/master_scripts/master_ETL.sh >> /home/ubuntu/modular_ETL/arch/logs/modular_log-${v_run_date}.log 2>&1
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Atom ETL load Status called from Wrapper: $v_task_status";


v_subtask="Atom ETL load";
p_exit_upon_error "$v_task_status" "$v_subtask";



bash /home/ubuntu/modular_ETL/master_scripts/transform_bom_to_orderline.sh >> /home/ubuntu/modular_ETL/master_scripts/log_transform_bom_to_orderline_${v_run_date}.log 2>&1
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Transform BOM to orderline Status called from Wrapper: $v_task_status";


v_subtask="Transform BOM to orderline";
p_exit_upon_error "$v_task_status" "$v_subtask";



if [[ "$v_status" = "success" ]]; 
  then 
    echo `date` "Task: #v_subtask ended with status $v_task_status." | mail -s "Wrapper script calling ETL and Transform OL completed: $v_task_status" sairanganath.v@nearbuy.com;
  else 
    echo `date` "Task: #v_subtask ended with status $v_task_status." | mail -s "Wrapper script calling ETL and Transform OL completed: $v_task_status" sairanganath.v@nearbuy.com;
    exit 1;
fi


exit 0