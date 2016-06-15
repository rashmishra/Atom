#!/bin/bash

#Transform Mix Panel
#!/bin/bash

## Script Name: transform_category.sh


# Parameters: 
#             #### $1: Data Object. ####
#             #### $2: Data dump directory. ####
#             #### $3: Data Transform directory. ####
#             #### $4: Data Load directory. ####    
#             #### $5: ETL Home Directory. ####


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

        # Creating new file for category's ETL run. Content will be appended in further tasks of T and L.
        echo -e "$v_log_obj_txt" > $v_temp_dir/"$v_data_object"_log.log
        chmod 0777 $v_temp_dir/"$v_data_object"_log.log;

        #Exiting with an error-code
        exit 1;
    fi


}


#Copying file from Extract folder to Transform folder
cd $v_data_dump_dir
cp $v_filename.gz $v_transform_dir

cd $v_transform_dir

# Unzipping the extract for transformation
gzip -d $v_filename.gz

echo "$v_filename is the transforming filename"

# Transformation commands
# Replacing 2 Blanks in Key/ Field name with underscores
    sed  -r 's/(,"\w+)[ ](\w+)[ ](\w+":)/\1_\2_\3/g' $v_filename > pass1.json &
    v_transform_pid=$!

    # Waiting for the process to complete
    if wait $v_transform_pid; then
        echo "Process $v_transform_pid Status: success";
        v_task_status="success";
    else 
        echo "Process $v_transform_pid Status: failed";
        v_task_status="failed";
    fi

    v_subtask="Transformation # 1";
    p_exit_upon_error $v_task_status "$v_subtask"

    rm $v_filename
    echo $(date) ": after sed 1" 

    # Replacing 4 Blanks in Key/ Field name with underscores
    sed  -r 's/(,"\w+)[ ](\w+)[ ](\w+)[ ](\w+":)/\1_\2_\3_\4/g' pass1.json > pass2.json &
    v_transform_pid=$!

    # Waiting for the process to complete
    if wait $v_transform_pid; then
        echo "Process $v_transform_pid Status: success";
        v_task_status="success";
    else 
        echo "Process $v_transform_pid Status: failed";
        v_task_status="failed";
    fi

    v_subtask="Transformation # 2";
    p_exit_upon_error $v_task_status "$v_subtask"

    rm pass1.json
    echo $(date) ": after sed 2" 
    
    # Replacing 3 Blanks in Key/ Field name with underscores
    sed  -r 's/(,"\w+)[ ](\w+":)/\1_\2/g' pass2.json > pass3.json &
    v_transform_pid=$!

    # Waiting for the process to complete
    if wait $v_transform_pid; then
        echo "Process $v_transform_pid Status: success";
        v_task_status="success";
    else 
        echo "Process $v_transform_pid Status: failed";
        v_task_status="failed";
    fi

    v_subtask="Transformation # 3";
    p_exit_upon_error $v_task_status "$v_subtask"

    rm pass2.json
    echo $(date) ": after sed 3" 
    
    # Replacing 5 Blanks in Key/ Field name with underscores
    sed  -r 's/(,"\w+)[ ](\w+)[ ](\w+)[ ](\w+)[ ](\w+":)/\1_\2_\3_\4_\5/g' pass3.json > pass4.json &
    v_transform_pid=$!

    # Waiting for the process to complete
    if wait $v_transform_pid; then
        echo "Process $v_transform_pid Status: success";
        v_task_status="success";
    else 
        echo "Process $v_transform_pid Status: failed";
        v_task_status="failed";
    fi

    v_subtask="Transformation # 4";
    p_exit_upon_error $v_task_status "$v_subtask"


    rm pass3.json
    echo $(date) ": after sed 4" 
    
    # Renaming the field 'city' as 'src_city'
    sed  -r 's/,"city":/,"src_city":/' pass4.json > pass5.json &
    v_transform_pid=$!

    # Waiting for the process to complete
    if wait $v_transform_pid; then
        echo "Process $v_transform_pid Status: success";
        v_task_status="success";
    else 
        echo "Process $v_transform_pid Status: failed";
        v_task_status="failed";
    fi

    v_subtask="Transformation # 5";
    p_exit_upon_error $v_task_status "$v_subtask"

    rm pass4.json
    echo $(date) ": after sed 5" 

    sed  -r 's/("campaign_id":)([0-9]+)/\1"\2"/g'  pass5.json > pass6.json &
    v_transform_pid=$!

    # Waiting for the process to complete
    if wait $v_transform_pid; then
        echo "Process $v_transform_pid Status: success";
        v_task_status="success";
    else 
        echo "Process $v_transform_pid Status: failed";
        v_task_status="failed";
    fi

    v_subtask="Transformation # 6";
    p_exit_upon_error $v_task_status "$v_subtask"
    rm pass5.json
    echo $(date) ": after sed 6" 
    
    # Removing the symbol $ from the Keys/ field names
    sed  's/[$]//g' pass6.json > pass7.json &
    v_transform_pid=$!

    # Waiting for the process to complete
    if wait $v_transform_pid; then
        echo "Process $v_transform_pid Status: success";
        v_task_status="success";
    else 
        echo "Process $v_transform_pid Status: failed";
        v_task_status="failed";
    fi

    v_subtask="Transformation # 7";
    p_exit_upon_error $v_task_status "$v_subtask"
    rm pass6.json
    echo $(date) ": after sed 7" 

    # Removing extra column Duration as 'duration' already exists
    sed  -r 's/("Duration":)"[0-9]+.[0-9]+",//g' pass7.json > pass8.json &
    v_transform_pid=$!

    # Waiting for the process to complete
    if wait $v_transform_pid; then
        echo "Process $v_transform_pid Status: success";
        v_task_status="success";
    else 
        echo "Process $v_transform_pid Status: failed";
        v_task_status="failed";
    fi

    v_subtask="Transformation # 8";
    p_exit_upon_error $v_task_status "$v_subtask"
    rm pass7.json
    echo $(date) ": after sed 8" 

    # Replacing hiphen (-) with and underscore (_)
    sed  -r 's/(,"\w+)[-](\w+":)/\1_\2/g' pass8.json > $v_filename &
    v_transform_pid=$!

    # Waiting for the process to complete
    if wait $v_transform_pid; then
        echo "Process $v_transform_pid Status: success";
        v_task_status="success";
    else 
        echo "Process $v_transform_pid Status: failed";
        v_task_status="failed";
    fi

    v_subtask="Transformation # 9";
    p_exit_upon_error $v_task_status "$v_subtask"
    rm pass8.json
    echo $(date) ": after sed 9" 

   
gzip -f $v_filename

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
