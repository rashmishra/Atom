#!/bin/bash

###############################################################################
## Script Name: master_ETL.sh
## Author: Ranganath
## Date: 27-May-2016
## Purpose: Modularize the ETL process: Wrapper script for E, T and L.
## Flags used:       Flag Name   | Valid Values
##             ------------------------------------
##             a) Run Flag       | RUN/ SKIP
##             b) ETL Mode       | FULL/ PARTIAL
##             c) Extract Flag   | RUN/ SKIP
##             d) Transform Flag | RUN/ SKIP
##             e) Load Flag      | RUN/ SKIP
###############################################################################



## Source of environment variables
. /home/ubuntu/modular_ETL/config/masterenv.sh

export TZ;


taskStartTime=`date`

v_job_start_epoch=`date +%s`
v_job_start_ts=`echo $(date -d "@$v_job_start_epoch" +"%Y-%m-%d %r %Z")`

v_masterlog_txt=''


v_masterlog_txt+=`echo " \n"`;
v_email_text="ETL failed in one or more stages. \n";



########################################################################
## Removing the old files from directories

rm $LOGS_DIR/*.*

rm $DAILY_DUMP_PATH/*.*
rm $DAILY_LOAD_PATH/*.*
rm $DAILY_TRANSFORM_PATH/*.*

rm $TEMP_DIR/*.*

########################################################################




# Tokenizing of configuration
v_masterlog_txt+=`echo "\n$(date) Config file name is " $CONFIG_FILE_NAME`



v_config_line_cnr=`wc -l <(cat $CONFIG_FILE_NAME | sed 1d) | awk '{print $1}'`

v_masterlog_txt+=`echo "\n$(date) There are " $v_config_line_cnr " data object entries in configuration file."`


# Counter for data objects configured in config file.
v_data_obj_cnr=0;

v_masterlog_txt+=`echo "\n$(date) Started Tokenizing"`

cat $CONFIG_FILE_NAME | sed 1d > ./sed_config.txt

v_config_text=`cat $CONFIG_FILE_NAME | sed 1d`;

#v_masterlog_txt+=`echo "\n$(date) Config file's text: "`;
#v_masterlog_txt+=`echo "\n$(date) $v_config_text"`;


# Arrays to read configuration data
declare -a v_arr_data_object
#declare -a v_arr_export_file
declare -a v_arr_ETL_mode
declare -a v_arr_run_flag
declare -a v_arr_extract_flag
declare -a v_arr_transform_flag
declare -a v_arr_load_flag
declare -a v_arr_cloud_storage_path
declare -a v_arr_bq_dataset_name
declare -a v_arr_incremental_epoch
declare -a v_arr_epoch_mode

while IFS= read -r line; 
do 

#v_masterlog_txt+=`echo "\n$(date) Object Counter: $(($v_data_obj_cnr + 1))"`;
#v_masterlog_txt+=`echo "\n$(date) Configuration Line is :"  $line`; 
#echo "Tokens: ";
v_token_cnr=0
       IFS=','; for word in $line; do  
                  #v_masterlog_txt+=`echo "\n$(date) Token#: $v_token_cnr and Object count Variable: $v_data_obj_cnr"`;
                  #v_masterlog_txt+=`echo "\n$(date) Token is: $word"`;


                  case $v_token_cnr in
                      0) v_arr_data_object[$v_data_obj_cnr]="$word";
                       #echo "In Case: Object name $word.";
                       #echo "${v_arr_data_object[$v_data_obj_cnr]}";
                       ;;
                    1) v_arr_ETL_mode[$v_data_obj_cnr]="$word";
                       #echo "In Case: ETL Mode $word";
                       ;;
                    2) v_arr_run_flag[$v_data_obj_cnr]="$word";
                       #echo "In Case: Run Flag $word";
                       ;;
                    3) v_arr_extract_flag[$v_data_obj_cnr]="$word";
                       #echo "In Case: Extract Flag $word";
                       ;;
                    4) v_arr_transform_flag[$v_data_obj_cnr]="$word";
                       #echo "In Case: Transform Flag $word";
                       ;;
                    5) v_arr_load_flag[$v_data_obj_cnr]="$word";
                       #echo "In Case: Load Flag $word";
                       ;;
                    6) v_arr_cloud_storage_path[$v_data_obj_cnr]="${!word}";
                       echo "In Case: Cloud Storage path ${!word}";
                       ;;
                    7) v_arr_bq_dataset_name[$v_data_obj_cnr]="${!word}";
                       echo "In Case: BigQuery Dataset Name ${!word}";
                       ;;
                    8) v_arr_epoch_mode[$v_data_obj_cnr]="${word}";
                       echo "In Case: Epoch Mode ${word}";
                       ;;
                   esac
           
                  ((v_token_cnr++));

        done; 
((v_data_obj_cnr++));
done < sed_config.txt

# Removing the temp config file created during execution
rm sed_config.txt;

v_masterlog_txt+=`echo "\n$(date) Removed sed_config.txt, the temp file created for configuration data. Tokenizing complete"`

## Completed: Tokenizing of configuration

v_incremental_epoch='';

v_extract_pids_str=" ";

# Reading the Last Job ID to calculate the next Job ID
bq query "SELECT COALESCE(max( job_id ), 0 ) AS job_id FROM $METADATA_DATASET_NAME.etl_job_history" > $TEMP_DIR/last_Job_ID.txt;

v_last_job_id=`sed '5q;d'  $TEMP_DIR/last_Job_ID.txt | cut -d'|' -f2`;
v_current_job_id=$(($v_last_job_id+1));

## Procedure to add the status of the task into a <data-obj>_<task>_status.txt file
p_update_task_status(){

    v_log_dir="$1";
    v_status_filename="$2";
    v_task_status="$3";

    echo $v_task_status > $v_log_dir/$v_status_filename;

}

## Procedure to add the log specific to a data object into its respective file
p_add_task_log_file(){
    # $1 v_log_obj_txt 
    # $2 $TEMP_DIR/${v_arr_data_object[$i]}_log.log

    echo -e "$1" >> "$2";

}


## Procedure to add the status of the task into common CSV file containing the statuses and times of each task
p_add_task_log_row(){
    # Appends a row of task log into csv file

    echo "Writing row into csv file in $LOGS_DIR: $1"
    echo "$1" >> $LOGS_DIR/log_ETL_tasks.csv;
    #echo "$1" >> $TEMP_DIR/"${v_arr_data_object[$i]}"_log.log;
}




############################################################################################################
                                ############ Extraction Part############
############################################################################################################

#Looping for data element and extracting the ones set for extraction 
for ((i=0;i<$v_config_line_cnr; i++)); do
        #echo "Entered in Extraction loop. ${v_arr_run_flag[$i]}"

    v_task_start_epoch=`date +%s`
    v_task_start_ts=`echo $(date -d "@$v_task_start_epoch" +"%Y-%m-%d %r %Z")`;

    v_log_obj_txt="";


    if [ "${v_arr_run_flag[$i]}" != "RUN" ] && [ "${v_arr_run_flag[$i]}" != "SKIP" ]
        then v_masterlog_txt+=`echo "\n$(date) Run Flag is not properly configured for ${v_arr_data_object[$i]}"`;

             # Updating the status of the Extract status of the data object as 'incorrect-config'
             v_task_status='incorrect-config';
             #                         $1              $2                                     $3
             p_update_task_status "$TEMP_DIR" "${v_arr_data_object[$i]}_extract_status.txt" "$v_task_status"

             v_task_end_epoch=`date +%s`;
             v_task_end_ts=`echo $(date -d "@$v_task_end_epoch" +"%Y-%m-%d %r %Z")`;

              ###################################################################################
                ## Storing the log for BigQuery table logging in a common CSV file for all tasks ##

                v_bq_log_tbl_row='';

                ## Structure of Log row for Object section:
                # data_object,etl_task,data_object_task_start_time,data_object_task_start_ts,data_object_task_status,data_object_task_end_epoch,data_object_task_end_ts

                v_bq_log_tbl_row="${v_arr_data_object[$i]},extract,$v_task_start_epoch,$v_task_start_ts,$v_task_status,$v_task_end_epoch,$v_task_end_ts";

                ## Appending the log-table row portion specific to this activity to the main tasks CSV.
                p_add_task_log_row "$v_bq_log_tbl_row";

                #Adding the same row to task's log
                v_log_obj_txt+=`echo "\n Log Table Row: \n$v_bq_log_tbl_row"`;
                p_add_task_log_file "$v_log_obj_txt" "$TEMP_DIR/${v_arr_data_object[$i]}_log.log"
                ###################################################################################

             #echo $v_task_status > $TEMP_DIR/${v_arr_data_object[$i]}_extract_status.txt;

    elif [ "${v_arr_run_flag[$i]}" == "SKIP" ]    
        then v_masterlog_txt+=`echo "\n$(date) ETL run will be skipped for ${v_arr_data_object[$i]} as per configuration"`;

             echo -e "\n$(date) ETL Extraction run will be skipped for ${v_arr_data_object[$i]} as per configuration" > $TEMP_DIR/"${v_arr_data_object[$i]}"_log.log
             
             # Updating the status of the Extract status of the data object as 'skipped'
             v_task_status='skipped';

             echo "Task Start epoch time is $v_task_start_epoch, $v_task_start_ts";

             #                         $1              $2                                     $3
             p_update_task_status "$TEMP_DIR" "${v_arr_data_object[$i]}_extract_status.txt" "$v_task_status"

             v_task_end_epoch=`date +%s`;
             v_task_end_ts=`echo $(date -d "@$v_task_end_epoch" +"%Y-%m-%d %r %Z")`;

              ###################################################################################
                ## Storing the log for BigQuery table logging in a common CSV file for all tasks ##

                v_bq_log_tbl_row='';

                ## Structure of Log row for Object section:
                # data_object,etl_task,data_object_task_start_time,data_object_task_start_ts,data_object_task_status,data_object_task_end_epoch,data_object_task_end_ts
                
                echo "Task CSV Row of lefted pull for BigQuery:"
                
                v_bq_log_tbl_row="${v_arr_data_object[$i]},extract,$v_task_start_epoch,$v_task_start_ts,$v_task_status,$v_task_end_epoch,$v_task_end_ts";

                ## Appending the log-table row portion specific to this activity to the main tasks CSV.
                p_add_task_log_row "$v_bq_log_tbl_row";

                #Adding the same row to task's log
                v_log_obj_txt+=`echo "\n Log Table Row: \n$v_bq_log_tbl_row"`;
                p_add_task_log_file "$v_log_obj_txt" "$TEMP_DIR/${v_arr_data_object[$i]}_log.log"
                ###################################################################################

    else v_masterlog_txt+=`echo "\n$(date) ${v_arr_data_object[$i]} is configured for running."`;

        if [ ${v_arr_extract_flag[$i]} == "RUN" ]; then
            
                # This if block is to calculate the incremental epoch value for extraction
                if [ ${v_arr_ETL_mode[$i]} == "FULL" ]
                    then v_incremental_epoch=$FULL_LOAD_EPOCH;
                    #echo "In Full mode condition"  && [[ ${#v_arr_epoch_mode[$i]} -eq 10 ]]
                elif [ ${v_arr_epoch_mode[$i]} != "DEFAULT" ] && [[ ${v_arr_epoch_mode[$i]} -eq ${v_arr_epoch_mode[$i]} ]] && [[ ${v_arr_epoch_mode[$i]} -gt 0 ]] && [[ ${v_arr_epoch_mode[$i]} -lt $((`date +%s`)) ]]
                    then v_incremental_epoch=${v_arr_epoch_mode[$i]} ;
                else 
                    bq query "SELECT COALESCE(max( etl_job_start_epoch ), 1420050600 ) AS start_epoch FROM $METADATA_DATASET_NAME.etl_job_history WHERE data_object_task_status = 'success' AND etl_task = 'load' AND data_object = '${v_arr_data_object[$i]}'" > $TEMP_DIR/${v_arr_data_object[$i]}_epoch.txt;
                    v_incremental_epoch=`sed '5q;d'  $TEMP_DIR/${v_arr_data_object[$i]}_epoch.txt | cut -d'|' -f2`;
                    v_incremental_epoch=`echo $v_incremental_epoch | tr -d ' '`;

                    # Reducing 10 seconds from epoch
                    v_incremental_epoch=$(($v_incremental_epoch-10));
                    #echo "In Icremental mode condition"
                    
                 #rm $TEMP_DIR/${v_arr_data_object[$i]}_epoch.txt


                fi

                
                 echo "$v_incremental_epoch is the incremental Epoch for ${v_arr_data_object[$i]} in ${v_arr_ETL_mode[$i]} refresh mode";

                 v_arr_incremental_epoch[$i]=$v_incremental_epoch;
                 
                # Internally calling the extract files for the scheduled
                # Parameters: 
                #             #### $1: Data Object. ####
                #             #### $2: Data dump location. ####
                #             #### $3: Mongo cp directory. ####
                #             #### $4: Incremental Epoch. ####
                #             #### $5: ETL Home Directory. ####
                #             #### $6: ETL Refresh Mode (FULL/ PARTIAL). ####

                echo " Scripts Export path: $EXPORT_SCRIPTS_PATH ."
                echo "Script Name is: $EXPORT_SCRIPTS_PATH/export_${v_arr_data_object[$i]}.sh "

                if [ ${v_arr_data_object[$i]} == "communication" ||  ${v_arr_data_object[$i]} == "message" || ${v_arr_data_object[$i]} == "message_status_history" || ${v_arr_data_object[$i]} == "user_device_token_status" ]
                    #3.5
                    then export MONGO_PATH='/home/ubuntu/mongodb3-4-ssl/bin' 
                
                else 
                    #2.4.9
                    export MONGO_PATH='/home/ubuntu/mongo_cp/bin'
                fi
                #                                                                  $1                      $2               $3               $4                     $5                 $6
                bash $EXPORT_SCRIPTS_PATH/export_${v_arr_data_object[$i]}.sh ${v_arr_data_object[$i]} $DAILY_DUMP_PATH $MONGO_PATH ${v_arr_incremental_epoch[$i]} $ETL_HOME_DIR  ${v_arr_ETL_mode[$i]} &
                v_pid=$!
                v_extract_pids_str="$v_extract_pids_str $v_pid"
                echo "Current PIDs in queue for completion $v_extract_pids_str";

                # echo "i is $i and v_config_line_cnr is "$(($v_config_line_cnr-1));

        elif [ ${v_arr_extract_flag[$i]}  == "SKIP" ]; then
            ## Log, status and row are updated as Skip
            # Updating the status of the Extract status of the data object as 'skipped'
             v_task_status='skipped';

             v_log_obj_txt+=`echo "\n$(date) Skipping the extract for ${v_arr_data_object[$i]} as per its Extract flag configuration"`;

             #                         $1              $2                                     $3
             p_update_task_status "$TEMP_DIR" "${v_arr_data_object[$i]}_extract_status.txt" "$v_task_status"

             v_task_end_epoch=`date +%s`;
             v_task_end_ts=`echo $(date -d "@$v_task_end_epoch" +"%Y-%m-%d %r %Z")`;

              ###################################################################################
                ## Storing the log for BigQuery table logging in a common CSV file for all tasks ##

                v_bq_log_tbl_row='';

                ## Structure of Log row for Object section:
                # data_object,etl_task,data_object_task_start_time,data_object_task_start_ts,data_object_task_status,data_object_task_end_epoch,data_object_task_end_ts

                v_bq_log_tbl_row="${v_arr_data_object[$i]},extract,$v_task_start_epoch,$v_task_start_ts,$v_task_status,$v_task_end_epoch,$v_task_end_ts";

                ## Appending the log-table row portion specific to this activity to the main tasks CSV.
                p_add_task_log_row "$v_bq_log_tbl_row";

                #Adding the same row to task's log
                v_log_obj_txt+=`echo "\n Log Table Row: \n$v_bq_log_tbl_row"`;

                p_add_task_log_file "$v_log_obj_txt" "$TEMP_DIR/${v_arr_data_object[$i]}_log.log"
                ###################################################################################


        else 
            ## Log, status and row are updated as incorrect-config
            # Updating the status of the Extract status of the data object as 'incorrect-config'
             v_task_status='incorrect-config';

             v_log_obj_txt+=`echo "\n$(date) Not extracting data for ${v_arr_data_object[$i]} as its Extract flag configuration is incorrect"`;

             #                         $1              $2                                     $3
             p_update_task_status "$TEMP_DIR" "${v_arr_data_object[$i]}_extract_status.txt" "$v_task_status"

             v_task_end_epoch=`date +%s`;
             v_task_end_ts=`echo $(date -d "@$v_task_end_epoch" +"%Y-%m-%d %r %Z")`;

              ###################################################################################
                ## Storing the log for BigQuery table logging in a common CSV file for all tasks ##

                v_bq_log_tbl_row='';

                ## Structure of Log row for Object section:
                # data_object,etl_task,data_object_task_start_time,data_object_task_start_ts,data_object_task_status,data_object_task_end_epoch,data_object_task_end_ts

                v_bq_log_tbl_row="${v_arr_data_object[$i]},extract,$v_task_start_epoch,$v_task_start_ts,$v_task_status,$v_task_end_epoch,$v_task_end_ts";

                ## Appending the log-table row portion specific to this activity to the main tasks CSV.
                p_add_task_log_row "$v_bq_log_tbl_row";

                #Adding the same row to task's log
                v_log_obj_txt+=`echo "\n Log Table Row: \n$v_bq_log_tbl_row"`;

                p_add_task_log_file "$v_log_obj_txt" "$TEMP_DIR/${v_arr_data_object[$i]}_log.log"
                ###################################################################################
        fi
        

    fi
    
            # Calling the wait command to let all extracts finish
            if [ $i -eq $(($v_config_line_cnr-1)) ]
                then echo "All extracts invoked. Waiting for completion of "$v_extract_pids_str;
                     echo "wait $v_extract_pids_str";
                     wait 
                else echo "Extract Invocation in progress";
            fi

done;

v_masterlog_txt+=`echo "\n$(date) Waiting for the following processes to complete: $v_extract_pids_str "`;

#wait "$v_extract_pids_str";


v_masterlog_txt+=`echo "\n$(date) Extract processes are complete."`;

############################################################################################################
                        ############ Completed: Extraction Part############
############################################################################################################




# Arrays to read statuses of the ETL stages.

declare -a v_arr_extract_status
declare -a v_arr_transform_status
declare -a v_arr_load_status



##################################################################
### Reading the run status of the previous Extract runs.

v_all_extracts_status="success";
v_extract_failed_items="";

for ((i=0;i<$v_config_line_cnr; i++)); do

    #echo "$i is the iteration with directory as $TEMP_DIR";
    #echo "Home Directory of ETL: $ETL_HOME_DIR";
    #ls $TEMP_DIR/
    v_arr_extract_status[$i]=`cat $TEMP_DIR/${v_arr_data_object[$i]}_extract_status.txt`;

    if [[ ${v_arr_extract_status[$i]} == *"failed"* ]]; then
        v_all_extracts_status="failed";
        v_extract_failed_items+="\n    ${v_arr_data_object[$i]} : ${v_arr_extract_status[$i]}"
    fi

    echo "Extract Status for ${v_arr_data_object[$i]} is ${v_arr_extract_status[$i]}";

done;
#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#

echo "ALL Extracts status: $v_all_extracts_status";

if [ $v_all_extracts_status == "failed" ]; 
    then v_email_text+="\nExtract failed for: $v_extract_failed_items";
         echo -e "Extract failed for: \n $v_extract_failed_items";
fi

sleep 1;

## Mailing part



# send the email with the overall Extract status
#mail -a $LOGS_DIR/log_ETL_tasks.csv -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  sairanganath.v@nearbuy.com < /dev/null
# cat sample_text.txt | mutt -a $LOGS_DIR/log_ETL_tasks.csv -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com < /dev/null
mutt -a $LOGS_DIR/log_ETL_tasks.csv -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "     snehesh.mitra@nearbuy.com abhishek.manocha@nearbuy.com harsh.choudhary@nearbuy.com  rashmi.mishra@nearbuy.com < /dev/null

#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#



############################################################################################################
                            ############ Transformation Part############
############################################################################################################


v_extract_pids_str="";

#Looping for data element and Transforming the the ones set for extraction 
for ((i=0;i<$v_config_line_cnr; i++)); do

    v_task_start_epoch=`date +%s`
    v_task_start_ts=`echo $(date -d "@$v_task_start_epoch" +"%Y-%m-%d %r %Z")`;

    v_log_obj_txt="";


        #echo "Entered in Transformation loop. ${v_arr_run_flag[$i]}"
        # Checking for proper configuration
    if [ "${v_arr_run_flag[$i]}" != "RUN" ] && [ "${v_arr_run_flag[$i]}" != "SKIP" ]
        then v_masterlog_txt+=`echo "\n$(date) Run Flag is not properly configured for ${v_arr_data_object[$i]} "`;
             v_log_obj_txt+=`echo "\n$(date) Run Flag is not properly configured for ${v_arr_data_object[$i]} "`;

             # Updating the status of the Extract status of the data object as 'incorrect-config'
             v_task_status='incorrect-config';
             #                         $1              $2                                     $3
             p_update_task_status "$TEMP_DIR" "${v_arr_data_object[$i]}_transform_status.txt" "$v_task_status"

             #echo $v_task_status > $TEMP_DIR/${v_arr_data_object[$i]}_transform_status.txt;
                  v_task_end_epoch=`date +%s`;
                  v_task_end_ts=`echo $(date -d "@$v_task_end_epoch" +"%Y-%m-%d %r %Z")`;

              ###################################################################################
                ## Storing the log for BigQuery table logging in a common CSV file for all tasks ##

                v_bq_log_tbl_row='';

                ## Structure of Log row for Object section:
                # data_object,etl_task,data_object_task_start_time,data_object_task_start_ts,data_object_task_status,data_object_task_end_epoch,data_object_task_end_ts

                v_bq_log_tbl_row="${v_arr_data_object[$i]},transform,$v_task_start_epoch,$v_task_start_ts,$v_task_status,$v_task_end_epoch,$v_task_end_ts";

                ## Appending the log-table row portion specific to this activity to the main tasks CSV.
                p_add_task_log_row "$v_bq_log_tbl_row";

                #Adding the same row to task's log
                v_log_obj_txt+=`echo "\n Log Table Row: \n$v_bq_log_tbl_row"`;
                p_add_task_log_file "$v_log_obj_txt" "$TEMP_DIR/${v_arr_data_object[$i]}_log.log"
                ###################################################################################



    # Running ETL for this data object is 'SKIP'ped
    elif [ "${v_arr_run_flag[$i]}" == "SKIP" ]    
        then v_masterlog_txt+=`echo "\n$(date) ETL run will be skipped for ${v_arr_data_object[$i]} as per configuration"`;

        echo -e "\n$(date) ETL Transformation run will be skipped for ${v_arr_data_object[$i]} as per configuration" >> $TEMP_DIR/"${v_arr_data_object[$i]}"_log.log

             # Updating the status of the Extract status of the data object as 'skipped' in status.txt file
             v_task_status='skipped';
             #                         $1              $2                                     $3
             p_update_task_status "$TEMP_DIR" "${v_arr_data_object[$i]}_transform_status.txt" "$v_task_status"
             #echo $v_task_status > $TEMP_DIR/${v_arr_data_object[$i]}_transform_status.txt;

             v_task_end_epoch=`date +%s`;
             v_task_end_ts=`echo $(date -d "@$v_task_end_epoch" +"%Y-%m-%d %r %Z")`;

              ###################################################################################
                ## Storing the log for BigQuery table logging in a common CSV file for all tasks ##

                v_bq_log_tbl_row='';

                ## Structure of Log row for Object section:
                # data_object,etl_task,data_object_task_start_time,data_object_task_start_ts,data_object_task_status,data_object_task_end_epoch,data_object_task_end_ts

                v_bq_log_tbl_row="${v_arr_data_object[$i]},transform,$v_task_start_epoch,$v_task_start_ts,$v_task_status,$v_task_end_epoch,$v_task_end_ts";

                ## Appending the log-table row portion specific to this activity to the main tasks CSV.
                p_add_task_log_row "$v_bq_log_tbl_row";

                #Adding the same row to task's log
                v_log_obj_txt+=`echo "\n Log Table Row: \n$v_bq_log_tbl_row"`;
                p_add_task_log_file "$v_log_obj_txt" "$TEMP_DIR/${v_arr_data_object[$i]}_log.log"
                ###################################################################################


    
    else v_masterlog_txt+=`echo "\n$(date) ${v_arr_data_object[$i]} is configured for running."`;
        
        # ETL Run Flag is a yes; Transform is a Skip (Not required)/ Run/ incorrect (config) value
        if [  ${v_arr_transform_flag[$i]} != "SKIP" ] && [ ${v_arr_transform_flag[$i]} != "RUN" ]
            then #incorrect-config
            v_log_obj_txt+=`echo "\n$(date) Transform Flag is not properly configured for ${v_arr_data_object[$i]} "`;

             # Updating the status of the Extract status of the data object as 'incorrect-config'
             v_task_status='incorrect-config';
             #                         $1              $2                                     $3
             p_update_task_status "$TEMP_DIR" "${v_arr_data_object[$i]}_transform_status.txt" "$v_task_status"

             #echo $v_task_status > $TEMP_DIR/${v_arr_data_object[$i]}_transform_status.txt;
                  v_task_end_epoch=`date +%s`;
                  v_task_end_ts=`echo $(date -d "@$v_task_end_epoch" +"%Y-%m-%d %r %Z")`;

              ###################################################################################
                ## Storing the log for BigQuery table logging in a common CSV file for all tasks ##

                v_bq_log_tbl_row='';

                ## Structure of Log row for Object section:
                # data_object,etl_task,data_object_task_start_time,data_object_task_start_ts,data_object_task_status,data_object_task_end_epoch,data_object_task_end_ts

                v_bq_log_tbl_row="${v_arr_data_object[$i]},transform,$v_task_start_epoch,$v_task_start_ts,$v_task_status,$v_task_end_epoch,$v_task_end_ts";

                ## Appending the log-table row portion specific to this activity to the main tasks CSV.
                p_add_task_log_row "$v_bq_log_tbl_row";

                #Adding the same row to task's log
                v_log_obj_txt+=`echo "\n Log Table Row: \n$v_bq_log_tbl_row"`;
                p_add_task_log_file "$v_log_obj_txt" "$TEMP_DIR/${v_arr_data_object[$i]}_log.log"
                ###################################################################################
        elif [ ${v_arr_transform_flag[$i]} == "SKIP" ]
            then v_masterlog_txt+=`echo "\n$(date) Transformation is not configured for ${v_arr_data_object[$i]}. So moving the extract file from Extract to Transform folder"`;
                 echo -e "\n$(date) Transformation is not configured for ${v_arr_data_object[$i]}. So moving the extract file from Extract to Transform folder" >> $TEMP_DIR/"${v_arr_data_object[$i]}"_log.log
                 

                 if [[ "$v_arr_extract_status[$i]" == *"failed"* ]]
                    then 
                         # Updating the status of the Transform status of the data object as 'failed-skipped'
                         # as the relevant 'Extract' has failed.
                        v_task_status="failed-skipped";
                    else 
                         # Updating the status of the Transform status of the data object as 'skipped'.
                         v_task_status='skipped';

                         # Moving any existing object file(s) to Archives folder
                         #mv $DAILY_LOAD_PATH/${v_arr_data_object[$i]}*.* $ARCHIVES_DIR/

                         # Moving Extracted file(s) of data object to Load folder
                         cd $DAILY_DUMP_PATH
                         mv ${v_arr_data_object[$i]}* $DAILY_TRANSFORM_PATH/
                 fi
                 

                 
                 # Updating the status of Transform activity for this data object

                    ###################################################################################
                    ## Storing the status (success/ skipped/ failed/ failed-skipped) into respective text file. This will  
                    ## be in consumed by the main script to determine the status of entire Extract 
                    ## activity and this object's ETL flow.
                    
                    #                         $1              $2                                     $3
                    p_update_task_status "$TEMP_DIR" "${v_arr_data_object[$i]}_transform_status.txt" "$v_task_status"
                    #echo $v_task_status > $TEMP_DIR/${v_arr_data_object[$i]}_transform_status.txt

                ###################################################################################

                v_task_end_epoch=`date +%s`;
                v_task_end_ts=`echo $(date -d "@$v_task_end_epoch" +"%Y-%m-%d %r %Z")`;

                ###################################################################################
                ## Storing the log for BigQuery table logging in a common CSV file for all tasks ##

                v_bq_log_tbl_row='';

                ## Structure of Log row for Object section:
                # data_object,etl_task,data_object_task_start_time,data_object_task_start_ts,data_object_task_status,data_object_task_end_epoch,data_object_task_end_ts

                v_bq_log_tbl_row="${v_arr_data_object[$i]},transform,$v_task_start_epoch,$v_task_start_ts,$v_task_status,$v_task_end_epoch,$v_task_end_ts";

                ## Appending the log-table row portion specific to this activity to the main tasks CSV.
                p_add_task_log_row "$v_bq_log_tbl_row";

                #Adding the same row to task's log
                v_log_obj_txt+=`echo "\n Log Table Row: \n$v_bq_log_tbl_row"`;
                p_add_task_log_file "$v_log_obj_txt" "$TEMP_DIR/${v_arr_data_object[$i]}_log.log"
                ###################################################################################
        else 
                echo "Transformation kicked off"
                # Relevant Extract task's status check. If Extract failed, then the Transform and Load shouldn't run
                if [[ "$v_arr_extract_status[$i]" == *"failed"* ]]
                    then 
                         # Updating the status of the Transform status of the data object as 'failed-skipped'
                         # as the relevant 'Extract' has failed.
                        v_task_status="failed-skipped";
                        
                        # Adding <data-obj>_transform_status.txt file
                        p_update_task_status "$TEMP_DIR" "${v_arr_data_object[$i]}_transform_status.txt" "$v_task_status"

                        v_task_end_epoch=`date +%s`;
                        v_task_end_ts=`echo $(date -d "@$v_task_end_epoch" +"%Y-%m-%d %r %Z")`;

                       ###################################################################################
                        ## Storing the log for BigQuery table logging in a common CSV file for all tasks ##
                        v_bq_log_tbl_row='';

                        ## Structure of Log row for Object section:
                        #  data_object,etl_task,data_object_task_start_time,data_object_task_start_ts,data_object_task_status,data_object_task_end_epoch,data_object_task_end_ts
                        v_bq_log_tbl_row="${v_arr_data_object[$i]},transform,$v_task_start_epoch,$v_task_start_ts,$v_task_status,$v_task_end_epoch,$v_task_end_ts";

                        ## Appending the log-table row portion specific to this activity to the main tasks CSV.
                        p_add_task_log_row "$v_bq_log_tbl_row";

                        #Adding the same row to task's log
                        v_log_obj_txt+=`echo "\n Log Table Row: \n$v_bq_log_tbl_row"`;
                        p_add_task_log_file "$v_log_obj_txt" "$TEMP_DIR/${v_arr_data_object[$i]}_log.log"
                        ###################################################################################


                    else 
                         # Internally calling the Transform files for the scheduled
                        # Parameters: 
                        #             #### $1: Data Object. ####
                        #             #### $2: Data dump directory. ####
                        #             #### $3: Data Transform directory. ####
                        #             #### $4: Data Load directory. ####
                        #             #### $5: ETL Home Directory. ####

                        ## status.txt file and task log into common CSV file will happen inside the invoked transform file 

                        
                        #                                                                     $1                      $2               $3                    $4                  $5     
                        bash $TRANSFORM_SCRIPTS_PATH/transform_${v_arr_data_object[$i]}.sh ${v_arr_data_object[$i]}  $DAILY_DUMP_PATH $DAILY_TRANSFORM_PATH $DAILY_LOAD_PATH $ETL_HOME_DIR &
                        v_pid=$!
                        v_extract_pids_str="$v_extract_pids_str $v_pid"
                 fi
             
                
                echo "Current PIDs in queue for completion $v_extract_pids_str";


                # echo "i is $i and v_config_line_cnr is "$(($v_config_line_cnr-1));


        fi   

    fi
            # Calling the wait command to let all Transformations finish
            if [ $i -eq $(($v_config_line_cnr-1)) ]
                then v_masterlog_txt+=`echo "\n$(date) Waiting for the following processes to complete: $v_extract_pids_str "`;
                     wait 
                else echo "Transformations in progress";
            fi



done;



v_masterlog_txt+=`echo "\n$(date) Transform processes are complete."`;
############################################################################################################
                        ############ Completed: Transformation Part ############
############################################################################################################


##################################################################
### Reading the run status of the previous Transformation runs.

v_all_transformations_status="success";
v_transform_failed_items="";

for ((i=0;i<$v_config_line_cnr; i++)); do

    #echo "$i is the iteration with directory as $TEMP_DIR";
    #echo "Home Directory of ETL: $ETL_HOME_DIR";
    #ls $TEMP_DIR/
    v_arr_transform_status[$i]=`cat $TEMP_DIR/${v_arr_data_object[$i]}_transform_status.txt`;

    if [[ ${v_arr_transform_status[$i]} == *"failed"* ]]; then
        v_all_transformations_status="failed";
        v_transform_failed_items+="\n    ${v_arr_data_object[$i]} : ${v_arr_transform_status[$i]}"
    fi

    echo "Transformation Status for ${v_arr_data_object[$i]} is ${v_arr_transform_status[$i]}";

done;
#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#

echo "ALL Transformations status: $v_all_transformations_status";

if [ $v_all_transformations_status == "failed" ]; 
    then v_email_text+="\n\nTransformation failed for: $v_transform_failed_items";
fi


sleep 1;

## Mailing part

# send the email with the overall Transform status
#mail -a $LOGS_DIR/log_ETL_tasks.csv -s "Atom Refresh: All Transformations status:  $v_all_transformations_status. `date` "  sairanganath.v@nearbuy.com < /dev/null
# mutt -a $LOGS_DIR/log_ETL_tasks.csv -s "Atom Refresh: All Transformations status:  $v_all_transformations_status. `date` "  -- sairanganath.v@nearbuy.com < /dev/null
mutt -a $LOGS_DIR/log_ETL_tasks.csv -s "Atom Refresh: All Transformations status:  $v_all_transformations_status. `date` "   rashmi.mishra@nearbuy.com < /dev/null

#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#

sleep 1;


############################################################################################################
                                ############ Loading Part############                                       
############################################################################################################

v_extract_pids_str="";
#Looping for data element and extracting the ones set for extraction 
for ((i=0;i<$v_config_line_cnr; i++)); do

     v_task_start_epoch=`date +%s`
     v_task_start_ts=`echo $(date -d "@$v_task_start_epoch" +"%Y-%m-%d %r %Z")`;

     v_log_obj_txt="";


        #echo "Entered in Loading loop. ${v_arr_run_flag[$i]}"
    if [ "${v_arr_run_flag[$i]}" != "RUN" ] && [ "${v_arr_run_flag[$i]}" != "SKIP" ]
        then v_masterlog_txt+=`echo "\n$(date) Run Flag is not properly configured for ${v_arr_data_object[$i]} "`;
             v_log_obj_txt+=`echo "\n$(date) Run Flag is not properly configured for ${v_arr_data_object[$i]}"`;
             
             # Updating the status of the Extract status of the data object as 'incorrect-config'
             v_task_status='incorrect-config';
             #                         $1              $2                                     $3
             p_update_task_status "$TEMP_DIR" "${v_arr_data_object[$i]}_load_status.txt" "$v_task_status"

             #echo $v_task_status > $TEMP_DIR/${v_arr_data_object[$i]}_load_status.txt;
                  v_task_end_epoch=`date +%s`;
                  v_task_end_ts=`echo $(date -d "@$v_task_end_epoch" +"%Y-%m-%d %r %Z")`;

              ###################################################################################
                ## Storing the log for BigQuery table logging in a common CSV file for all tasks ##

                v_bq_log_tbl_row='';

                ## Structure of Log row for Object section:
                # data_object,etl_task,data_object_task_start_time,data_object_task_start_ts,data_object_task_status,data_object_task_end_epoch,data_object_task_end_ts

                v_bq_log_tbl_row="${v_arr_data_object[$i]},load,$v_task_start_epoch,$v_task_start_ts,$v_task_status,$v_task_end_epoch,$v_task_end_ts";

                ## Appending the log-table row portion specific to this activity to the main tasks CSV.
                p_add_task_log_row "$v_bq_log_tbl_row";

                #Adding the same row to task's log
                v_log_obj_txt+=`echo "\n Log Table Row: \n$v_bq_log_tbl_row"`;
                p_add_task_log_file "$v_log_obj_txt" "$TEMP_DIR/${v_arr_data_object[$i]}_log.log"
                #-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#
    
    # Running ETL for this data object is 'SKIP'ped
    elif [ "${v_arr_run_flag[$i]}" == "SKIP" ]    
        then v_masterlog_txt+=`echo "\n$(date) ETL run will be skipped for ${v_arr_data_object[$i]} as per configuration"`;
              v_log_obj_txt+=`echo -e "\n$(date) ETL Load run will be skipped for ${v_arr_data_object[$i]} as per configuration"`;
             
             
             v_task_status='skipped';
             #                         $1              $2                                     $3
             p_update_task_status "$TEMP_DIR" "${v_arr_data_object[$i]}_load_status.txt" "$v_task_status"

             #echo $v_task_status > $TEMP_DIR/${v_arr_data_object[$i]}_load_status.txt;
                  v_task_end_epoch=`date +%s`;
                  v_task_end_ts=`echo $(date -d "@$v_task_end_epoch" +"%Y-%m-%d %r %Z")`;

              ###################################################################################
                ## Storing the log for BigQuery table logging in a common CSV file for all tasks ##

                v_bq_log_tbl_row='';

                ## Structure of Log row for Object section:
                # data_object,etl_task,data_object_task_start_time,data_object_task_start_ts,data_object_task_status,data_object_task_end_epoch,data_object_task_end_ts

                v_bq_log_tbl_row="${v_arr_data_object[$i]},load,$v_task_start_epoch,$v_task_start_ts,$v_task_status,$v_task_end_epoch,$v_task_end_ts";

                ## Appending the log-table row portion specific to this activity to the main tasks CSV.
                p_add_task_log_row "$v_bq_log_tbl_row";

                #Adding the same row to task's log
                v_log_obj_txt+=`echo "\n Log Table Row: \n$v_bq_log_tbl_row"`;
                p_add_task_log_file "$v_log_obj_txt" "$TEMP_DIR/${v_arr_data_object[$i]}_log.log"
                #-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#
    
    else v_masterlog_txt+=`echo "\n$(date) ${v_arr_data_object[$i]} is configured for running."`;
        if [  ${v_arr_load_flag[$i]} != "SKIP" ] && [ ${v_arr_load_flag[$i]} != "RUN" ]
          then #incorrect
               v_log_obj_txt+=`echo "\n$(date) Load Flag is not properly configured for ${v_arr_data_object[$i]} "`;

             # Updating the status of the Extract status of the data object as 'incorrect-config'
             v_task_status='incorrect-config';
             #                         $1              $2                                     $3
             p_update_task_status "$TEMP_DIR" "${v_arr_data_object[$i]}_load_status.txt" "$v_task_status"

             #echo $v_task_status > $TEMP_DIR/${v_arr_data_object[$i]}_load_status.txt;
                  v_task_end_epoch=`date +%s`;
                  v_task_end_ts=`echo $(date -d "@$v_task_end_epoch" +"%Y-%m-%d %r %Z")`;

              ###################################################################################
                ## Storing the log for BigQuery table logging in a common CSV file for all tasks ##

                v_bq_log_tbl_row='';

                ## Structure of Log row for Object section:
                # data_object,etl_task,data_object_task_start_time,data_object_task_start_ts,data_object_task_status,data_object_task_end_epoch,data_object_task_end_ts

                v_bq_log_tbl_row="${v_arr_data_object[$i]},load,$v_task_start_epoch,$v_task_start_ts,$v_task_status,$v_task_end_epoch,$v_task_end_ts";

                ## Appending the log-table row portion specific to this activity to the main tasks CSV.
                p_add_task_log_row "$v_bq_log_tbl_row";

                #Adding the same row to task's log
                v_log_obj_txt+=`echo "\n Log Table Row: \n$v_bq_log_tbl_row"`;
                p_add_task_log_file "$v_log_obj_txt" "$TEMP_DIR/${v_arr_data_object[$i]}_log.log"
                ###################################################################################

        elif [ ${v_arr_load_flag[$i]} == "SKIP" ]
            then 
                 if [[ "$v_arr_transform_status[$i]" == *"failed"* ]]
                    then 
                         # Updating the status of the Load status of the data object as 'failed-skipped'
                         # as the relevant 'Transformation' has failed.
                        v_task_status="failed-skipped";
                        v_masterlog_txt+=`echo "\n$(date) Loading is being skipped for ${v_arr_data_object[$i]} as per its configuration flag and also the relevant 'Transformation' has failed."`;
                        v_log_obj_txt+=`echo "\n$(date) Loading is being skipped for ${v_arr_data_object[$i]} as per its configuration flag and also the relevant 'Transformation' has failed."`;

                    else 
                         # Updating the status of the Load status of the data object as 'skipped'; its 
                         # relevant 'Transformation' hasn't failed.
                         v_task_status='skipped';
                         v_masterlog_txt+=`echo "\n$(date) Loading is being skipped for ${v_arr_data_object[$i]} as per its configuration flag."`;
                         v_log_obj_txt+=`echo "\n$(date) Loading is being skipped for ${v_arr_data_object[$i]} as per its configuration flag."`;

                 fi

                 #                         $1              $2                                     $3
                 p_update_task_status "$TEMP_DIR" "${v_arr_data_object[$i]}_load_status.txt" "$v_task_status"

                #echo $v_task_status > $TEMP_DIR/${v_arr_data_object[$i]}_load_status.txt;
                  v_task_end_epoch=`date +%s`;
                  v_task_end_ts=`echo $(date -d "@$v_task_end_epoch" +"%Y-%m-%d %r %Z")`;

              ###################################################################################
                ## Storing the log for BigQuery table logging in a common CSV file for all tasks ##

                v_bq_log_tbl_row='';

                ## Structure of Log row for Object section:
                # data_object,etl_task,data_object_task_start_time,data_object_task_start_ts,data_object_task_status,data_object_task_end_epoch,data_object_task_end_ts

                v_bq_log_tbl_row="${v_arr_data_object[$i]},load,$v_task_start_epoch,$v_task_start_ts,$v_task_status,$v_task_end_epoch,$v_task_end_ts";

                ## Appending the log-table row portion specific to this activity to the main tasks CSV.
                p_add_task_log_row "$v_bq_log_tbl_row";

                #Adding the same row to task's log
                v_log_obj_txt+=`echo "\n Log Table Row: \n$v_bq_log_tbl_row"`;
                p_add_task_log_file "$v_log_obj_txt" "$TEMP_DIR/${v_arr_data_object[$i]}_log.log"
                #-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#
        else 
              # ETL Run Flag and Load Flag are a 'Yes'. Checking the prior Transform's status
                if [[ "$v_arr_transform_status[$i]" == *"failed"* ]]
                    then 
                         # Updating the status of the Load status of the data object as 'failed-skipped'
                         # as the relevant 'Transformation' has failed.
                        v_task_status="failed-skipped";
                        v_masterlog_txt+=`echo "\n$(date) Loading is being skipped for ${v_arr_data_object[$i]} as per its configuration flag and also the relevant 'Transformation' has failed."`;
                        v_log_obj_txt+=`echo "\n$(date) Loading is being skipped for ${v_arr_data_object[$i]} as per its configuration flag and also the relevant 'Transformation' has failed."`;

                         #                         $1              $2                                     $3
                         p_update_task_status "$TEMP_DIR" "${v_arr_data_object[$i]}_load_status.txt" "$v_task_status"

                        #echo $v_task_status > $TEMP_DIR/${v_arr_data_object[$i]}_load_status.txt;
                          v_task_end_epoch=`date +%s`;
                          v_task_end_ts=`echo $(date -d "@$v_task_end_epoch" +"%Y-%m-%d %r %Z")`;

                        ###################################################################################
                        ## Storing the log for BigQuery table logging in a common CSV file for all tasks ##

                        v_bq_log_tbl_row='';

                        ## Structure of Log row for Object section:
                        # data_object,etl_task,data_object_task_start_time,data_object_task_start_ts,data_object_task_status,data_object_task_end_epoch,data_object_task_end_ts

                        v_bq_log_tbl_row="${v_arr_data_object[$i]},load,$v_task_start_epoch,$v_task_start_ts,$v_task_status,$v_task_end_epoch,$v_task_end_ts";

                        ## Appending the log-table row portion specific to this activity to the main tasks CSV.
                        p_add_task_log_row "$v_bq_log_tbl_row";

                        #Adding the same row to task's log
                        v_log_obj_txt+=`echo "\n Log Table Row: \n$v_bq_log_tbl_row"`;
                        p_add_task_log_file "$v_log_obj_txt" "$TEMP_DIR/${v_arr_data_object[$i]}_log.log";
                        #-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#

                    else 
                         #echo "Loading kicked off, scripts in $LOAD_SCRIPTS_PATH";
             
                        # Internally calling the Loading files for the scheduled
                        # Parameters: 
                        #             #### $1: Data Object. ####
                        #             #### $2: Cloud Bucket Name. ####
                        #             #### $3: Data Load directory. ####
                        #             #### $4: Metadata Dataset name. ####
                        #             #### $5: Dataset Name. ####
                        #             #### $6: ETL Home Directory. ####
                        #             #### $7: Respective incremental epoch. ####

                        #                                                                 $1                      $2                        $3                    $4                  $5                    $6               $7
                        bash $LOAD_SCRIPTS_PATH/load_${v_arr_data_object[$i]}.sh ${v_arr_data_object[$i]} ${v_arr_cloud_storage_path[$i]}  $DAILY_LOAD_PATH  $METADATA_DATASET_NAME ${v_arr_bq_dataset_name[$i]} $ETL_HOME_DIR ${v_arr_incremental_epoch[$i]} &
                        v_pid=$!
                        v_extract_pids_str="$v_extract_pids_str $v_pid"
                        echo "Current PIDs in queue for completion $v_extract_pids_str";

                        # echo "i is $i and v_config_line_cnr is "$(($v_config_line_cnr-1));

                 fi

        fi   

    fi
            # Calling the wait command to let all Loads finish
            if [ $i -eq $(($v_config_line_cnr-1)) ]
                then v_masterlog_txt+=`echo "\n$(date) Waiting for the following processes to complete: $v_extract_pids_str "`;
                     wait 
                else echo "Loads in progress";
            fi



done;



v_masterlog_txt+=`echo "\n$(date) Load processes are complete."`;

############################################################################################################
                         ############ Completed: Loading Part############                                   
############################################################################################################


##################################################################
### Reading the run status of the previous load runs.

v_all_loads_status="success";
v_load_failed_items="";

for ((i=0;i<$v_config_line_cnr; i++)); do

    #echo "$i is the iteration with directory as $TEMP_DIR";
    #echo "Home Directory of ETL: $ETL_HOME_DIR";
    #ls $TEMP_DIR/
    v_arr_load_status[$i]=`cat $TEMP_DIR/${v_arr_data_object[$i]}_load_status.txt`;

    if [[ ${v_arr_load_status[$i]} == *"failed"* ]]; then
        v_all_loads_status="failed";
        v_load_failed_items+="\n    ${v_arr_data_object[$i]} : ${v_arr_load_status[$i]}"
    fi

    echo "load Status for ${v_arr_data_object[$i]} is ${v_arr_load_status[$i]}";

done;


if [ $v_all_loads_status == "failed" ]; then
    v_email_text+="\n\nLoading failed for: $v_load_failed_items" ;
    echo -e "Load Failed items: $v_load_failed_items";
    
fi

echo "ALL loads status: $v_all_loads_status";


## Mailing part
# send the email with the overall load status
#mail -a $LOGS_DIR/log_ETL_tasks.csv -s "Atom Refresh: All loads status:  $v_all_loads_status. `date` "  sairanganath.v@nearbuy.com < /dev/null
mutt -a $LOGS_DIR/log_ETL_tasks.csv -s "Atom Refresh: All loads status:  $v_all_loads_status. `date` "    snehesh.mitra@nearbuy.com abhishek.manocha@nearbuy.com harsh.choudhary@nearbuy.com  rashmi.mishra@nearbuy.com < /dev/null

#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#


# Calculating the status of entire ETL job
v_ETL_job_run_status="success";

# if [ $v_all_loads_status == "failed" -o  $v_all_extracts_status == "failed" -o  $v_all_transformations_status == "failed" ];
#     then v_ETL_job_run_status="failed";
# fi

if [ $v_all_loads_status == "success" -a  $v_all_extracts_status == "success" -a  $v_all_transformations_status == "success" ];
    then v_email_text="All is Well :)\n Extract, Transform and Load were successful for all data objects";
fi
# Mailing all runs Status
echo -e "$v_email_text" | mutt -a $LOGS_DIR/log_ETL_tasks.csv -s "Atom Refresh: All ETL runs status:  $v_ETL_job_run_status. `date` "    snehesh.mitra@nearbuy.com abhishek.manocha@nearbuy.com harsh.choudhary@nearbuy.com  rashmi.mishra@nearbuy.com mahesh.sharma@nearbuy.com sunny.sharma@nearbuy.com abhishek.manocha@nearbuy.com alka.gupta@nearbuy.com;

# mutt -a $LOGS_DIR/log_ETL_tasks.csv -s "Atom Refresh: All ETL runs status:  $v_ETL_job_run_status. `date` "  -- sairanganath.v@nearbuy.com rashmi.mishra@nearbuy.com mahesh.sharma@nearbuy.com sunny.sharma@nearbuy.com < /dev/null




## Preparing total.csv; the file containing the rows for JOb History table
for (( i=0; i<$v_data_obj_cnr; i++ )) do
    echo "Inside Last for loop"
    echo "$i";
    echo "Array Element: ${v_arr_data_object[$i]}";
    
    #echo "cat $TEMP_DIR/${v_arr_data_object[$i]}_log.log >> $LOGS_DIR/log_modular_ETL_$v_task_datetime.log"
    cat "$TEMP_DIR/${v_arr_data_object[$i]}_log.log" >> "$LOGS_DIR/log_modular_ETL_$v_task_datetime.log"
done



# Sample element printing of an array
echo ${v_arr_data_object[2]}

#Sizes of Arrays.
echo ${#v_arr_data_object[@]}
#echo ${#v_arr_export_file[@]}
# echo ${#v_arr_ETL_mode[@]}
# echo ${#v_arr_extract_flag[@]}
# echo ${#v_arr_transform_flag[@]}
# echo ${#v_arr_load_flag[@]}

taskEndTime=`date`;

v_job_end_epoch=`date +%s`
v_job_end_ts=`echo $(date -d "@$v_job_end_epoch" +"%Y-%m-%d %r %Z")`

v_masterlog_txt+=`echo "ETL Wrapper Script started at " $taskStartTime " and ended at " $taskEndTime`;

echo "printing Log data"
IFS=$'\n'
for obj_csv in `cat $LOGS_DIR/log_ETL_tasks.csv`;
do 
    echo "$v_current_job_id,$v_job_start_epoch,$v_job_start_ts,$v_ETL_job_run_status,$v_job_end_epoch,$v_job_end_ts,$obj_csv" >> $LOGS_DIR/all_ETL_tasks.csv
done

###################################################################
              ### Loading Job History table ###
###################################################################
# Loading all CSV file (Job Log table data) onto cloud
gsutil cp $LOGS_DIR/all_ETL_tasks.csv $CLOUD_DAILY_STORAGE_PATH/

#bq load --field_delimiter=',' --source_format=CSV $METADATA_DATASET_NAME.jobHistory $SCRIPTS_PATH/jobHistory.csv "jobId:INTEGER,startAt:INTEGER,endAt:INTEGER,status:STRING"
bq load --field_delimiter=',' --source_format=CSV $METADATA_DATASET_NAME.etl_job_history $CLOUD_DAILY_STORAGE_PATH/all_ETL_tasks.csv $SCHEMA_FILE_PATH/schema_etl_job_history.json
#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#


#cat sample_text.txt | mutt -a $LOGS_DIR/log_ETL_tasks.csv -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com < /dev/null

v_task_datetime=`echo $(date -d "@$v_task_start_epoch" +"%Y-%m-%d_%H:%M_%Z")`;

# Master Log text into a master log file
echo -e  "$v_masterlog_txt" >> "$LOGS_DIR/log_modular_ETL_$v_task_datetime.log"


## Appending Logs of all data objects into main file

for (( i=0; i<$v_data_obj_cnr; i++ )) do
    echo "Inside Last for loop"
    echo "$i";
    echo "Array Element: ${v_arr_data_object[$i]}";
    
    #echo "cat $TEMP_DIR/${v_arr_data_object[$i]}_log.log >> $LOGS_DIR/log_modular_ETL_$v_task_datetime.log"
    cat "$TEMP_DIR/${v_arr_data_object[$i]}_log.log" >> "$LOGS_DIR/log_modular_ETL_$v_task_datetime.log"
done

# Moving the final (master) log file into archives
cp "$LOGS_DIR/log_modular_ETL_$v_task_datetime.log" "$ARCHIVES_DIR"
# Moving the ETL Tasks' CSV Rows file into archives
cat "$LOGS_DIR/log_ETL_tasks.csv" >> "$ARCHIVES_DIR/logs/log_ETL_tasks_$v_task_datetime.log"

echo -e "Master Log Text: " $v_masterlog_txt

echo "ETL Wrapper Script started at " $taskStartTime " and ended at " $taskEndTime;

## Log file consolidation and Job History table updation


exit 0
