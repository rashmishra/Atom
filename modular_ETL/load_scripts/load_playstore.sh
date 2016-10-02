#!/bin/bash

## Script Name: load_playstore.sh
## Purpose: Modular ETL flow of Atom.


                # Parameters: 
                #             #### $1: Data Object. ####
                #             #### $2: Cloud Bucket Name. ####
                #             #### $3: Data Load directory. ####
                #             #### $4: Metadata Dataset name. ####
                #             #### $5: Dataset Name. ####
                #             #### $6: ETL Home Directory. ####
                #             #### $7: Respective incremental epoch. ####


. /home/ubuntu/modular_ETL/config/masterenv.sh


taskStartTime=`date`
v_task_start_epoch=`date +%s`
v_task_start_ts=`echo $(date -d "@$v_task_start_epoch" +"%Y-%m-%d %r %Z")`;
v_task_datetime=`echo $(date -d "@$v_task_start_epoch" +"%Y-%m-%d_%H:%M_%Z")`;


## Initializing required variables
v_etl_task='load'


maxBadRecords=0

v_data_object=$1;
tableName=$1;
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

echo "In playstore Loading script";
v_task_status='Not set yet';
v_log_obj_txt+=`echo "\n------------------------------------------------------------"`;
v_log_obj_txt+=`echo "\n------------------------------------------------------------"`;
v_log_obj_txt+=`echo "\n$v_etl_task process started for $v_data_object at $v_task_start_ts"`;

echo -e "v_logs_dir, \n v_temp_dir, \n v_arch_dir, \n v_schema_filepath"
echo -e "$v_logs_dir, \n $v_temp_dir, \n $v_arch_dir, \n $v_schema_filepath"
echo -e "$1 \n $2 \n $3 \n $4 \n $5 \n $6 \n $7 \n $8"


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


        # Creating new file for playstore's ETL run. Content will be appended in further tasks of T and L.
        echo -e "$v_log_obj_txt" >> $v_temp_dir/"$v_data_object"_log.log

        chmod 0777 $v_temp_dir/"$v_data_object"_log.log;

        

        exit 1;

    fi

}


# Detailed Reports movement
v_report_month=$(date +%Y%m);

v_load_pids="";

for i in $PLAY_TRANSFORM_DETAILED_REPORT_NAMES; do
    # echo "mv $v_transform_dir/${i}_${v_report_month}.csv $v_load_dir/"
    mv $v_transform_dir/${i}_${v_report_month}.csv $v_load_dir/
    v_load_pids+=" $!"
    # echo "gsutil -m cp $v_load_dir/${i}_${v_report_month}.csv $v_cloud_storage_path"
    gsutil -m cp $v_load_dir/${i}_${v_report_month}.csv $v_cloud_storage_path &
    v_load_pids+=" $!"

done

v_subtask="Moving and Cloud upload of Detailed Reports files"
if wait $v_load_pids; then
    echo "Processes $v_load_pids Status: success";
    v_task_status="success";
else 
    echo "Processes $v_load_pids Status: failed";
    v_task_status="failed";
fi


p_exit_upon_error "$v_task_status" "$v_subtask"


## Crashes Table

# Incremental Table
v_fileName=crashes_com.nearbuy.nearbuymobile_${v_report_month}.csv
tableName=crashes_crashes
v_destination_tbl="$v_metadataset_name.incremental_$tableName";
schemaFileName=schema_playstore_crashes_crashes.json
v_load_pids="";


echo "bq load --quiet --replace --field_delimiter=',' --source_format=CSV --skip_leading_rows=1 --max_bad_records=0  --allow_jagged_rows=1 --allow_quoted_newlines=1 --ignore_unknown_values=1   $v_destination_tbl $v_cloud_storage_path/$v_fileName $v_schema_filepath/$schemaFileName"
bq load --quiet --replace --field_delimiter=',' --source_format=CSV --skip_leading_rows=1 --max_bad_records=0  --allow_jagged_rows=1 --allow_quoted_newlines=1 --ignore_unknown_values=1   $v_destination_tbl $v_cloud_storage_path/$v_fileName $v_schema_filepath/$schemaFileName 
v_load_pids+=" $!"

if [[ "`bq ls $v_dataset_name | awk '{print $1}' | grep \"\b$tableName\b\"`" == "$tableName" ]] ; 
    then 
    # prior table
    echo "Table $v_metadataset_name.$tableName exists";
    v_query="SELECT * FROM $v_dataset_name.$tableName WHERE crash_report_date_and_time NOT IN (SELECT crash_report_date_and_time FROM $v_metadataset_name.incremental_$tableName)";
    v_destination_tbl="$v_metadataset_name.prior_$tableName";
    echo "Destination table is $v_destination_tbl and Query is $v_query"
    bq query --maximum_billing_tier 10 --allow_large_results=1  --quiet -n 1 --replace --destination_table=$v_destination_tbl "$v_query"
    v_load_pids+=" $!"

    else echo "Table $v_dataset_name.$tableName missing"; 

fi

# Merging prior and incremental
v_destination_tbl="$v_metadataset_name.prior_$tableName";
v_query="SELECT * FROM $v_metadataset_name.incremental_$tableName";
bq query --append=1 --maximum_billing_tier 10 --allow_large_results=1 --quiet -n 0 --flatten_results=0 --allow_large_results=1 --destination_table=$v_destination_tbl "$v_query" 
v_load_pids+=" $!"


# Updating the Atom table with updated data
bq rm -f $v_dataset_name.$tableName 
v_load_pids+=" $!"
bq cp $v_metadataset_name.prior_$tableName $v_dataset_name.$tableName 
v_load_pids+=" $!"
# Removing Prior and Incremental tables
bq rm -f $v_metadataset_name.prior_$tableName &
v_load_pids+=" $!"
bq rm -f $v_metadataset_name.incremental_$tableName &
v_load_pids+=" $!"

v_subtask="Detailed Reports data update: Crashes"
if wait $v_load_pids; then
    echo "Processes $v_load_pids Status: success";
    v_task_status="success";
else 
    echo "Processes $v_load_pids Status: failed";
    v_task_status="failed";
fi
p_exit_upon_error "$v_task_status" "$v_subtask"


## ANRs Table

# Incremental Table
v_fileName=anrs_com.nearbuy.nearbuymobile_${v_report_month}.csv
tableName=crashes_anrs
v_destination_tbl="$v_metadataset_name.incremental_$tableName";
schemaFileName=schema_playstore_crashes_anrs.json
v_load_pids=""

echo "bq load --quiet --replace --field_delimiter=',' --source_format=CSV --skip_leading_rows=1 --max_bad_records=0  --allow_jagged_rows=1 --allow_quoted_newlines=1 --ignore_unknown_values=1   $v_destination_tbl $v_cloud_storage_path/$v_fileName $v_schema_filepath/$schemaFileName"
bq load --quiet --replace --field_delimiter=',' --source_format=CSV --skip_leading_rows=1 --max_bad_records=0  --allow_jagged_rows=1 --allow_quoted_newlines=1 --ignore_unknown_values=1   $v_destination_tbl $v_cloud_storage_path/$v_fileName $v_schema_filepath/$schemaFileName
v_load_pids+=" $!"

if [[ "`bq ls $v_dataset_name | awk '{print $1}' | grep \"\b$tableName\b\"`" == "$tableName" ]] ; 
    then 
    # prior table
    echo "Table $v_metadataset_name.$tableName exists";
    v_query="SELECT * FROM $v_dataset_name.$tableName WHERE crash_report_date_and_time NOT IN (SELECT crash_report_date_and_time FROM $v_metadataset_name.incremental_$tableName)";
    v_destination_tbl="$v_metadataset_name.prior_$tableName";
    echo "Destination table is $v_destination_tbl and Query is $v_query"
    bq query  --maximum_billing_tier 10 --allow_large_results=1  --quiet -n 1 --replace --destination_table=$v_destination_tbl "$v_query"
    v_load_pids+=" $!"

    else echo "Table $v_dataset_name.$tableName missing"; 

fi

# Merging prior and incremental
v_destination_tbl="$v_metadataset_name.prior_$tableName";
v_query="SELECT * FROM $v_metadataset_name.incremental_$tableName";
bq query --append=1 --maximum_billing_tier 10 --allow_large_results=1 --quiet -n 0 --flatten_results=0 --allow_large_results=1 --destination_table=$v_destination_tbl "$v_query" 
v_load_pids+=" $!"


# Updating the Atom table with updated data
bq rm -f $v_dataset_name.$tableName
v_load_pids+=" $!"
bq cp $v_metadataset_name.prior_$tableName $v_dataset_name.$tableName
v_load_pids+=" $!"
# Removing Prior and Incremental tables
bq rm -f $v_metadataset_name.prior_$tableName &
v_load_pids+=" $!"
bq rm -f $v_metadataset_name.incremental_$tableName &
v_load_pids+=" $!"


v_subtask="Detailed Reports data update: ANRs"
if wait $v_load_pids; then
    echo "Processes $v_load_pids Status: success";
    v_task_status="success";
else 
    echo "Processes $v_load_pids Status: failed";
    v_task_status="failed";
fi

p_exit_upon_error "$v_task_status" "$v_subtask"


## Reviews table

# Incremental Table
v_fileName=reviews_com.nearbuy.nearbuymobile_${v_report_month}.csv
tableName=reviews_reviews
v_destination_tbl="$v_metadataset_name.incremental_$tableName";
schemaFileName=schema_playstore_reviews_reviews.json
v_load_pids=""

echo "bq load --quiet --replace --field_delimiter=',' --source_format=CSV --skip_leading_rows=1 --max_bad_records=0  --allow_jagged_rows=1 --allow_quoted_newlines=1 --ignore_unknown_values=1   $v_destination_tbl $v_cloud_storage_path/$v_fileName $v_schema_filepath/$schemaFileName"
bq load --quiet --replace --field_delimiter=',' --source_format=CSV --skip_leading_rows=1 --max_bad_records=0  --allow_jagged_rows=1 --allow_quoted_newlines=1 --ignore_unknown_values=1   $v_destination_tbl $v_cloud_storage_path/$v_fileName $v_schema_filepath/$schemaFileName
v_load_pids+=" $!"

if [[ "`bq ls $v_dataset_name | awk '{print $1}' | grep \"\b$tableName\b\"`" == "$tableName" ]] ; 
    then 
    # prior table
    echo "Table $v_metadataset_name.$tableName exists";
    v_query="SELECT * FROM $v_dataset_name.$tableName WHERE review_submit_date_and_time NOT IN (SELECT review_submit_date_and_time FROM $v_metadataset_name.incremental_$tableName)";
    v_destination_tbl="$v_metadataset_name.prior_$tableName";
    echo "Destination table is $v_destination_tbl and Query is $v_query"
    bq query  --maximum_billing_tier 10 --allow_large_results=1  --quiet -n 1 --replace --destination_table=$v_destination_tbl "$v_query"
    v_load_pids+=" $!"

    else echo "Table $v_dataset_name.$tableName missing"; 

fi

# Merging prior and incremental
v_destination_tbl="$v_metadataset_name.prior_$tableName";
v_query="SELECT * FROM $v_metadataset_name.incremental_$tableName";
bq query --append=1 --maximum_billing_tier 10 --allow_large_results=1 --quiet -n 0 --flatten_results=0 --allow_large_results=1 --destination_table=$v_destination_tbl "$v_query" 
v_load_pids+=" $!"

# Updating the Atom table with updated data
bq rm -f $v_dataset_name.$tableName
v_load_pids+=" $!"
bq cp $v_metadataset_name.prior_$tableName $v_dataset_name.$tableName
v_load_pids+=" $!"
# Removing Prior and Incremental tables
bq rm -f $v_metadataset_name.prior_$tableName &
v_load_pids+=" $!"
bq rm -f $v_metadataset_name.incremental_$tableName &
v_load_pids+=" $!"


v_subtask="Detailed Reports data update: Reviews"
if wait $v_load_pids; then
    echo "Processes $v_load_pids Status: success";
    v_task_status="success";
else 
    echo "Processes $v_load_pids Status: failed";
    v_task_status="failed";
fi

p_exit_upon_error "$v_task_status" "$v_subtask"

echo "Completed Detailed Reports Conversion"

## Aggregated Reports movement
# Aggregated Reports: Crashes
v_report_month=$(date +%Y%m);

tableName=stats_crashes

v_destination_tbl="$v_metadataset_name.incremental_$tableName";
schemaFileName=schema_playstore_stats_crashes.json
v_load_pids=""

if [[ "`bq ls $v_metadataset_name | awk '{print $1}' | grep \"\bincremental_$tableName\b\"`" == "incremental_$tableName" ]] ; 
	then bq rm $v_destination_tbl;
fi


for i in $PLAY_AGGREGATED_CRASHES_REPORT_NAMES; do
    v_fileName=${PLAY_AGGREGATED_CRASHES_REPORT_PREFIX}_${v_report_month}_${i}.csv
    # echo "mv $v_transform_dir/$v_fileName $v_load_dir/"
    mv $v_transform_dir/$v_fileName $v_load_dir/
    v_load_pids+=" $!"
    # echo "gsutil -m cp $v_load_dir/$v_fileName $v_cloud_storage_path"
    gsutil -m cp $v_load_dir/$v_fileName $v_cloud_storage_path
    v_load_pids+=" $!"
    # echo "bq load --quiet --field_delimiter=',' --source_format=CSV --skip_leading_rows=1 --max_bad_records=0  --allow_jagged_rows=1 --allow_quoted_newlines=1 --ignore_unknown_values=1   $v_destination_tbl $v_cloud_storage_path/$v_fileName $v_schema_filepath/$schemaFileName"
    bq load --quiet --field_delimiter=',' --source_format=CSV --skip_leading_rows=1 --max_bad_records=0  --allow_jagged_rows=1 --allow_quoted_newlines=1 --ignore_unknown_values=1   $v_destination_tbl $v_cloud_storage_path/$v_fileName $v_schema_filepath/$schemaFileName &
    v_load_pids+=" $!"
done

wait $v_load_pids

if [[ "`bq ls $v_dataset_name | awk '{print $1}' | grep \"\b$tableName\b\"`" == "$tableName" ]] ; 
    then 
    # prior table
    echo "Table $v_metadataset_name.$tableName exists";
    v_query="SELECT Dimension_Name, date,   package_name_app_name,  Dimension_Value,    daily_crashes,  daily_ANRs
             FROM (SELECT CONCAT(LOWER(RTRIM(LTRIM(Dimension_Name))), STRING(date)) as dkey, * FROM playstore.stats_crashes) A
             WHERE dkey NOT IN ( SELECT CONCAT(LOWER(RTRIM(LTRIM(Dimension_Name))), STRING(date)) as dkey  FROM [metadata.incremental_stats_crashes]   GROUP BY dkey)"
    v_destination_tbl="$v_metadataset_name.prior_$tableName";
    echo "Destination table is $v_destination_tbl and Query is $v_query"
    bq query  --maximum_billing_tier 10 --allow_large_results=1  --quiet -n 0  --replace --destination_table=$v_destination_tbl "$v_query"
    v_load_pids+=" $!"

    else echo "Table $v_dataset_name.$tableName missing"; 

fi

# Merging prior and incremental
v_destination_tbl="$v_metadataset_name.prior_$tableName";
v_query="SELECT * FROM $v_metadataset_name.incremental_$tableName";
bq query --append=1 --maximum_billing_tier 10 --allow_large_results=1 --quiet -n 0 --flatten_results=0 --allow_large_results=1 --destination_table=$v_destination_tbl "$v_query" 
v_load_pids+=" $!"

# Updating the Atom table with updated data
bq rm -f $v_dataset_name.$tableName
v_load_pids+=" $!"
bq cp $v_metadataset_name.prior_$tableName $v_dataset_name.$tableName
v_load_pids+=" $!"
# Removing Prior and Incremental tables
bq rm -f $v_metadataset_name.prior_$tableName &
v_load_pids+=" $!"
bq rm -f $v_metadataset_name.incremental_$tableName &
v_load_pids+=" $!"


v_subtask="Aggregated Reports data update: Crashes"
if wait $v_load_pids; then
    echo "Processes $v_load_pids Status: success";
    v_task_status="success";
else 
    echo "Processes $v_load_pids Status: failed";
    v_task_status="failed";
fi

p_exit_upon_error "$v_task_status" "$v_subtask"
# Completed Aggregated Reports: Crashes

# Aggregated Reports: GCM
v_report_month=$(date +%Y%m);

tableName=stats_gcm

v_destination_tbl="$v_metadataset_name.incremental_$tableName";
schemaFileName=schema_playstore_stats_gcm.json
v_load_pids=""

if [[ "`bq ls $v_metadataset_name | awk '{print $1}' | grep \"\bincremental_$tableName\b\"`" == "incremental_$tableName" ]] ; 
	then bq rm $v_destination_tbl;
fi


for i in $PLAY_AGGREGATED_GCM_REPORT_NAMES; do
    v_fileName=${PLAY_AGGREGATED_GCM_REPORT_PREFIX}_${v_report_month}_${i}.csv
    # echo "mv $v_transform_dir/$v_fileName $v_load_dir/"
    mv $v_transform_dir/$v_fileName $v_load_dir/
    v_load_pids+=" $!"
    
    # echo "gsutil -m cp $v_load_dir/$v_fileName $v_cloud_storage_path"
    gsutil -m cp $v_load_dir/$v_fileName $v_cloud_storage_path
    v_load_pids+=" $!"

    # echo "bq load --quiet --field_delimiter=',' --source_format=CSV --skip_leading_rows=1 --max_bad_records=0  --allow_jagged_rows=1 --allow_quoted_newlines=1 --ignore_unknown_values=1   $v_destination_tbl $v_cloud_storage_path/$v_fileName $v_schema_filepath/$schemaFileName"
    bq load --quiet --field_delimiter=',' --source_format=CSV --skip_leading_rows=1 --max_bad_records=0  --allow_jagged_rows=1 --allow_quoted_newlines=1 --ignore_unknown_values=1   $v_destination_tbl $v_cloud_storage_path/$v_fileName $v_schema_filepath/$schemaFileName &
    v_load_pids+=" $!"
done

wait $v_load_pids

if [[ "`bq ls $v_dataset_name | awk '{print $1}' | grep \"\b$tableName\b\"`" == "$tableName" ]] ; 
    then 
    # prior table
    echo "Table $v_metadataset_name.$tableName exists";
    v_query="SELECT  Dimension_Name, date, package_name_app_name, sender_id, Dimension_Value, gcm_messages, gcm_registrations   
             FROM (SELECT CONCAT(LOWER(RTRIM(LTRIM(Dimension_Name))), STRING(date)) as dkey, * FROM playstore.stats_gcm) A
             WHERE dkey NOT IN ( SELECT CONCAT(LOWER(RTRIM(LTRIM(Dimension_Name))), STRING(date)) as dkey  FROM [metadata.incremental_stats_gcm] GROUP BY dkey )";
    v_destination_tbl="$v_metadataset_name.prior_$tableName";
    echo "Destination table is $v_destination_tbl and Query is $v_query"
    bq query  --maximum_billing_tier 10 --allow_large_results=1  --quiet -n 0  --replace --destination_table=$v_destination_tbl "$v_query"
    v_load_pids+=" $!"

    else echo "Table $v_dataset_name.$tableName missing"; 

fi

# Merging prior and incremental
v_destination_tbl="$v_metadataset_name.prior_$tableName";
v_query="SELECT * FROM $v_metadataset_name.incremental_$tableName";
bq query --append=1 --maximum_billing_tier 10 --allow_large_results=1 --quiet -n 0 --flatten_results=0 --allow_large_results=1 --destination_table=$v_destination_tbl "$v_query" 
v_load_pids+=" $!"

# Updating the Atom table with updated data
bq rm -f $v_dataset_name.$tableName
v_load_pids+=" $!"
bq cp $v_metadataset_name.prior_$tableName $v_dataset_name.$tableName
v_load_pids+=" $!"
# Removing Prior and Incremental tables
bq rm -f $v_metadataset_name.prior_$tableName &
v_load_pids+=" $!"
bq rm -f $v_metadataset_name.incremental_$tableName &
v_load_pids+=" $!"


v_subtask="Aggregated Reports data update: GCM"
if wait $v_load_pids; then
    echo "Processes $v_load_pids Status: success";
    v_task_status="success";
else 
    echo "Processes $v_load_pids Status: failed";
    v_task_status="failed";
fi

p_exit_upon_error "$v_task_status" "$v_subtask"
# Completed Aggregated Reports: GCM

# Aggregated Reports: Installs
v_report_month=$(date +%Y%m);

tableName=stats_installs

v_destination_tbl="$v_metadataset_name.incremental_$tableName";
schemaFileName=schema_playstore_stats_installs.json
v_load_pids=""

if [[ "`bq ls $v_metadataset_name | awk '{print $1}' | grep \"\bincremental_$tableName\b\"`" == "incremental_$tableName" ]] ; 
	then bq rm $v_destination_tbl;
fi


for i in $PLAY_AGGREGATED_INSTALLS_REPORT_NAMES; do
    v_fileName=${PLAY_AGGREGATED_INSTALLS_REPORT_PREFIX}_${v_report_month}_${i}.csv
    # echo "mv $v_transform_dir/$v_fileName $v_load_dir/"
    mv $v_transform_dir/$v_fileName $v_load_dir/
    v_load_pids+=" $!"
    # echo "gsutil -m cp $v_load_dir/$v_fileName $v_cloud_storage_path"
    gsutil -m cp $v_load_dir/$v_fileName $v_cloud_storage_path
    v_load_pids+=" $!"

    # echo "bq load --quiet --field_delimiter=',' --source_format=CSV --skip_leading_rows=1 --max_bad_records=0  --allow_jagged_rows=1 --allow_quoted_newlines=1 --ignore_unknown_values=1   $v_destination_tbl $v_cloud_storage_path/$v_fileName $v_schema_filepath/$schemaFileName"
    bq load --quiet --field_delimiter=',' --source_format=CSV --skip_leading_rows=1 --max_bad_records=0  --allow_jagged_rows=1 --allow_quoted_newlines=1 --ignore_unknown_values=1   $v_destination_tbl $v_cloud_storage_path/$v_fileName $v_schema_filepath/$schemaFileName &
    v_load_pids+=" $!"
done

wait $v_load_pids

if [[ "`bq ls $v_dataset_name | awk '{print $1}' | grep \"\b$tableName\b\"`" == "$tableName" ]] ; 
    then 
    # prior table
    echo "Table $v_metadataset_name.$tableName exists";
    v_query="SELECT Dimension_Name, date, package_name_app_name, Dimension_Value, current_device_installs, daily_device_installs, daily_device_upgrades, current_user_installs, total_user_installs, daily_user_installs, daily_user_uninstalls
             FROM (SELECT CONCAT(LOWER(RTRIM(LTRIM(Dimension_Name))), STRING(date)) as dkey, * FROM [playstore.stats_installs]) A       
             WHERE dkey NOT IN (SELECT CONCAT(LOWER(RTRIM(LTRIM(Dimension_Name))), STRING(date)) as dkey FROM [metadata.incremental_stats_installs] GROUP BY dkey )";
    v_destination_tbl="$v_metadataset_name.prior_$tableName";
    echo "Destination table is $v_destination_tbl and Query is $v_query"
    bq query  --maximum_billing_tier 10 --allow_large_results=1  --quiet -n 0  --replace --destination_table=$v_destination_tbl "$v_query"
    v_load_pids+=" $!"

    else echo "Table $v_dataset_name.$tableName missing"; 

fi

# Merging prior and incremental
v_destination_tbl="$v_metadataset_name.prior_$tableName";
v_query="SELECT * FROM $v_metadataset_name.incremental_$tableName";
bq query --append=1 --maximum_billing_tier 10 --allow_large_results=1 --quiet -n 0 --flatten_results=0 --allow_large_results=1 --destination_table=$v_destination_tbl "$v_query" 
v_load_pids+=" $!"

# Updating the Atom table with updated data
bq rm -f $v_dataset_name.$tableName
v_load_pids+=" $!"
bq cp $v_metadataset_name.prior_$tableName $v_dataset_name.$tableName
v_load_pids+=" $!"
# Removing Prior and Incremental tables
bq rm -f $v_metadataset_name.prior_$tableName &
v_load_pids+=" $!"
bq rm -f $v_metadataset_name.incremental_$tableName &
v_load_pids+=" $!"


v_subtask="Aggregated Reports data update: GCM"
if wait $v_load_pids; then
    echo "Processes $v_load_pids Status: success";
    v_task_status="success";
else 
    echo "Processes $v_load_pids Status: failed";
    v_task_status="failed";
fi

p_exit_upon_error "$v_task_status" "$v_subtask"

# Completed Aggregated Reports: Installs

# Aggregated Reports: Ratings
v_report_month=$(date +%Y%m);

tableName=stats_ratings
v_destination_tbl="$v_metadataset_name.incremental_$tableName";
schemaFileName=schema_playstore_stats_ratings.json
v_load_pids=""

if [[ "`bq ls $v_metadataset_name | awk '{print $1}' | grep \"\bincremental_$tableName\b\"`" == "incremental_$tableName" ]] ; 
    then bq rm $v_destination_tbl;
fi

for i in $PLAY_AGGREGATED_RATINGS_REPORT_NAMES; do
    v_fileName=${PLAY_AGGREGATED_RATINGS_REPORT_PREFIX}_${v_report_month}_${i}.csv
    # echo "mv $v_transform_dir/$v_fileName $v_load_dir/"
    mv $v_transform_dir/$v_fileName $v_load_dir/
    v_load_pids+=" $!"
    
    # echo "gsutil -m cp $v_load_dir/$v_fileName $v_cloud_storage_path"
    gsutil -m cp $v_load_dir/$v_fileName $v_cloud_storage_path
    v_load_pids+=" $!"

    # echo "bq load --quiet --field_delimiter=',' --source_format=CSV --skip_leading_rows=1 --max_bad_records=0  --allow_jagged_rows=1 --allow_quoted_newlines=1 --ignore_unknown_values=1   $v_destination_tbl $v_cloud_storage_path/$v_fileName $v_schema_filepath/$schemaFileName"
    bq load --quiet --field_delimiter=',' --source_format=CSV --skip_leading_rows=1 --max_bad_records=0  --allow_jagged_rows=1 --allow_quoted_newlines=1 --ignore_unknown_values=1   $v_destination_tbl $v_cloud_storage_path/$v_fileName $v_schema_filepath/$schemaFileName &
    v_load_pids+=" $!"
done

wait $v_load_pids

if [[ "`bq ls $v_dataset_name | awk '{print $1}' | grep \"\b$tableName\b\"`" == "$tableName" ]] ; 
    then 
    # prior table
    echo "Table $v_metadataset_name.$tableName exists";
    v_query="SELECT  Dimension_Name, date, package_name_app_name, Dimension_Value, daily_average_rating, total_average_rating
             FROM (SELECT CONCAT(LOWER(RTRIM(LTRIM(Dimension_Name))), STRING(date)) as dkey, * FROM [playstore.stats_ratings]) A       
             WHERE dkey NOT IN (SELECT CONCAT(LOWER(RTRIM(LTRIM(Dimension_Name))), STRING(date)) as dkey FROM [metadata.incremental_stats_ratings] GROUP BY dkey )";
    v_destination_tbl="$v_metadataset_name.prior_$tableName";
    echo "Destination table is $v_destination_tbl and Query is $v_query"
    bq query  --maximum_billing_tier 10 --allow_large_results=1  --quiet -n 0  --replace --destination_table=$v_destination_tbl "$v_query"

    else echo "Table $v_dataset_name.$tableName missing"; 

fi

# Merging prior and incremental
v_destination_tbl="$v_metadataset_name.prior_$tableName";
v_query="SELECT * FROM $v_metadataset_name.incremental_$tableName";
bq query --append=1 --maximum_billing_tier 10 --allow_large_results=1 --quiet -n 0 --flatten_results=0 --allow_large_results=1 --destination_table=$v_destination_tbl "$v_query" 
v_load_pids+=" $!"

# Updating the Atom table with updated data
bq rm -f $v_dataset_name.$tableName
v_load_pids+=" $!"
bq cp $v_metadataset_name.prior_$tableName $v_dataset_name.$tableName
v_load_pids+=" $!"
# Removing Prior and Incremental tables
bq rm -f $v_metadataset_name.prior_$tableName &
v_load_pids+=" $!"
bq rm -f $v_metadataset_name.incremental_$tableName &
v_load_pids+=" $!"


v_subtask="Aggregated Reports data update: GCM"
if wait $v_load_pids; then
    echo "Processes $v_load_pids Status: success";
    v_task_status="success";
else 
    echo "Processes $v_load_pids Status: failed";
    v_task_status="failed";
fi

p_exit_upon_error "$v_task_status" "$v_subtask"
# Completed Aggregated Reports: Ratings

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


# Creating new file for playstore's ETL run. Content will be appended in further tasks of T and L.
echo -e "$v_log_obj_txt" >> $v_temp_dir/"$v_data_object"_log.log

chmod 0777 $v_temp_dir/"$v_data_object"_log.log;
#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#

echo -e "Log text is: \n"
echo -e "$v_log_obj_txt";

exit 0
