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

        mail -s "PROD Machine | Task ($v_subtask) failed in Atom wrapper script"   snehesh.mitra@nearbuy.com  rashmi.mishra@nearbuy.com sairanganath.v@nearbuy.com < /dev/null ;

        exit 1;

    fi

}

# Step 1

echo `date` "Atom ETL master script invoked from Wrapper";

mail -s "PROD Machine | Atom ETL master script invoked."  snehesh.mitra@nearbuy.com  rashmi.mishra@nearbuy.com sairanganath.v@nearbuy.com < /dev/null

bash /home/ubuntu/modular_ETL/master_scripts/master_ETL.sh >> /home/ubuntu/modular_ETL/arch/logs/modular_log-${v_run_date}.log 2>&1
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Atom ETL master script Status called from Wrapper: $v_task_status";


v_subtask="Atom ETL master script";
p_exit_upon_error "$v_task_status" "$v_subtask";

mail -s "PROD Machine | Atom ETL master script completed. Invoking Transformation of BOM to old OL"  snehesh.mitra@nearbuy.com  rashmi.mishra@nearbuy.com sairanganath.v@nearbuy.com < /dev/null


# Step 2
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



if [ "$v_status" == "success" ]; 
  then 
    echo `date` "Task: $v_subtask ended with status $v_task_status." | mail -s "PROD Machine | Wrapper script calling ETL and Transform OL completed: $v_task_status"  snehesh.mitra@nearbuy.com  rashmi.mishra@nearbuy.com sairanganath.v@nearbuy.com;
  else 
    echo `date` "Task: $v_subtask ended with status $v_task_status." | mail -s "PROD Machine | Wrapper script calling ETL and Transform OL completed: $v_task_status"  snehesh.mitra@nearbuy.com  rashmi.mishra@nearbuy.com sairanganath.v@nearbuy.com;
    exit 1;
fi


exit 0