#!/bin/bash

# Purpose: Transform BOM data into old Orderline format
# Script Name: transform_bom_to_orderline.sh
# Author: Ranganath

taskStartTime=`date`;

p_exit_upon_error(){

    ## Parameters
    # $1: Task Status (passed/ failed)
    # $2: Sub Task (e.g. Extraction of data, Cloud upload, metadata table creation, final table population)

    v_task_status="$1";
    v_subtask="$2";


    if [ "$v_task_status" == "failed" ] ; then
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




## Step 1: Find the delta rows in BOM by fetching latest createdat and updated from Atom.order_line

v_query="SELECT
  orderlineid AS orderlineid,
  CAST (null AS STRING) AS cancellationpolicies,
  CAST (null AS STRING) AS cda,
  tbl2.createdat AS createdat,
  tbl2.createdby AS createdby,
  CAST (null AS INTEGER) AS endat,
  expiresat,
  CAST(finalprice AS INTEGER) AS finalprice,
  fineprint,
  highlightsection,
  imageurl,
  iscancellable,
  isconfirmationrequired,
  isshippingrequired,
  isvoucherrequired,
  marginpercentage,
  merchantid,
  merchantname,
  offerid,
  offertitle,
  tbl2.orderid AS orderid,
  partnernumber,
  paymentterms,
  productshortdesc,
  quantity,
  redemptionbyid,
  redemptionbyemail,
  redemptionbyname,
  redemptionbyrole,
  redemptiondate,
  STATUS AS status,
  title,
  CAST(unitprice AS INTEGER) AS unitprice,
  tbl2.updatedat AS updatedat,
  tbl2.updatedby AS updatedby,
  validfrom,
  vertical,
  vouchercode,
  voucherid,
  voucherstatus,
  voucherurl,
  whatyouget,
  offerdescription,
  CAST (null AS STRING) AS paymenttermbreackage,
  CAST (null AS STRING) AS paymenttermfrequency,
  CAST (null AS STRING) AS paymenttermtrigger,
  CAST (null AS STRING) AS paymenttermtype,
  bookingrequestid,
  CAST (null AS STRING) AS paymentnumber,
  paymentstatus,
  bookedat,
  paidat,
  cancelledat,
  closedat,
  isbooked,
  iscancelled,
  isopen,
  ispaid,
  ismerchantcoderequired,
  isautoredeem,
  merchantcode,
  redemptionlat,
  redemptionlong,
  CAST(bookingdate AS STRING) AS bookingdate,
  bookingtimeslot,
  cancellationpolicyid,
  paymenttermid,
  CAST (null AS STRING) AS senttoaps,
  dealId as dealid,
  categoryid,
  flatcommission,
  exclusions,
  nearbuymenu,
  termsandconditions,
  expiredat,
  acceptedat,
  ispaidtomerchant,
  cashbackamount
FROM [Atom.order_bom]   tbl1
INNER JOIN [Atom.order_line_new]  tbl2
         ON tbl1.productId = tbl2.productId
WHERE tbl2.createdat >= (SELECT MAX(createdat) - 100 FROM Atom.order_line)
   OR tbl2.updatedat >= (SELECT MAX(updatedat) - 100 FROM Atom.order_line)";


v_dataset_name=Atom;

tableName=incremental_order_line_recreated
v_destination_tbl="$v_dataset_name.${tableName}";
echo "/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query\""
/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query"
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` " Task Incremental order_line_recreated: $v_task_status";


v_subtask="Transformation Step 1: Incremental order_line_recreated creation";
p_exit_upon_error "$v_task_status" "$v_subtask";




## Step 2: Create Prior table which contains the orderline records created prior/ apart to/ from the incremental table's records

v_query="SELECT * FROM Atom.order_line WHERE orderlineid NOT IN (SELECT orderlineid FROM Atom.incremental_order_line_recreated)";

v_dataset_name=Atom;
tableName=prior_order_line
v_destination_tbl="$v_dataset_name.${tableName}";
echo "/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query\""
/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query";
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` " Task prior_order_line creation: $v_task_status";


v_subtask="Transformation Step 2: prior_order_line creation";
p_exit_upon_error "$v_task_status" "$v_subtask";



## Step 3: Appending the incremental data to prior table. (UNION operation)
v_query="SELECT * FROM Atom.incremental_order_line_recreated";

v_dataset_name=Atom;
tableName=prior_order_line
v_destination_tbl="$v_dataset_name.${tableName}";
echo "/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 100 --allow_large_results=1  --append -n 1 --destination_table=$v_destination_tbl \"$v_query\""
/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 100 --allow_large_results=1 --append -n 0 --destination_table=$v_destination_tbl "$v_query";
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` " Task UNION of prior_order_line and incremental: $v_task_status";


v_subtask="Transformation Step 3: UNION of prior_order_line and incremental";
p_exit_upon_error "$v_task_status" "$v_subtask";




# Step 4: Copy the combined data of incremental and prior lying in prior table to main table
/home/ubuntu/google-cloud-sdk/bin/bq cp -f Atom.prior_order_line Atom.order_line
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` " Task Copy UNION result to main table: $v_task_status";


v_subtask="Transformation Step 4: Copy UNION result to main table";
p_exit_upon_error "$v_task_status" "$v_subtask";


/home/ubuntu/google-cloud-sdk/bin/bq rm Atom.prior_order_line;
/home/ubuntu/google-cloud-sdk/bin/bq rm Atom.incremental_order_line_recreated;

if [ "$v_status" == "success" ]; 
  then 
    echo `date` "Task: #v_subtask ended with status $v_task_status." | mail -s "Transformation of BOM to OL completed: $v_task_status" sairanganath.v@nearbuy.com;
  else 
    echo `date` "Task: #v_subtask ended with status $v_task_status." | mail -s "Transformation of BOM to OL completed: $v_task_status" sairanganath.v@nearbuy.com;
    exit 1;
fi

echo "ETL Wrapper Script started at " $taskStartTime " and ended at " `date`;

exit 0;