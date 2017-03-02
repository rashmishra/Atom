#!/bin/bash

# Purpose: Runtime OMS refresh - Transform BOM data into old Orderline format
# Script Name: transform_bom_to_orderline_rt.sh
# Author: Ranganath






## Step 1: Find the delta rows in BOM by fetching latest createdat and updated from Atom_rt.order_line

v_query="SELECT
  tbl1.orderlineid AS orderlineid,
  CAST (null AS STRING) AS cancellationpolicies,
  CAST (null AS STRING) AS cda,
  tbl2.createdat AS createdat,
  tbl2.createdby AS createdby,
  CAST (null AS INTEGER) AS endat,
  expiresat,
  finalprice,
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
  unitprice,
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
  bookingdate,
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
FROM [Atom_rt.order_bom]   tbl1
INNER JOIN [Atom_rt.order_line_new]  tbl2
         ON tbl1.productId = tbl2.productId
WHERE tbl2.createdat >= (SELECT MAX(createdat) - 100 FROM Atom_rt.order_line)
   OR tbl2.updatedat >= (SELECT MAX(updatedat) - 100 FROM Atom_rt.order_line)";


v_dataset_name=Atom_rt;

tableName=incremental_order_line_recreated
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query"


## Step 2: Create Prior table which contains the orderline records created prior/ apart to/ from the incremental table's records

v_query="SELECT * FROM Atom_rt.order_line WHERE orderlineid NOT IN (SELECT orderlineid FROM Atom_rt.incremental_order_line_recreated)";

v_dataset_name=Atom_rt;
tableName=prior_order_line
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query";


## Step 3: Appending the incremental data to prior table. (UNION operation)
v_query="SELECT * FROM Atom_rt.incremental_order_line_recreated";

v_dataset_name=Atom_rt;
tableName=prior_order_line
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --append -n 1 --destination_table=$v_destination_tbl \"$v_query\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --append -n 0 --destination_table=$v_destination_tbl "$v_query";




# Step 4: Copy the combined data of incremental and prior lying in prior table to main table
bq cp -r Atom_rt.prior_order_line Atom_rt.order_line
bq rm Atom_rt.prior_order_line;
bq rm Atom_rt.incremental_order_line_recreated;

exit 0;