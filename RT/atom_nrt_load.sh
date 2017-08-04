#!/bin/bash
# Script Name: adhoc_sachan_version_post_bom.sh

export TZ=Asia/Calcutta
taskStartTime=`date`
echo "Data processing for marketing started : $taskStartTime "
DBHOST=nb-prod-oms-db-ugd.c6vqep7kcqpl.ap-southeast-1.rds.amazonaws.com
#DBHOST=nb-prod-oms-db-ugd.c6vqep7kcqpl.ap-southeast-1.rds.amazonaws.com
DBPORT=5432
DBNAME=oms_v2
DBUSER=oms
DBPASS=0mspr0d$
DATE=`date +%Y-%m-%d`

# Copying table from Atom to Atom_rt if table is missing

if [[ "`bq ls --max_results=10000 Atom_rt | awk '{print $1}' | grep \"\border_bom\b\"`" == "order_bom" ]] 
    then echo "Order BOM exists in the dataset Atom_rt";

		 v_order_bom_health_check="SELECT IF (CNR_ATOM <= CNR_ATOM_RT, 'GOOD' , 'BAD') as health
FROM 
(SELECT COUNT(3) AS CNR_ATOM, 'order_bom' AS tablename
FROM Atom.order_bom
) a INNER JOIN 
(SELECT COUNT(3) AS CNR_ATOM_RT, 'order_bom' AS tablename
FROM Atom_rt.order_bom 
) b
ON a.tablename = b.tablename";
          echo "Order BOM health check query: $v_order_bom_health_check"

fi

# get last max created time updated time from different tables which need to be loaded
echo "$(/home/ubuntu/google-cloud-sdk/bin/bq query 'select max(createdAt) as lastrun from Atom_rt.order_header')" | sed -n 5p | sed 's/[^0-9]*//g' > /home/ubuntu/modular_ETL/RT/lastordercreatedtime.txt
echo "$(/home/ubuntu/google-cloud-sdk/bin/bq query 'select max(updatedAt) as lastrun from Atom_rt.order_header')" | sed -n 5p | sed 's/[^0-9]*//g' > /home/ubuntu/modular_ETL/RT/lastorderupdatedtime.txt
echo "$(/home/ubuntu/google-cloud-sdk/bin/bq query 'select max(createdAt) as lastrun from Atom_rt.order_line_new')" | sed -n 5p | sed 's/[^0-9]*//g' > /home/ubuntu/modular_ETL/RT/lastorderlinecreatedtime.txt
echo "$(/home/ubuntu/google-cloud-sdk/bin/bq query 'select max(updatedAt) as lastrun from Atom_rt.order_line_new')" | sed -n 5p | sed 's/[^0-9]*//g' > /home/ubuntu/modular_ETL/RT/lastorderlineupdatedtime.txt
echo "$(/home/ubuntu/google-cloud-sdk/bin/bq query 'select max(createdAt) as lastrun from Atom_rt.order_bom')" | sed -n 5p | sed 's/[^0-9]*//g' > /home/ubuntu/modular_ETL/RT/lastorderbomcreatedtime.txt
echo "$(/home/ubuntu/google-cloud-sdk/bin/bq query 'select max(updatedAt) as lastrun from Atom_rt.order_bom')" | sed -n 5p | sed 's/[^0-9]*//g' > /home/ubuntu/modular_ETL/RT/lastorderbomupdatedtime.txt
echo "$(/home/ubuntu/google-cloud-sdk/bin/bq query 'select max(createdAt) as lastrun from Atom_rt.product')" | sed -n 5p | sed 's/[^0-9]*//g' > /home/ubuntu/modular_ETL/RT/lastproductcreatedtime.txt
echo "$(/home/ubuntu/google-cloud-sdk/bin/bq query 'select max(updatedAt) as lastrun from Atom_rt.product')" | sed -n 5p | sed 's/[^0-9]*//g' > /home/ubuntu/modular_ETL/RT/lastproductupdatedtime.txt
#echo "$(bq query 'select max(createdAt) as lastrun from Atom_rt.customer')" | sed -n 5p | sed 's/[^0-9]*//g' > /home/ubuntu/modular_ETL/RT/lastcustomercreatedtime.txt
#echo "$(bq query 'select max(lastModifiedAt) as lastrun from Atom_rt.customer')" | sed -n 5p | sed 's/[^0-9]*//g' > /home/ubuntu/modular_ETL/RT/lastcustomerupdatedtime.txt


LAST_OH_CREATEDTIME=$(($(head -1 /home/ubuntu/modular_ETL/RT/lastordercreatedtime.txt)));
LAST_OH_UPDATEDTIME=$(($(head -1 /home/ubuntu/modular_ETL/RT/lastorderupdatedtime.txt)));

LAST_OL_CREATEDTIME=$(($(head -1 /home/ubuntu/modular_ETL/RT/lastorderlinecreatedtime.txt)));
LAST_OL_UPDATEDTIME=$(($(head -1 /home/ubuntu/modular_ETL/RT/lastorderlineupdatedtime.txt)));

LAST_OB_CREATEDTIME=$(($(head -1 /home/ubuntu/modular_ETL/RT/lastorderbomcreatedtime.txt)));
LAST_OB_UPDATEDTIME=$(($(head -1 /home/ubuntu/modular_ETL/RT/lastorderbomupdatedtime.txt)));

LAST_PR_CREATEDTIME=$(($(head -1 /home/ubuntu/modular_ETL/RT/lastproductcreatedtime.txt)));
LAST_PR_UPDATEDTIME=$(($(head -1 /home/ubuntu/modular_ETL/RT/lastproductupdatedtime.txt)));
#LAST_CUST_CREATEDTIME=$(($(head -1 /home/ubuntu/modular_ETL/RT/lastcustomercreatedtime.txt)))
#LAST_CUST_UPDATEDTIME=$(($(head -1 /home/ubuntu/modular_ETL/RT/lastcustomerupdatedtime.txt)))


echo "last order header entry was created at: $LAST_OH_CREATEDTIME";
echo "last order header entry was updated at: $LAST_OH_UPDATEDTIME";

echo "last order line entry was created at: $LAST_OL_CREATEDTIME";
echo "last order line entry was updated at: $LAST_OL_UPDATEDTIME";

echo "last order BOM entry was created at: $LAST_OB_CREATEDTIME";
echo "last order BOM entry was updated at: $LAST_OB_UPDATEDTIME";

echo "last product entry was created at: $LAST_PR_CREATEDTIME";
echo "last product entry was updated at: $LAST_PR_UPDATEDTIME";

#echo "last customer entry was created at: $LAST_CUST_CREATEDTIME"
#echo "last customer  entry was updated at: $LAST_CUST_UPDATEDTIME"

export PGPASSWORD='0mspr0d$'
/usr/bin/psql -d $DBNAME -h $DBHOST -p $DBPORT -U $DBUSER <<EOF
\copy (select * from oms_data.orderheader where createdAt > $LAST_OH_CREATEDTIME or updatedAt> $LAST_OH_UPDATEDTIME )  to /home/ubuntu/modular_ETL/RT/ohExport.csv with DELIMITER ',' CSV HEADER;
\copy (select * from oms_data.orderline where createdAt > $LAST_OL_CREATEDTIME or updatedAt> $LAST_OL_UPDATEDTIME ) to /home/ubuntu/modular_ETL/RT/olExport.csv with DELIMITER ',' CSV HEADER;
\copy (select * from oms_data.orderbom where createdAt > $LAST_OB_CREATEDTIME or updatedAt> $LAST_OB_UPDATEDTIME ) to /home/ubuntu/modular_ETL/RT/oBOMExport.csv with DELIMITER ',' CSV HEADER;
\copy (select * from oms_data.product where createdAt > $LAST_PR_CREATEDTIME or updatedAt> $LAST_PR_UPDATEDTIME ) to /home/ubuntu/modular_ETL/RT/productExport.csv with DELIMITER ',' CSV HEADER;
EOF

#query="{\$or:[{\"createdAt\":{\$gt:$LAST_CUST_CREATEDTIME}},{\"lastModifiedAt\":{\$gt:$LAST_CUST_UPDATEDTIME}}]}"
#echo $query
#cd /home/ubuntu/mongo_cp/bin
#./mongoexport --host 10.2.4.15:27017 --db nearbuy_customer_profile -q $query -c customer  --out /home/ubuntu/modular_ETL/RT/customerProfileExport.json

#gzip $1/customerProfileExport.json
/home/ubuntu/google-cloud-sdk/bin/gsutil -m cp -r /home/ubuntu/modular_ETL/RT/*Export* gs://nb_rt

/home/ubuntu/google-cloud-sdk/bin/bq load --field_delimiter=',' --source_format=CSV --skip_leading_rows=1 --max_bad_records=0  --allow_jagged_rows=1 --allow_quoted_newlines=1 --ignore_unknown_values=1  Atom_rt.order_header_temp gs://nb_rt/ohExport.csv /home/ubuntu/modular_ETL/schema_files/schema_order_header.json

/home/ubuntu/google-cloud-sdk/bin/bq load --field_delimiter=',' --source_format=CSV --skip_leading_rows=1 --max_bad_records=0  --allow_jagged_rows=1 --allow_quoted_newlines=1 --ignore_unknown_values=1  Atom_rt.order_line_new_temp gs://nb_rt/olExport.csv /home/ubuntu/modular_ETL/schema_files/schema_order_line.json

#BOM
/home/ubuntu/google-cloud-sdk/bin/bq load --field_delimiter=',' --source_format=CSV --skip_leading_rows=1 --max_bad_records=0  --allow_jagged_rows=1 --allow_quoted_newlines=1 --ignore_unknown_values=1  Atom_rt.order_bom_temp gs://nb_rt/oBOMExport.csv /home/ubuntu/modular_ETL/schema_files/schema_orderbom.json

#Product
/home/ubuntu/google-cloud-sdk/bin/bq load --field_delimiter=',' --source_format=CSV --skip_leading_rows=1 --max_bad_records=0  --allow_jagged_rows=1 --allow_quoted_newlines=1 --ignore_unknown_values=1  Atom_rt.product_temp gs://nb_rt/productExport.csv /home/ubuntu/modular_ETL/schema_files/schema_product.json

#bq  load  --source_format=NEWLINE_DELIMITED_JSON --ignore_unknown_values=1 --max_bad_records=0 Atom_rt.customer_temp gs://nb_rt/customerProfileExport.json /home/ubuntu/modular_ETL/schema_files/schema_customer.json

/home/ubuntu/google-cloud-sdk/bin/bq query --replace --allow_large_results=1 --destination_table=Atom_rt.order_header_final 'select * from Atom_rt.order_header where orderid not in (select orderid from Atom_rt.order_header_temp)' > /dev/null
/home/ubuntu/google-cloud-sdk/bin/bq query --append=1 --allow_large_results=1 --destination_table=Atom_rt.order_header_final 'select * from Atom_rt.order_header_temp' > /dev/null
/home/ubuntu/google-cloud-sdk/bin/bq rm -f Atom_rt.order_header
/home/ubuntu/google-cloud-sdk/bin/bq cp Atom_rt.order_header_final Atom_rt.order_header
/home/ubuntu/google-cloud-sdk/bin/bq rm -f Atom_rt.order_header_final
/home/ubuntu/google-cloud-sdk/bin/bq rm -f Atom_rt.order_header_temp

/home/ubuntu/google-cloud-sdk/bin/bq query --replace --allow_large_results=1 --destination_table=Atom_rt.order_line_new_final 'select * from Atom_rt.order_line_new where orderlineid not in (select orderlineid from Atom_rt.order_line_new_temp)' > /dev/null
/home/ubuntu/google-cloud-sdk/bin/bq query --append=1 --allow_large_results=1 --destination_table=Atom_rt.order_line_new_final 'select * from Atom_rt.order_line_new_temp' > /dev/null
/home/ubuntu/google-cloud-sdk/bin/bq rm -f Atom_rt.order_line_new
/home/ubuntu/google-cloud-sdk/bin/bq cp Atom_rt.order_line_new_final Atom_rt.order_line_new
/home/ubuntu/google-cloud-sdk/bin/bq rm -f Atom_rt.order_line_new_final
/home/ubuntu/google-cloud-sdk/bin/bq rm -f Atom_rt.order_line_new_temp

# BOM
/home/ubuntu/google-cloud-sdk/bin/bq query --replace --allow_large_results=1 --destination_table=Atom_rt.order_bom_final 'select * from Atom_rt.order_bom where orderbomid not in (select orderbomid from Atom_rt.order_bom_temp)' > /dev/null
/home/ubuntu/google-cloud-sdk/bin/bq query --append=1 --allow_large_results=1 --destination_table=Atom_rt.order_bom_final 'select * from Atom_rt.order_bom_temp' > /dev/null
/home/ubuntu/google-cloud-sdk/bin/bq rm -f Atom_rt.order_bom
/home/ubuntu/google-cloud-sdk/bin/bq cp Atom_rt.order_bom_final Atom_rt.order_bom
/home/ubuntu/google-cloud-sdk/bin/bq rm -f Atom_rt.order_bom_final
/home/ubuntu/google-cloud-sdk/bin/bq rm -f Atom_rt.order_bom_temp

# Product
/home/ubuntu/google-cloud-sdk/bin/bq query --replace --allow_large_results=1 --destination_table=Atom_rt.product_final 'select * from Atom_rt.product where productid not in (select productid from Atom_rt.product_temp)' > /dev/null
/home/ubuntu/google-cloud-sdk/bin/bq query --append=1 --allow_large_results=1 --destination_table=Atom_rt.product_final 'select * from Atom_rt.product_temp' > /dev/null
/home/ubuntu/google-cloud-sdk/bin/bq rm -f Atom_rt.product
/home/ubuntu/google-cloud-sdk/bin/bq cp Atom_rt.product_final Atom_rt.product
/home/ubuntu/google-cloud-sdk/bin/bq rm -f Atom_rt.product_final
/home/ubuntu/google-cloud-sdk/bin/bq rm -f Atom_rt.product_temp

#bq query --allow_large_results=1 --flatten_results=0 --destination_table=Atom_rt.customer_final 'select * from Atom_rt.customer where customerId not in (select customerId from Atom_rt.customer_temp)' > /dev/null
#bq query --append=1 --allow_large_results=1 --flatten_results=0 --destination_table=Atom_rt.customer_final 'select * from Atom_rt.customer_temp' > /dev/null
#bq rm -f Atom_rt.customer
#bq cp Atom_rt.customer_final Atom_rt.customer
#bq rm -f Atom_rt.customer_final
#bq rm -f Atom_rt.customer_temp


taskEndTime=`date`
echo "Data processing for marketing ended at: $taskEndTime "
exit 0
