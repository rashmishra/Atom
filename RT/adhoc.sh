#!/bin/bash
export TZ=Asia/Calcutta
taskStartTime=`date`
echo "Data processing for marketing started : $taskStartTime "
#DBHOST=nb-prod-oms-db-ugd-read.c6vqep7kcqpl.ap-southeast-1.rds.amazonaws.com
DBHOST=nb-prod-oms-db-ugd.c6vqep7kcqpl.ap-southeast-1.rds.amazonaws.com
DBPORT=5432
DBNAME=oms
DBUSER=oms
DBPASS=0mspr0d$
DATE=`date +%Y-%m-%d`

# get last max created time updated time from different tables which need to be loaded
echo "$(bq query 'select max(createdAt) as lastrun from Atom_rt.order_header')" | sed -n 5p | sed 's/[^0-9]*//g' > /home/ubuntu/RT/lastordercreatedtime.txt
echo "$(bq query 'select max(updatedAt) as lastrun from Atom_rt.order_header')" | sed -n 5p | sed 's/[^0-9]*//g' > /home/ubuntu/RT/lastorderupdatedtime.txt
echo "$(bq query 'select max(createdAt) as lastrun from Atom_rt.order_line')" | sed -n 5p | sed 's/[^0-9]*//g' > /home/ubuntu/RT/lastorderlinecreatedtime.txt
echo "$(bq query 'select max(updatedAt) as lastrun from Atom_rt.order_line')" | sed -n 5p | sed 's/[^0-9]*//g' > /home/ubuntu/RT/lastorderlineupdatedtime.txt
#echo "$(bq query 'select max(createdAt) as lastrun from Atom_rt.customer')" | sed -n 5p | sed 's/[^0-9]*//g' > /home/ubuntu/RT/lastcustomercreatedtime.txt
#echo "$(bq query 'select max(lastModifiedAt) as lastrun from Atom_rt.customer')" | sed -n 5p | sed 's/[^0-9]*//g' > /home/ubuntu/RT/lastcustomerupdatedtime.txt


LAST_OH_CREATEDTIME=$(($(head -1 /home/ubuntu/RT/lastordercreatedtime.txt)))
LAST_OH_UPDATEDTIME=$(($(head -1 /home/ubuntu/RT/lastorderupdatedtime.txt)))
LAST_OL_CREATEDTIME=$(($(head -1 /home/ubuntu/RT/lastorderlinecreatedtime.txt)))
LAST_OL_UPDATEDTIME=$(($(head -1 /home/ubuntu/RT/lastorderlineupdatedtime.txt)))
#LAST_CUST_CREATEDTIME=$(($(head -1 /home/ubuntu/RT/lastcustomercreatedtime.txt)))
#LAST_CUST_UPDATEDTIME=$(($(head -1 /home/ubuntu/RT/lastcustomerupdatedtime.txt)))


echo "last order header entry was created at : $LAST_OH_CREATEDTIME"
echo "last order header entry was updated at: $LAST_OH_UPDATEDTIME"
echo "last order line entry was created at : $LAST_OL_CREATEDTIME"
echo "last order line entry was updated at: $LAST_OL_UPDATEDTIME"
#echo "last customer entry was created at : $LAST_CUST_CREATEDTIME"
#echo "last customer  entry was updated at: $LAST_CUST_UPDATEDTIME"

export PGPASSWORD='0mspr0d$'
sudo psql -d $DBNAME -h $DBHOST -p $DBPORT -U $DBUSER <<EOF
\copy (select * from oms_data.orderheader where createdAt > $LAST_OH_CREATEDTIME or updatedAt> $LAST_OH_UPDATEDTIME )  to /home/ubuntu/RT/ohExport.csv with DELIMITER ',' CSV HEADER;
\copy (select * from oms_data.orderline where createdAt > $LAST_OL_CREATEDTIME or updatedAt> $LAST_OL_UPDATEDTIME ) to /home/ubuntu/RT/olExport.csv with DELIMITER ',' CSV HEADER;
EOF

#query="{\$or:[{\"createdAt\":{\$gt:$LAST_CUST_CREATEDTIME}},{\"lastModifiedAt\":{\$gt:$LAST_CUST_UPDATEDTIME}}]}"
#echo $query
#cd /home/ubuntu/mongo_cp/bin
#sudo ./mongoexport --host 10.2.4.15:27017 --db nearbuy_customer_profile -q $query -c customer  --out /data/RT/customerProfileExport.json

#gzip $1/customerProfileExport.json
gsutil -m cp -r /home/ubuntu/RT/*Export* gs://nb_rt

bq load --field_delimiter=',' --source_format=CSV --skip_leading_rows=1 --max_bad_records=0  --allow_jagged_rows=1 --allow_quoted_newlines=1 --ignore_unknown_values=1  Atom_rt.order_header_temp gs://nb_rt/ohExport.csv /home/ubuntu/RT/schema_order_header.json

bq load --field_delimiter=',' --source_format=CSV --skip_leading_rows=1 --max_bad_records=0  --allow_jagged_rows=1 --allow_quoted_newlines=1 --ignore_unknown_values=1  Atom_rt.order_line_temp gs://nb_rt/olExport.csv /home/ubuntu/RT/schema_order_line.json

#bq  load  --source_format=NEWLINE_DELIMITED_JSON --ignore_unknown_values=1 --max_bad_records=0 Atom_rt.customer_temp gs://nb_rt/customerProfileExport.json /home/ubuntu/RT/schema_customer.json

bq query --allow_large_results=1 --destination_table=Atom_rt.order_header_final 'select * from Atom_rt.order_header where orderid not in (select orderid from Atom_rt.order_header_temp)' > /dev/null
bq query --append=1 --allow_large_results=1 --destination_table=Atom_rt.order_header_final 'select * from Atom_rt.order_header_temp' > /dev/null
bq rm -f Atom_rt.order_header
bq cp Atom_rt.order_header_final Atom_rt.order_header
bq rm -f Atom_rt.order_header_final
bq rm -f Atom_rt.order_header_temp

bq query --allow_large_results=1 --destination_table=Atom_rt.order_line_final 'select * from Atom_rt.order_line where orderid not in (select orderid from Atom_rt.order_line_temp)' > /dev/null
bq query --append=1 --allow_large_results=1 --destination_table=Atom_rt.order_line_final 'select * from Atom_rt.order_line_temp' > /dev/null
bq rm -f Atom_rt.order_line
bq cp Atom_rt.order_line_final Atom_rt.order_line
bq rm -f Atom_rt.order_line_final
bq rm -f Atom_rt.order_line_temp

#bq query --allow_large_results=1 --flatten_results=0 --destination_table=Atom_rt.customer_final 'select * from Atom_rt.customer where customerId not in (select customerId from Atom_rt.customer_temp)' > /dev/null
#bq query --append=1 --allow_large_results=1 --flatten_results=0 --destination_table=Atom_rt.customer_final 'select * from Atom_rt.customer_temp' > /dev/null
#bq rm -f Atom_rt.customer
#bq cp Atom_rt.customer_final Atom_rt.customer
#bq rm -f Atom_rt.customer_final
#bq rm -f Atom_rt.customer_temp


taskEndTime=`date`
echo "Data processing for marketing ended at : $taskEndTime "
exit 0
