#!/bin/bash

###############################################################################
## Script Name: masterenv.sh
## Author: Ranganath
## Date: 27-May-2016
## Purpose: Modularize the ETL process: Environment variables are stored here.
###############################################################################



# Changing time zone to local
TZ=Asia/Calcutta

HOME_DIR=/home/ubuntu

## Home folder of all ETL procedure
ETL_HOME_DIR=/home/ubuntu/modular_ETL

# Epoch of 1st Jan 2015 00:00:00 IST in seconds
FULL_LOAD_EPOCH=1420050600


## Job Configuration parameters
CONFIG_DIR="$ETL_HOME_DIR/config";

CONFIG_FILE_NAME=$CONFIG_DIR/config.csv

# Name of cloud bucket
CLOUD_BUCKET_NAME=nb_batman_begins_dev

# Path where daily exported files will be stores in google cloud
CLOUD_DAILY_STORAGE_PATH=gs://$CLOUD_BUCKET_NAME/dailydump
CLOUD_MIXPANEL_STORAGE_PATH=gs://$CLOUD_BUCKET_NAME/mixpanel
CLOUD_APPSFLYER_STORAGE_PATH=gs://$CLOUD_BUCKET_NAME/dailydump
CLOUD_HASOFFER_STORAGE_PATH=gs://$CLOUD_BUCKET_NAME/dailydump
CLOUD_UNINSTALLIO_STORAGE_PATH=gs://$CLOUD_BUCKET_NAME/dailydump
CLOUD_CHEETAH_STORAGE_PATH=gs://$CLOUD_BUCKET_NAME/nb_cheetah
CLOUD_ADWORDS_STORAGE_PATH=gs://$CLOUD_BUCKET_NAME/nb_adwords
CLOUD_PLAYSTORE_STORAGE_PATH=gs://$CLOUD_BUCKET_NAME/playstore
CLOUD_PAYMENT_GATEWAYS_STORAGE_PATH=gs://$CLOUD_BUCKET_NAME/payment_gateways
CLOUD_CEREBRO_STORAGE_PATH=gs://$CLOUD_BUCKET_NAME/cerebro
CLOUD_CASHBACK_STORAGE_PATH=gs://$CLOUD_BUCKET_NAME/dailydump
CLOUD_EVENT_STORAGE_PATH=gs://$CLOUD_BUCKET_NAME/dailydump

# Name of datasets where the tables will be created/refreshed
PRIME_DATASET_NAME=Atom
METADATA_DATASET_NAME=metadata
APPSFLYER_DATASET_NAME=appsflyer
MIXPANEL_DATASET_NAME=mixpanel
CHEETAH_DATASET_NAME=cheetah
UNINSTALLIO_DATASET_NAME=uninstallio
HASOFFER_DATASET_NAME=hasoffer
CHEETAH_DATASET_NAME=cheetah
ADWORDS_DATASET_NAME=adwords
PLAYSTORE_DATASET_NAME=playstore
PAYMENT_GATEWAYS_DATASET_NAME=payment_gateways
PAYU_DATASET_NAME=payu
PAYTM_DATASET_NAME=paytm
MOBIKWIK_DATASET_NAME=mobikwik
CEREBRO_DATASET_NAME=cerebro
CUSTOMER_COHORT_DATASET_NAME=customer_cohort



## Directories used in ETL Flow
LOGS_DIR="$ETL_HOME_DIR/logs";
TEMP_DIR="$ETL_HOME_DIR/temp";
ARCHIVES_DIR="$ETL_HOME_DIR/arch";

# Path where the data export will take place as per the frequency
DAILY_DUMP_PATH=$ETL_HOME_DIR/data/extract
DAILY_TRANSFORM_PATH=$ETL_HOME_DIR/data/transform
DAILY_LOAD_PATH=$ETL_HOME_DIR/data/load

# Path of the folder where all the scripts are present
SCRIPTS_PATH=$ETL_HOME_DIR/master_scripts
SCHEMA_FILE_PATH=$ETL_HOME_DIR/schema_files
EXPORT_SCRIPTS_PATH=$ETL_HOME_DIR/export_scripts
TRANSFORM_SCRIPTS_PATH=$ETL_HOME_DIR/transform_scripts
LOAD_SCRIPTS_PATH=$ETL_HOME_DIR/load_scripts


# Path to Mongo executables
MONGO_PATH=/home/ubuntu/mongo_cp/bin


ADWORDS_YAML_FILENAMES=( "googleads_1.yaml" "googleads_2.yaml" "googleads_3.yaml" )

## PLaystore Varaibles
PLAY_BUCKET=gs://pubsite_prod_rev_04054290636169056455
PLAY_APP_NAME=com.nearbuy.nearbuymobile

PLAY_DETAILED_REPORT_NAMES=( "crashes/crashes_${PLAY_APP_NAME} crashes/anrs_${PLAY_APP_NAME} reviews/reviews_${PLAY_APP_NAME}" )

PLAY_TRANSFORM_DETAILED_REPORT_NAMES=( "crashes_${PLAY_APP_NAME} anrs_${PLAY_APP_NAME} reviews_${PLAY_APP_NAME}" )

PLAY_AGGREGATED_CRASHES_REPORT_NAMES=( "app_version device os_version overview tablets" )
PLAY_AGGREGATED_CRASHES_REPORT_FOLDER="${PLAY_BUCKET}/stats/crashes"
PLAY_AGGREGATED_CRASHES_REPORT_PREFIX="crashes_${PLAY_APP_NAME}"

PLAY_AGGREGATED_GCM_REPORT_NAMES=("app_version carrier country device language message_status os_version overview response_code tablets") 
PLAY_AGGREGATED_GCM_REPORT_FOLDER="${PLAY_BUCKET}/stats/gcm"
PLAY_AGGREGATED_GCM_REPORT_PREFIX="gcm_${PLAY_APP_NAME}"

PLAY_AGGREGATED_INSTALLS_REPORT_NAMES=("app_version carrier country device language os_version tablets")
PLAY_AGGREGATED_INSTALLS_REPORT_FOLDER="${PLAY_BUCKET}/stats/installs"
PLAY_AGGREGATED_INSTALLS_REPORT_PREFIX="installs_${PLAY_APP_NAME}"

PLAY_AGGREGATED_RATINGS_REPORT_NAMES=("app_version carrier country device language os_version tablets")
PLAY_AGGREGATED_RATINGS_REPORT_FOLDER="${PLAY_BUCKET}/stats/ratings"
PLAY_AGGREGATED_RATINGS_REPORT_PREFIX="ratings_${PLAY_APP_NAME}"

#<gsutil_report_bucket>/<report_folder>/<report_name>_<chosen_application_name>_YYYYMM.csv
#<gsutil_report_bucket>/<report_folder>/<report_name>_<chosen_application_name>_YYYYMM[_<subreport_name>].csv

# Payment Gateways Variables
PAYU_FILE_NAME=payu.json
PAYTM_FILE_NAME=paytm.json
MOBIKWIK_FILE_NAME=mobikwik.json
