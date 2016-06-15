#!/bin/bash

###############################################################################
## Script Name: masterenv.sh
## Author: Ranganath
## Date: 27-May-2016
## Purpose: Modularize the ETL process: Environment variables are stored here.
###############################################################################



# Changing time zone to local
TZ=Asia/Calcutta

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
CLOUD_MIXPANEL_STORAGE_PATH=gs://$CLOUD_BUCKET_NAME/dailydump
CLOUD_HASOFFER_STORAGE_PATH=gs://$CLOUD_BUCKET_NAME/dailydump
CLOUD_UNINSTALLIO_STORAGE_PATH=gs://$CLOUD_BUCKET_NAME/dailydump
CLOUD_CHEETAH_STORAGE_PATH=gs://$CLOUD_BUCKET_NAME/cheetah

# Name of datasets where the tables will be created/refreshed
PRIME_DATASET_NAME=Atom
METADATA_DATASET_NAME=metadata
APPSFLYER_DATASET_NAME=appsflyer
MIXPANEL_DATASET_NAME=mixpanel
CHEETAH_DATASET_NAME=cheetah
UNINSTALLIO_DATASET_NAME=uninstallio
HASOFFER_DATASET_NAME=hasoffer
CHEETAH_DATASET_NAME=cheetah

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
