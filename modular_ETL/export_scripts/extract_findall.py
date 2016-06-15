## Script Name: extract_findall.py
## This script extracts Conversions data of Hasoffer


## Parameters:
##        $1: Data Dump Location
##        

import sys
import json
import os
from datetime import date, timedelta




path="/home/ubuntu/modular_ETL/data/extract/"
#path=str(sys.argv[1])

yesterday = date.today() - timedelta(1)

yday = "20"+yesterday.strftime('%y-%m-%d')

os.system("rm " + path + "findall_stg.json")

command = "https://api.hasoffers.com/Apiv3/json?NetworkId=grouponindia1&Target=Affiliate&Method=findAll&NetworkToken=NETWly039P0dTqxWtIlNN6Nun6FheB&fields%5B%5D=account_manager_id&fields%5B%5D=address1&fields%5B%5D=address2&fields%5B%5D=affiliate_tier_id&fields%5B%5D=city&fields%5B%5D=company&fields%5B%5D=country&fields%5B%5D=date_added&fields%5B%5D=fax&fields%5B%5D=fraud_activity_alert_threshold&fields%5B%5D=fraud_activity_block_threshold&fields%5B%5D=fraud_activity_score&fields%5B%5D=fraud_profile_alert_threshold&fields%5B%5D=fraud_profile_block_threshold&fields%5B%5D=fraud_profile_score&fields%5B%5D=id&fields%5B%5D=method_data&fields%5B%5D=modified&fields%5B%5D=other&fields%5B%5D=payment_method&fields%5B%5D=payment_terms&fields%5B%5D=phone&fields%5B%5D=ref_id&fields%5B%5D=referral_id&fields%5B%5D=region&fields%5B%5D=signup_ip&fields%5B%5D=status&fields%5B%5D=w9_filed&fields%5B%5D=wants_alerts&fields%5B%5D=website&fields%5B%5D=zipcode"
command += "&data_start=" + yday
command += "&data_end=" + yday + '"'
#command += "&limit=9999" + '"'
command = 'curl "' + command
command += " -o " + path + "findall_stg.json"

os.system(command)

with open(path+"findall_stg.json") as f:
	data = json.load(f)

os.system("rm " + path +"findall.json")

#output = json.dumps(data['response']['data'], indent = 4, sort_keys = True)

dicts = data['response']['data']

str = ""
for key, value in dicts.iteritems():
	str += json.dumps(value['Affiliate']) + '\n'

os.system("touch "+path+ "findall.json")

with open(path+ "findall.json", "w") as f:
	f.write(str)

#os.system("rm input.json")


