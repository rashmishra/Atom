## Script Name: extract_conversion.py
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

os.system("rm "+path+"conversion_stg.json")

command = "https://api.hasoffers.com/Apiv3/json?NetworkId=grouponindia1&Target=Report&Method=getConversions&NetworkToken=NETWly039P0dTqxWtIlNN6Nun6FheB&fields%5B%5D=Stat.date&fields%5B%5D=Stat.datetime&fields%5B%5D=Stat.datetime_diff&fields%5B%5D=Stat.hour&fields%5B%5D=Stat.id&fields%5B%5D=Stat.ip&fields%5B%5D=Stat.month&fields%5B%5D=Stat.offer_id&fields%5B%5D=Stat.payout&fields%5B%5D=Stat.pending_payout&fields%5B%5D=Stat.pending_revenue&fields%5B%5D=Stat.pending_sale_amount&fields%5B%5D=Stat.refer&fields%5B%5D=Stat.session_date&fields%5B%5D=Stat.session_datetime&fields%5B%5D=Stat.session_ip&fields%5B%5D=Stat.source&fields%5B%5D=Stat.status&fields%5B%5D=Stat.status_code&fields%5B%5D=Stat.week&fields%5B%5D=Stat.year&fields%5B%5D=Stat.ad_id&fields%5B%5D=Stat.affiliate_id&fields%5B%5D=Stat.advertiser_id&fields%5B%5D=Stat.advertiser_info&fields%5B%5D=Stat.sale_amount&fields%5B%5D=Stat.revenue&fields%5B%5D=Stat.net_sale_amount&fields%5B%5D=Stat.net_revenue&fields%5B%5D=Stat.net_payout&fields%5B%5D=Stat.currency&fields%5B%5D=Stat.approved_payout&fields%5B%5D=Stat.affiliate_info2&fields%5B%5D=Stat.country_code&fields%5B%5D=Stat.affiliate_info1&fields%5B%5D=Stat.affiliate_info3&fields%5B%5D=Stat.affiliate_info4&fields%5B%5D=Stat.affiliate_info5&fields%5B%5D=Stat.payout_type&fields%5B%5D=Stat.customer_id&fields%5B%5D=Stat.advertiser_manager_id&fields%5B%5D=Stat.affiliate_manager_id&fields%5B%5D=Stat.browser_id&fields%5B%5D=Stat.revenue_type&fields%5B%5D=Advertiser.company&fields%5B%5D=AdvertiserManager.full_name&fields%5B%5D=Affiliate.company&fields%5B%5D=AffiliateManager.full_name&fields%5B%5D=Browser.display_name&fields%5B%5D=ConversionsMobile.adv_sub2&fields%5B%5D=ConversionsMobile.adv_sub3&fields%5B%5D=ConversionsMobile.adv_sub4&fields%5B%5D=ConversionsMobile.adv_sub5&fields%5B%5D=ConversionsMobile.android_id&fields%5B%5D=ConversionsMobile.device_id&fields%5B%5D=ConversionsMobile.device_brand&fields%5B%5D=ConversionsMobile.device_model&fields%5B%5D=ConversionsMobile.device_os&fields%5B%5D=ConversionsMobile.google_aid&fields%5B%5D=ConversionsMobile.ios_ifa&fields%5B%5D=ConversionsMobile.ios_ifv&fields%5B%5D=ConversionsMobile.mac_address&fields%5B%5D=ConversionsMobile.mobile_carrier&fields%5B%5D=ConversionsMobile.user_id&fields%5B%5D=ConversionsMobile.windows_aid&fields%5B%5D=Country.name&fields%5B%5D=Offer.name&fields%5B%5D=PayoutGroup.id&fields%5B%5D=RevenueGroup.name&fields%5B%5D=PayoutGroup.name&fields%5B%5D=Goal.name&fields%5B%5D=Customer.provided_id&fields%5B%5D=ConversionsMobile.odin&fields%5B%5D=Stat.user_agent&fields%5B%5D=Stat.goal_id&fields%5B%5D=Stat.creative_url_id&fields%5B%5D=Stat.is_adjustment&fields%5B%5D=Stat.offer_url_id&fields%5B%5D=Stat.pixel_refer&fields%5B%5D=RevenueGroup.id&fields%5B%5D=OfferUrl.preview_url&fields%5B%5D=OfferUrl.name&fields%5B%5D=Offer.ref_id&fields%5B%5D=ConversionMeta.note"
command += "&filters%5BStat.session_date%5D%5Bconditional%5D=EQUAL_TO&filters%5BStat.session_date%5D%5Bvalues%5D=" + yday
command += "&limit=5000";
command += '"' 
command = 'curl "' + command
command += " -o " +path+ "conversion_stg.json"

print command

os.system(command)

with open(path+'conversion_stg.json') as f:
	data = json.load(f)

os.system("rm "+path+"conversions.json")

output = data['response']['data']['data']

os.system("touch "+path+ "/conversions.json")

with open(path+"conversions.json", 'w') as f:
	for op in output:
		toWrite = '{'
		for key, val in op.iteritems():
			for k, v in val.iteritems():
				toWrite += '"' + key + '_' + k + '"' + ':' + '"' + str(v) + '"' + ','
		toWrite = toWrite[:-1] + '}\n'
		f.write(toWrite)

#os.system("rm input.json")

