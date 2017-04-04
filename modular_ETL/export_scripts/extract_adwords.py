# for any queries regarding report structure, look at https://developers.google.com/adwords/api/docs/guides/reporting#create-a-report-definition
# and also look at https://adwords.google.com/api/adwords/reportdownload/v201605/reportDefinition.xsd

import logging
import sys
from googleads import adwords
import os

ACCPETED_DATE_RANGES = [
  'TODAY',
  'YESTERDAY',
  'LAST_7_DAYS',
  'LAST_WEEK',
  'LAST_BUSINESS_WEEK',
  'THIS_MONTH',
  'LAST_MONTH',
  'ALL_TIME',
  'CUSTOM_DATE',
  'LAST_14_DAYS',
  'LAST_14_DAYS',
  'THIS_WEEK_SUN_TODAY',
  'THIS_WEEK_MON_TODAY',
  'LAST_WEEK_SUN_SAT'
]

logging.basicConfig(level=logging.INFO)
logging.getLogger('suds.transport').setLevel(logging.DEBUG)

def main(client):

  # initializing the report_downloader object
  report_downloader = client.GetReportDownloader(version='v201702')

  # generating report structure
  report = {}
  report['downloadFormat'] = 'CSV'
  report['dateRangeType'] = 'YESTERDAY'   #required default value which is changed later in the code
  report['selector'] = {}

  nameSuffix = "_"

  # adding date range properties in report structure on the basis of input
  if len(sys.argv) == 1:      # when no arguments are passed
    nameSuffix = ""
  elif len(sys.argv) == 2:
    dateRange = sys.argv[1]
    dateRange = dateRange.upper()
    dateRange = dateRange.replace('-', '_')
    if dateRange not in ACCPETED_DATE_RANGES:
      print "Please enter a valid dateRange."
      print "This is a list of valid date ranges"
      for adr in ACCPETED_DATE_RANGES:
        print adr
      return
    report['dateRangeType'] = dateRange
    nameSuffix += dateRange

  elif len(sys.argv) == 3:
    startDate, endDate = sys.argv[1], sys.argv[2]
    if startDate > endDate:
      startDate, endDate = endDate, startDate
    nameSuffix += startDate + '_' + endDate
    report['selector']['dateRange'] = {
      # date 16 December 1994 will be written as 19941216
      # or also as 1994-12-16
      'min': startDate,
      'max': endDate
    }

  else:
    print "You had one job, and that was to give me a valid input. Is it really that hard? Actually, no, don't answer that."
    return


  # iterating through all table names needed
  with open('reportFields.txt') as f:
    for line in f:
      line = line.strip()
      if len(line)==0:
	      continue
      if line[0] == '#':
        continue

      array = line.split(',')
      array = map(lambda element: element.strip(), array)

      reportType = array[0]
      reportFields = array[1:]

      #doing the next line only because we want at most 4 fields for now
      #reportFields = reportFields[: min(4, len(reportFields))]

      report['reportType'] = reportType
      report['selector']['fields'] = reportFields
      report['reportName'] = reportType + nameSuffix

      path="/home/ubuntu/modular_ETL/data/extract/"
      #path=str(sys.argv[1])

      outputFileName = path +report['reportName'].lower() + '.' + report['downloadFormat'].lower()
      # v_compressedFileName=outputFileName+ '.gz'

      #outputFileName=  outputFileName.lower()

      with open(outputFileName, 'a') as g:
        g.write(report_downloader.DownloadReportAsString(
          report, skip_report_header=True, skip_column_header=True,
          skip_report_summary=True)
          .encode('utf-8')
        )


    #   print "This is the Extracted Adwords report file name: " + outputFileName
      
    # with open(outputFileName, 'rb') as f_in, gzip.open(v_compressedFileName, 'wb') as f_out:
    #     shutil.movefileobj(f_in, f_out)  


if __name__ == "__main__":
  adwords_client = adwords.AdWordsClient.LoadFromStorage()
  main(adwords_client)
