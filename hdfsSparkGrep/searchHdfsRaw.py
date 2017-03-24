# spark-submit --conf spark.yarn.executor.memoryOverhead=4000 --driver-memory 4G --driver-cores 4 --executor-memory 4G --executor-cores 4 --master yarn --deploy-mode cluster searchHDFSRaw.py 2017-01-07 userOption

from pyspark import SparkContext
import re
import sys
import base64
from datetime import datetime, timedelta, date


if len(sys.argv) <5:
    print "valid input load_date required : searchHDFSRaw.py <start-yyyy-mm-dd> <end-yyyy-mm-dd> <search-keyword> <userName>"
    sys.exit()
sdt = sys.argv[1]
edt = sys.argv[2]
searchKey = sys.argv[3]
userName = sys.argv[4]
sdt = sdt.strip()
edt = edt.strip()
searchKey = searchKey.strip()
userName = userName.strip()

def matchBody(keywords, txt):
    for searchKey in keywords:
        if re.search(searchKey, txt, re.IGNORECASE):
            return True
    return False

if not re.match(r'\d{4}\-\d{2}\-\d{2}', sdt) or not re.match(r'\d{4}\-\d{2}\-\d{2}', edt):
    print "valid input load_date required : yyyy-mm-dd"
    sys.exit()

keywords = []
if searchKey.startswith('file:'):
    filename = searchKey[5:]
    keywords = open(filename, 'r').read().split()
else:
    keywords.append(searchKey)

sc = SparkContext(appName="findTxt")
stDtObj = datetime.strptime(sdt, '%Y-%m-%d').date()
endDtObj = datetime.strptime(edt, '%Y-%m-%d').date()
dtObj = stDtObj
paths = []
ctrDays = 0
while dtObj <= endDtObj:
    dt = datetime.strftime(dtObj, '%Y-%m-%d')
    paths.append('/data/logs/sample/raw/load_date='+dt+'/*')	## need to parameterize the hdfs source path
    dtObj = (dtObj + timedelta(days=1))
    ctrDays += 1
    if ctrDays > 10:
	break

fh = sc.textFile(','.join(paths))
secRdd = fh.filter(lambda line: matchBody(keywords, line) == True).repartition(1)
secRdd.saveAsTextFile("/tmp/SearchResult-"+userName)

