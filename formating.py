import sys
from pyspark import SparkContext
from csv import reader
from pyspark.sql import SparkSession
from pyspark.sql import functions
spark = SparkSession \
.builder \
.appName("peoject") \
.config("spark.some.config.option", "some-value") \
.getOrCreate()

sc = SparkContext()
weather = sc.textFile(sys.argv[1],1)
collision = sc.textFile(sys.argc[2],1)
weather = weather.mapPartitions(lambda x: reader(x))
collision = collision.mapPartitions(lambda x: reader(x))
def split_timeweather(x):
	time=[]
	time.append(float(x[:4]))
 	time.append(float(x[4:6]))
 	time.append(float(x[6:]))
 	return time
 def split_time(x):
 	time=[]
 	month, date, year = x.split('/')
 	time.append(year)
 	time.append(month)
 	time.append(date)
 	return time
weather = weather.map(lambda x: (x[0],(x[2],x[3],x[4],x[5],x[6],x[7],x[8])))
weather = weather.map(lambda x: (split_timeweather(x[0]),x[1]))
collision=collision.map(lambda x: (x[0],(x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10],x[11],x[12],x[13],x[14],x[15],x[16],x[17],x[18],x[19],x[20],x[21],x[22],x[23],x[24],x[25],x[26],x[27],x[28])))
collision = collision.map(lambda x: (split_time(x[0]),x[1]))


weather.saveAsTextFile('weatherout.txt')
collsion.saveAsTextFile('collsionout.txt')

