#!/usr/bin/python
import string
import sys
from pyspark.sql import functions
from pyspark.sql.session import SparkSession
from csv import reader
spark = SparkSession \
.builder \
.appName("task7-sql") \
.config("spark.some.config.option", "some-value") \
.getOrCreate()


parking = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
openvio = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2])
weather.createOrReplaceTempView("weather")
collision.createOrReplaceTempView("collision")
weather = spark.sql('SELECT * FROM weather GROUP BY  Date')
collision = spark.sql('SELECT * FROM collision GROUP BY  Date')
results = spark.sql('SELECT * FROM weather INNER JOIN collision ON weather.    Date = collision.    Date')
results.write.save('collweather.out',format='text')