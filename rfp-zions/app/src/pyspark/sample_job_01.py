# Code converted on 2023-04-24 13:21:56
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

try:

	# Processing node DSLink8, type SOURCE
	# COLUMN COUNT: 3
	# Original node name Data_Set_1, link DSLink8

	DSLink8 = spark.read.format('csv').option('inferSchema','true').option('header','false').option('sep', ',').load('C:/Projects/InfosysBofa/DataFiles/datafile2.txt')

	# Processing node DSLink5, type SOURCE
	# COLUMN COUNT: 3
	# Original node name Sequential_File_7, link DSLink5

	DSLink5 = spark.read.format('csv').option('inferSchema','true').option('header','false').option('sep', ',').load('C:/Projects/InfosysBofa/DataFiles/datafile1.txt')

	# Processing node DSLink9, type JOINER
	# COLUMN COUNT: 4
	# Original node name Join_2, link DSLink9

	DSLink9Joined = DSLink5.join(DSLink8,['COL1','COL3'],'LEFT_OUTER')

	DSLink9 = DSLink9Joined.select(
		DSLink5.COL1.alias('COL1'),
		DSLink5.COL2.alias('COL2'),
		DSLink5.COL3.alias('COL3'),
		DSLink8.COL5.alias('COL5')
	)

	# Processing node DSLink14, type TRANSFORMATION
	# COLUMN COUNT: 4
	# Original node name Transformer_3, link DSLink14

	DSLink14 = DSLink9.select(
		DSLink9.COL2.alias('COL2'),
		DSLink9.COL3.alias('COL3'),
		DSLink9.COL5.alias('COL5'),
		(CONVERT(lit('ABC') , lit('xyz') , DSLink9.COL1)).alias('COL1')
	).filter("COL2 = 'zzz'")

	# Processing node DSLink10, type TRANSFORMATION
	# COLUMN COUNT: 3
	# Original node name Transformer_3, link DSLink10

	DSLink10 = DSLink9.select(
		DSLink9.COL1.alias('OUT1'),
		(Char(DSLink9.COL2)).alias('OUT2'),
		(when(DSLink9.COL5 == lit(10.12) , lit(1000)).otherwise(lit(2000))).alias('OUT3')
	).filter("COL1 = 'abc'")

	# Processing node DSLink16, type TRANSFORMATION
	# COLUMN COUNT: 4
	# Original node name Transformer_4, link DSLink16

	DSLink16 = DSLink14.select(
		(UpCase(DSLink14.COL2)).alias('ORA_COL1'),
		DSLink14.COL3.alias('ORA_COL2'),
		DSLink14.COL5.alias('ORA_COL3'),
		(DateFromJulianDay(DSLink14.COL5)).alias('COL5')
	)

	# Processing node Sequential_File_4, type TARGET
	# COLUMN COUNT: 3

	Sequential_File_4 = DSLink10.select('*')
	Sequential_File_4.write.format('csv').option('header','false').mode('overwrite').option('sep', ',').csv('C:/Projects/InfosysBofa/DataFiles/datafile3.txt')

	# Processing node Oracle_Connector_15, type TARGET
	# COLUMN COUNT: 4

	Oracle_Connector_15 = DSLink16.select('*')
	Oracle_Connector_15.write.mode('append').jdbc("", """CUSTOMER_DIM""", properties={'user': , 'password': , 'driver': })	

except OSError:
	print('Error Occurred')


quit()
