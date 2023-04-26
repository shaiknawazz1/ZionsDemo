# Code converted on 2023-04-24 13:21:55
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

try:

	# Processing node DSLink18, type SOURCE
	# COLUMN COUNT: 2
	# Original node name ODBC_Connector_16, link DSLink18

	DSLink18 = spark.read.format('csv').option('inferSchema','true').option('header','false').option('sep', ',').load('%PATH%')

	# Processing node DSLink4, type SOURCE
	# COLUMN COUNT: 3
	# Original node name Oracle_Connector_0, link DSLink4

	DSLink4 = spark.read.jdbc('DATA_SOURCE', f"""""")

	# Processing node DSLink3, type SOURCE
	# COLUMN COUNT: 3
	# Original node name JDBC_Connector_1, link DSLink3

	DSLink3 = spark.read.format('csv').option('inferSchema','true').option('header','false').option('sep', ',').load('%PATH%')

	# Processing node DSLink36, type SOURCE
	# COLUMN COUNT: 2
	# Original node name Oracle_Connector_44, link DSLink36

	DSLink36 = spark.read.jdbc('DATA_SOURCE', f"""SELECT CC1, CC2
	FROM TBL100 WHERE ID > 1000""")

	# Processing node DSLink8, type MERGE
	# COLUMN COUNT: 2
	# Original node name Lookup_2, link DSLink8

	DSLink8 = DSLink3.join(DSLink4, [DSLink4.COL12 == DSLink3.COL3, DSLink4.COL10 == DSLink3.COL1], 'LEFT_OUTER').join(DSLink36, [DSLink36.CC1 == DSLink3.COL1], 'LEFT_OUTER').select(
		DSLink3.COL1.alias('c1'),
		DSLink4.COL11.alias('c2')
	)

	# Processing node DSLink31, type MERGE
	# COLUMN COUNT: 4
	# Original node name Lookup_17, link DSLink31

	DSLink31 = DSLink8.join(DSLink18, [DSLink18.ZZ1 == DSLink8.c1], 'LEFT_OUTER').select(
		DSLink8.c1.alias('c1'),
		DSLink8.c2.alias('c2'),
		DSLink18.ZZ1.alias('ZZ1'),
		DSLink18.ZZ2.alias('ZZ2')
	)

	# Processing node DSLink28, type REPLICATE
	# COLUMN COUNT: 4
	# Original node name Copy_19, link DSLink28

	DSLink28 = DSLink31.select(
		DSLink31.c1,
		DSLink31.c2,
		DSLink31.ZZ1,
		DSLink31.ZZ2
	)

	# Processing node DSLink30, type REPLICATE
	# COLUMN COUNT: 4
	# Original node name Copy_19, link DSLink30

	DSLink30 = DSLink31.select(
		DSLink31.sort_col1,
		DSLink31.sort_col2,
		DSLink31.sort_col3,
		DSLink31.sort_col4
	)

	# Processing node DSLink16, type TRANSFORMATION
	# COLUMN COUNT: 3
	# Original node name Transformer_21, link DSLink16

	DSLink16 = DSLink28.select(
		DSLink28.ZZ1.alias('ORA_COL1'),
		(datediff(DSLink28.c1 , DSLink28.c2)).alias('ORA_COL2'),
		(when(DSLink28.c1 == lit(100) , lit('OK')).otherwise(lit('ERROR'))).alias('ORA_COL3')
	)

	# Processing node DSLink33, type SORTER
	# COLUMN COUNT: 4
	# Original node name Sort_29, link DSLink33

	DSLink33 = DSLink30.select(
		DSLink30.sort_col1.alias('sort_col1'),
		DSLink30.sort_col2.alias('sort_col2'),
		DSLink30.sort_col3.alias('sort_col3'),
		DSLink30.sort_col4.alias('sort_col4')
	).sort(col('sort_col1').asc(), lower(col('sort_col4')).desc())

	# Processing node Oracle_Connector_15, type TARGET
	# COLUMN COUNT: 3

	Oracle_Connector_15 = DSLink16.select('*')
	Oracle_Connector_15.write.mode('append').jdbc("", """CUSTOMER_DIM""", properties={'user': , 'password': , 'driver': })

	# Processing node DSLink17, type TRANSFORMATION
	# COLUMN COUNT: 3
	# Original node name Transformer_32, link DSLink17

	DSLink17 = DSLink33.select(
		DSLink33.sort_col1.alias('ORA_COL1'),
		DSLink33.sort_col3.alias('ORA_COL2'),
		(AsInteger(DSLink33.sort_col4)).alias('ORA_COL3')
	)

	# Processing node Copy_of_Oracle_Connector_15, type TARGET
	# COLUMN COUNT: 3

	Copy_of_Oracle_Connector_15 = DSLink17.select('*')
	Copy_of_Oracle_Connector_15.write.mode('append').jdbc("", """CUSTOMER_DIM_H""", properties={'user': , 'password': , 'driver': })	

except OSError:
	print('Error Occurred')


quit()
