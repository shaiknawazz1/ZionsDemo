#Code converted on 2022-09-20 18:28:37
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import SparkContext;
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)



		
try:

		# Processing node DSLink14, type SOURCE
		# COLUMN COUNT: 4
		# Original node name Departments_Master, link DSLink14

		DSLink14 = spark.read.jdbc('SOME_SOURCE', """(select 
		DEPT_NAME,
		DEPT_CODE,
		DEPT_KEY,
		CREATED_TS
		from IICS_CONV.DEPT_LOOKUP) QRY""", properties={'user': , 'password': , 'driver': })

		# Processing node DSLink2, type SOURCE
		# COLUMN COUNT: 7
		# Original node name Get_Employee_Data, link DSLink2

		DSLink2 = spark.read.jdbc('SOME_SOURCE', """(SELECT
		Employees.Emp_id,
		Employees.first_name,
		Employees.last_name,
		Employees.hired_date,
		Employees.last_upd_date,
		Employees.salary,
		Employees.dept_name
		FROM Employees
		WHERE Employees.hired_date > '2020-01-01') QRY""", properties={'user': , 'password': , 'driver': })

		# Processing node DSLink8, type TRANSFORMATION
		# COLUMN COUNT: 1
		# Original node name Trn1, link DSLink8

		DSLink8 = DSLink2.select( \
			DSLink2.dept_name.alias('dept_name') \
		)

		# Processing node DSLink10, type TRANSFORMATION
		# COLUMN COUNT: 3
		# Original node name trn2, link DSLink10

		DSLink10 = DSLink8.select( \
			DSLink8.dept_name.alias('dept_name'), \
			(lit('ID ') + DSLink8.dept_name).alias('dept_id'), \
			(lit(1)).alias('dummy') \
		)

		# Processing node Dim_Party, type TARGET
		# COLUMN COUNT: 0
		# 


		Dim_Party = DSLink4.select('*')
		Dim_Party.write.mode('append').jdbc("", """DIM_PARTY""", properties={'user': , 'password': , 'driver': })

		# Processing node DSLink13, type AGGREGATOR
		# COLUMN COUNT: 3
		# Original node name Aggregator_9, link DSLink13

		DSLink13 = DSLink10.groupBy("dept_name","dept_id").agg( \
			sum("dummy").alias("CNT")) \
			.select( \
				DSLink10.dept_name.alias('dept_name'), \
				DSLink10.dept_id.alias('dept_id'), \
				'CNT' \
			)

		# Processing node DSLink6, type LOOKUP
		# COLUMN COUNT: 3
		# Original node name Dept_Lkp, link DSLink6

		DSLink6Joined = DSLink13.join(DSLink14,[DSLink14.DEPT_CODE == DSLink13.dept_name]).select( \
				DSLink13["*"], DSLink14.DEPT_NAME)

		DSLink6 = DSLink6Joined.select( \
			DSLink14.DEPT_NAME.alias('DEPT_NAME'),  \
			DSLink13.CNT.alias('CNT'),  \
			DSLink13.dept_id.alias('DEPT_ID') \
		)

		# Processing node dim_Dept, type TARGET
		# COLUMN COUNT: 3
		# 


		dim_Dept = DSLink6.select('*')
		dim_Dept.write.mode('append').jdbc("", """DIM_DEPARTMENTS""", properties={'user': , 'password': , 'driver': })		

except OSError:
	print('Error Occurred')


quit()