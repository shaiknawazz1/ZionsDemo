#Code converted on 2023-04-10 17:39:17
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import SparkContext;
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)



		
try:

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

		# Processing node DSLink4, type TRANSFORMATION
		# COLUMN COUNT: 5
		# Original node name Transformer_1, link DSLink4

		DSLink4 = DSLink2.select( \
			(DSLink2.first_name + lit(' ') + DSLink2.last_name).alias('FULL_NAME'), \
			DSLink2.hired_date.alias('DATED_HIRED'), \
			(datediff(current_date(),DSLink2.hired_date)).alias('DAYS_EMPLOYEED'), \
			(current_date()).alias('CREATED_TS'), \
			(lit('Y')).alias('ACTIVE_IND') \
		) \
		.filter("DSLink2.Emp_id > 0")

		# Processing node Dim_Party, type TARGET
		# COLUMN COUNT: 5
		# 


		Dim_Party = DSLink4.select('*')
		Dim_Party.write.mode('append').jdbc("", """DIM_PARTY""", properties={'user': , 'password': , 'driver': })		

except OSError:
	print('Error Occurred')


quit()