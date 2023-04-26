# Code converted on 2023-04-24 13:21:55
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

try:

	# Processing node DSLink2, type SOURCE
	# COLUMN COUNT: 7
	# Original node name Get_Employee_Data, link DSLink2

	DSLink2 = spark.read.jdbc('DATA_SOURCE', f"""SELECT
	Employees.Emp_id,
	Employees.first_name,
	Employees.last_name,
	Employees.hired_date,
	Employees.last_upd_date,
	Employees.salary,
	Employees.dept_name
	FROM Employees
	WHERE Employees.hired_date > '2020-01-01'""")

	# Processing node DSLink6, type TRANSFORMATION
	# COLUMN COUNT: 3
	# Original node name Transformer_1, link DSLink6

	DSLink6 = DSLink2.select(
		(lit(12345)).alias('DEPT_ID'),
		DSLink2.dept_name.alias('DEPT_NAME'),
		(lit(10)).alias('CNT')
	)

	# Processing node DSLink4, type TRANSFORMATION
	# COLUMN COUNT: 5
	# Original node name Transformer_1, link DSLink4

	DSLink4 = DSLink2.select(
		(DSLink2.first_name + lit(' ') + DSLink2.last_name).alias('FULL_NAME'),
		DSLink2.hired_date.alias('DATED_HIRED'),
		(datediff(current_date() , DSLink2.hired_date)).alias('DAYS_EMPLOYEED'),
		(current_date()).alias('CREATED_TS'),
		(lit('Y')).alias('ACTIVE_IND')
	).filter("Emp_id > 0")

	# Processing node dim_Dept, type TARGET
	# COLUMN COUNT: 3

	dim_Dept = DSLink6.select('*')
	dim_Dept.write.mode('append').jdbc("", """DIM_DEPARTMENTS""", properties={'user': , 'password': , 'driver': })

	# Processing node Dim_Party, type TARGET
	# COLUMN COUNT: 5

	Dim_Party = DSLink4.select('*')
	Dim_Party.write.mode('append').jdbc("", """DIM_PARTY""", properties={'user': , 'password': , 'driver': })	

except OSError:
	print('Error Occurred')


quit()
