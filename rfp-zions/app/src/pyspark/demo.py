# Code converted on 2023-04-24 13:21:55
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
import pandas
sc = SparkContext('local')
spark = SparkSession(sc)

try:

    # Processing node DSLink2, type SOURCE
    # COLUMN COUNT: 7
    # Original node name Get_Employee_Data, link DSLink2

    df = spark.read.csv("../../test-data/demo/input.csv", header=True)

    df2 = df.select("CustomerNumber", "GivenName", "MiddleName", "LastName", "FamilyName", concat_ws(" ", df.GivenName, df.MiddleName, df.LastName)
                    .alias("FullName"), "NameprefixTxt")
    df2.show()
    csv_file_path = "../../test-data/demo/pySpark_output.csv"
    df2.toPandas().to_csv(csv_file_path)

except OSError:
    print('Error Occurred')


quit()
