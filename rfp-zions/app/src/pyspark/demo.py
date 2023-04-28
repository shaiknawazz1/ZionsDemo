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
    csv_file_path_in = os.getcwd() + "/rfp-zions/app/test-data/demo/input.csv"

    # df = spark.read.csv(csv_file_path +"../../test-data/demo/input.csv", header=True)
    df = spark.read.csv(csv_file_path_in, header=True)
    

    df2 = df.select("CustomerNumber", "GivenName", "MiddleName", "LastName", "FamilyName", concat_ws(" ", df.GivenName, df.LastName, df.FamilyName)
                    .alias("FullName"), "NameprefixTxt")
    df2
    df2.show()
    print ("current path : " + os.getcwd())
    csv_file_path_out = os.getcwd() + "/rfp-zions/app/test-data/demo/pySpark_output.csv"
    # opts = {"header":"true", "delimiter":","}
    # df2.write.options(opts).csv(csv_file_path_out)
    #df2 = df2.style.hide_index()
    df2.toPandas().set_index("CustomerNumber").to_csv(csv_file_path_out)

except OSError:
    print('Error Occurred')


quit()
