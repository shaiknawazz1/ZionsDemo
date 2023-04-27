# from pyspark.dbutils import DBUtils

# def spark_to_csv(df, file_path):
#     """ Converts spark dataframe to CSV file """
#     with open(file_path, "w") as f:
#         writer = csv.DictWriter(f, fieldnames=df.columns)
#         writer.writerow(dict(zip(fieldnames, fieldnames)))
#         for row in df.toLocalIterator():
#             writer.writerow(row.asDict())


# def get_db_utils(spark):

#       dbutils = None
      
#       if spark.conf.get("spark.databricks.service.client.enabled") == "true":
        
#         from pyspark.dbutils import DBUtils
#         dbutils = DBUtils(spark)
      
#       else:
        
#         import IPython
#         dbutils = IPython.get_ipython().user_ns["dbutils"]
      
#       return dbutils