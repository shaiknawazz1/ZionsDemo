from time import sleep
from flask import Flask, send_from_directory, make_response, send_file
from flask_restx import Api, Resource, fields
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.datastructures import FileStorage
import os
import platform
import json
from os.path import exists
import time
import pandas as pd
import xlrd as xl
import requests as rq

import pyspark
from pyspark.sql import SparkSession


app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app)
api = Api(app, version="1.0", title="API to get Zions RFP data stage migration", description="API to get data stage migration",)


ns = api.namespace("Zions-RFP", description="general APIs like service health and availability")
nsqa = api.namespace("qautils", description="Testing utility APIs")


parser = api.parser()
# parser.add_argument(
#    "query", type=str, required=True, help="Input Data Stage Job Name", location="form"
# )
#parser.add_argument(
#    "wf_name", type=str, required=True, help="unique work flow name, if workflow has multiple seesions append with _ss1, _ss2, _ss3 etc.. for each session", location="form"
#)

@ns.route("/health")
@api.representation('application/json')
class Health(Resource):
    """TODO"""
    @api.doc(parser=parser)
    def get(self):
        """get the health of the service"""
        #TODO check dependent service health and availbility
        return json.dumps({"status": "ok"}), 200

@ns.route("/pySparkHealth")
class PySparkHealth(Resource):
    """TODO"""
    @api.doc(parser=parser)
    def get(self):
        """get the health of the service"""
        #TODO check dependent service health and availbility
        df = testPySPark()
        return df.to_json(), 200
    
    # @api.doc(parser=parser)
    # def post(self):
    #     """get the health of the service"""
    #     #TODO check dependent service health and availbility
    #     df = testPySPark()
    #     return df.to_json(), 200

#generate unit tests for pySpark code

#test pySpark
def testPySPark():

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('tepst') \
        .getOrCreate()

    df = spark.read \
        .option("header", "true") \
        .csv('sampleInput.csv')
    
    return df

    #df.show()

#test pySpark
def validateDSJob():

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('tepst') \
        .getOrCreate()

    df = spark.read \
        .option("header", "true") \
        .csv('sampleInput.xlsx')
    
    return df

if __name__ == "__main__":
    app.run(port=5000, debug=True)
