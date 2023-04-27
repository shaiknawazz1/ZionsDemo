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
from qautils import csvComparator as csvComp

# import pyspark
# from pyspark.sql import SparkSession


app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app)
api = Api(app, version="1.0", title="API to get Zions RFP data stage migration", description="API to get data stage migration",)


ns = api.namespace("Zions-rfp", description="general APIs like service health and availability")
nsqa = api.namespace("qa-Utilities", description="Testing utility APIs")
nsschdlr = api.namespace("Scheduling-Utilities", description="Scheduling utility APIs")

# parserhlth = api.parser()
parser = api.parser()
parser.add_argument(
   "dsName", type=str, required=True, help="Input Data Stage Job Name", location="form"
)
# parser.add_argument(
#    "wf_name", type=str, required=True, help="unique work flow name, if workflow has multiple seesions append with _ss1, _ss2, _ss3 etc.. for each session", location="form"
# )

@ns.route("/health")
class Health(Resource):
    """TODO"""
    # @api.doc(parser=parserhlth)
    def get(self):
        """ get the health of the service """
        #TODO check dependent service health and availbility
        return json.dumps({"status": "ok"}), 200
    
@nsqa.route("/validate_job")
class DSValidator(Resource):
    """TODO"""

    @api.doc(parser=parser)
    def post(self):
        """TODO"""
        args = parser.parse_args()
        ds_name = args["dsName"]
        resp = validateDSJob(ds_name)
        return resp, 201


def validateDSJob(ds_job):
    #TODO Suriya please add a testing logic here and generate the detailed report
    # identify the job name
    #resp = csvComp.initaiteValidation(mdlName)
    return {"Inprogress": "Soon you will be seeing beatiful things"}


if __name__ == "__main__":
    app.run(port=5000)
