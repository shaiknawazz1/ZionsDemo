import xlrd
from collections import Counter

def initaiteValidation(mdlName):
    ds_out = "../../test-data/" + mdlName + "/ds_output.csv"
    py_out = "../../test-data/" + mdlName + "/pySpark_output.csv"
    return processCSV(ds_out,py_out)


def processCSV(src,tgt):

    
    respJSON = {"Inprogress": "Soon you will be seeing beatiful things"}
    return respJSON
#    tofile.close()
