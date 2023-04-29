from pathlib import Path
import platform
import os
from pathlib import Path


# def spark_to_csv(df, file_path):
#     """ Converts spark dataframe to CSV file """
#     with open(file_path, "w") as f:
#         writer = csv.DictWriter(f, fieldnames=df.columns)
#         writer.writerow(dict(zip(fieldnames, fieldnames)))
#         for row in df.toLocalIterator():
#             writer.writerow(row.asDict())

def get_input_folder(job_name=None):
    """return the path for input files to validate data stage job"""
    global pathSep
    p1 = os.getcwd()
    if platform.system() == 'Windows':
        pathSep = "\\"
        if job_name == None or job_name == "":
            p1 = p1 + pathSep + "rfp-zions" + pathSep + "app" + pathSep + "test-data"
        else:
            p1 =  p1 + pathSep + "rfp-zions" + pathSep + "app" + pathSep + "test-data" + pathSep + job_name
    else:
        print("linux path (test data folder) : " + p1)
        plis = p1.split("/")
        plis.pop()
        p1 = "/".join(plis)
        
        pathSep = "/"
        if job_name == None or job_name == "":
            p1 = p1 + "/test-data/" 
        else:
            p1 = p1 + "/test-data/" + job_name + pathSep

    print ("input folder : " + p1)
    return p1

    

def get_pySpark(job_name ):
    """return the path for input files to validate data stage job"""
    sep = ""
    p1 = os.getcwd()
    if platform.system() == 'Windows':
        sep = "\\"
        return p1 + sep + "rfp-zions" + sep + "app" + sep + "src" + sep + "pyspark" + sep + job_name + ".py"
    else:
        sep = "/"
        return p1 + sep + "pyspark" + sep + job_name + ".py"

def getJobsList():
    rootdir = get_input_folder()
    resp = []
    for rootdir, dirs, files in os.walk(rootdir):
        for subdir in dirs:
            resp.append(subdir)
    return resp

# if __name__ == "__main__":
#     print (getJobsList())

    