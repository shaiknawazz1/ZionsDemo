from pathlib import Path
import platform
import os

# def spark_to_csv(df, file_path):
#     """ Converts spark dataframe to CSV file """
#     with open(file_path, "w") as f:
#         writer = csv.DictWriter(f, fieldnames=df.columns)
#         writer.writerow(dict(zip(fieldnames, fieldnames)))
#         for row in df.toLocalIterator():
#             writer.writerow(row.asDict())


def get_input_folder(job_name = None):
    """return the path for input files to validate data stage job"""
    sep = ""
    if platform.system() == 'Windows':
        sep = "\\"
    else:
        sep = "/"
    p1 = os.getcwd()
    if job_name == None or job_name.isEmpty():
        return p1 + sep + "rfp-zions" + sep + "app" + sep + "test-data"
    return p1 + sep + "rfp-zions" + sep + "app" + sep + "test-data" + sep + job_name + sep

def getJobsList():
    rootdir = get_input_folder()
    resp = []
    for rootdir, dirs, files in os.walk(rootdir):
        for subdir in dirs:
            resp.append(subdir)
    return resp

# if __name__ == "__main__":
#     print (getJobsList())

    