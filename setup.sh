#!/bin/sh
echo "running the BB converter"
pwd = $(pwd)
cd bb/Datastage2Pyspark

ds2any.exe -c C:\Users\SMNawaz\Desktop\projects\RFPnDemo\zions\Datastage2Pyspark\license.txt.checksum_keys -i C:\Users\SMNawaz\Desktop\projects\RFPnDemo\zions\Datastage2Pyspark\Datastage\med_complex_ds_job.xml -M CodeGeneration::PySpark -g C:\Users\SMNawaz\Desktop\projects\RFPnDemo\zions\Datastage2Pyspark\Configs\ds2dws.cfg -o C:\Users\SMNawaz\Desktop\projects\RFPnDemo\zions\Datastage2Pyspark\PysparkOut -u C:\Users\SMNawaz\Desktop\projects\RFPnDemo\zions\Datastage2Pyspark\Configs\ds2pyspark.json

echo $pwd

# done