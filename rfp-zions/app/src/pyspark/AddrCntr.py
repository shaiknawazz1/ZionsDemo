#Code converted on 2023-04-28 09:36:13
import os
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql.session import SparkSession

def convert_encoding(input_str, from_encoding="ISO-8859-1", to_encoding="UTF-8"):
	return input_str.encode(from_encoding).decode(to_encoding)

udf_convert_encoding = udf(convert_encoding, StringType())

def days_in_year(date_str):
    year = int(date_str[:4])
    if calendar.isleap(year):
        return 366
    else:
        return 365

udf_days_in_year = udf(days_in_year, IntegerType())


# Variable_declaration_comment
os.environ['SOURCE_DIR'] = '/nas_pp/dev/dw/data/conv_253'
SOURCE_DIR = os.getenv('SOURCE_DIR')

os.environ['STG_DIR'] = '/nas_pp/dev/dw/stg'
STG_DIR = os.getenv('STG_DIR')

os.environ['HASH_DIR'] = '/nas_pp/dev/dw/hash'
HASH_DIR = os.getenv('HASH_DIR')

os.environ['BANK_NUM'] = '100'
BANK_NUM = os.getenv('BANK_NUM')

os.environ['DATA_DATE'] = '20090131'
DATA_DATE = os.getenv('DATA_DATE')


# Processing node ANH_LKP_1, type SOURCE
# COLUMN COUNT: 13
# Original node name AddressNaturalHash_REF_1, link ANH_LKP_1

ANH_LKP_1 = spark.read.csv("AddressNaturalHash", sep=',', header='false')

# Processing node ANH_LKP_4, type SOURCE
# COLUMN COUNT: 13
# Original node name AddressNaturalHash_REF_4, link ANH_LKP_4

ANH_LKP_4 = spark.read.csv("AddressNaturalHash", sep=',', header='false')

# Processing node ANH_LKP_3, type SOURCE
# COLUMN COUNT: 13
# Original node name AddressNaturalHash_REF_3, link ANH_LKP_3

ANH_LKP_3 = spark.read.csv("AddressNaturalHash", sep=',', header='false')

# Processing node ANH_LKP_2, type SOURCE
# COLUMN COUNT: 13
# Original node name AddressNaturalHash_REF_2, link ANH_LKP_2

ANH_LKP_2 = spark.read.csv("AddressNaturalHash", sep=',', header='false')

# Processing node HASH_OUT_2_PO3, type TRANSFORMATION
# COLUMN COUNT: 13
# Original node name XFM_ADDR, link HASH_OUT_2_PO3

# Joining dataframes ANH_LKP_1, ANH_LKP_2, ANH_LKP_3, ANH_LKP_4 to form HASH_OUT_2_PO3
HASH_OUT_2_PO3_joined = ANH_LKP_1.join(ANH_LKP_2, ANH_LKP_1.sys_row_id == ANH_LKP_2.sys_row_id, 'inner')
 ANH_LKP_2.join(ANH_LKP_3, ANH_LKP_2.sys_row_id == ANH_LKP_3.sys_row_id, 'inner')
  ANH_LKP_3.join(ANH_LKP_4, ANH_LKP_3.sys_row_id == ANH_LKP_4.sys_row_id, 'inner')
HASH_OUT_2_PO3 = HASH_OUT_2_PO3_joined
HASH_OUT_2_PO3 = HASH_OUT_2_PO3.withColumn("ADDRID", when((length(ANH_LKP_1.ADDR_ID) > lit(0) ),(ANH_LKP_1.ADDR_ID )).otherwise(KeyMgtGetNextValueConcurrent((lit('addr_id')))))
	
HASH_OUT_2_PO3 = HASH_OUT_2_PO3.withColumn("ADDRIDPO1", when((length(ANH_LKP_2.ADDR_ID) > lit(0) ),(ANH_LKP_2.ADDR_ID )).otherwise(KeyMgtGetNextValueConcurrent((lit('addr_id')))))
	
HASH_OUT_2_PO3 = HASH_OUT_2_PO3.withColumn("ADDRIDPO2", when((length(ANH_LKP_3.ADDR_ID) > lit(0) ),(ANH_LKP_3.ADDR_ID )).otherwise(KeyMgtGetNextValueConcurrent((lit('addr_id')))))
	
HASH_OUT_2_PO3 = HASH_OUT_2_PO3.withColumn("ADDRIDPO3", when((length(ANH_LKP_4.ADDR_ID) > lit(0) ),(ANH_LKP_4.ADDR_ID )).otherwise(KeyMgtGetNextValueConcurrent((lit('addr_id')))))
	
HASH_OUT_2_PO3 = HASH_OUT_2_PO3.withColumn("POBOXLINE1", when((((substring(PASS.ADDR_LINE_1 , lit(1) ).otherwise(lit(2)) == lit('P.')) |(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(2)) == lit('P ')) |(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(11)) == lit('POST OFFICE')) |(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(3)) == lit('PO '))) &(length(trim(PASS.ADDR_LINE_2)) > lit(0)) &((substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('P.')) &(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('PO')) &(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('P '))) &(TRY_substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(1)).cast(NUMERIC) IS NOT None) )).otherwise((lit('Y') )).otherwise(lit('N')))
	
HASH_OUT_2_PO3 = HASH_OUT_2_PO3.withColumn("POBOXLINE2", when((((substring(PASS.ADDR_LINE_2 , lit(1) ).otherwise(lit(2)) == lit('P.')) |(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(2)) == lit('P ')) |(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(11)) == lit('POST OFFICE')) |(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(3)) == lit('PO '))) &((substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('P.')) &(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('PO')) &(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('P '))) &(TRY_substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(1)).cast(NUMERIC) IS NOT None) )).otherwise((lit('Y') )).otherwise(lit('N')))
	
HASH_OUT_2_PO3 = HASH_OUT_2_PO3.withColumn("POBOXLINE3", when((((substring(PASS.ADDR_LINE_3 , lit(1) ).otherwise(lit(2)) == lit('P.')) |(substring(PASS.ADDR_LINE_3 ).otherwise(lit(1) ).otherwise(lit(2)) == lit('P ')) |(substring(PASS.ADDR_LINE_3 ).otherwise(lit(1) ).otherwise(lit(11)) == lit('POST OFFICE')) |(substring(PASS.ADDR_LINE_3 ).otherwise(lit(1) ).otherwise(lit(3)) == lit('PO '))) &((TRY_substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(1)) AS NUMERIC) IS NOT None) |(TRY_CAST(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(1)).cast(NUMERIC) IS NOT None)) )).otherwise((lit('Y') )).otherwise(lit('N'))).select(
	(when((length(trim(PASS.ADDR_LINE_1)) > lit(0) ),(trim(PASS.ADDR_LINE_1) )).otherwise(lit('None'))).alias('ADDR_LINE_1'),
	(when((length(trim(PASS.ADDR_LINE_2)) > lit(0) ),(trim(PASS.ADDR_LINE_2) )).otherwise(lit('None'))).alias('ADDR_LINE_2'),
	(lit('None')).alias('ADDR_LINE_3'),
	(when((length(trim(PASS.CITY)) > lit(0) ),(trim(PASS.CITY) )).otherwise(lit('None'))).alias('CITY'),
	(when((length(trim(PASS.ST_CD)) > lit(0) ),(trim(PASS.ST_CD) )).otherwise(lit('None'))).alias('ST_CD'),
	(when((length(trim(PASS.PRVNC_CD)) > lit(0) ),(trim(PASS.PRVNC_CD) )).otherwise(lit('None'))).alias('PRVNC_CD'),
	(when((length(trim(PASS.PRVNC_DESC)) > lit(0) ),(trim(PASS.PRVNC_DESC) )).otherwise(lit('None'))).alias('PRVNC_DESC'),
	(when((length(trim(PASS.US_ZIP_5_CD)) > lit(0) ),(trim(PASS.US_ZIP_5_CD) )).otherwise(lit('None'))).alias('US_ZIP_5_CD'),
	(when((length(trim(PASS.US_ZIP_4_CD)) > lit(0) ),(trim(PASS.US_ZIP_4_CD) )).otherwise(lit('None'))).alias('US_ZIP_4_CD'),
	(when((length(trim(PASS.POSTAL_CD)) > lit(0) ),(trim(PASS.POSTAL_CD) )).otherwise(lit('None'))).alias('POSTAL_CD'),
	(when((length(trim(PASS.CTRY_CD)) > lit(0) ),(trim(PASS.CTRY_CD) )).otherwise(lit('None'))).alias('CTRY_CD'),
	(when((length(trim(PASS.CTRY_DESC)) > lit(0) ),(trim(PASS.CTRY_DESC) )).otherwise(lit('None'))).alias('CTRY_DESC'),
	HASH_OUT_2_PO3_joined.ADDRIDPO3.alias('ADDR_ID')
).filter("POBOXLINE3 = 'Y' AND LEN(ADDR_ID) = 0")

# Processing node HASH_OUT_1_PO1, type TRANSFORMATION
# COLUMN COUNT: 13
# Original node name XFM_ADDR, link HASH_OUT_1_PO1

# Joining dataframes ANH_LKP_1, ANH_LKP_2, ANH_LKP_3, ANH_LKP_4 to form HASH_OUT_1_PO1
HASH_OUT_1_PO1_joined = ANH_LKP_1.join(ANH_LKP_2, ANH_LKP_1.sys_row_id == ANH_LKP_2.sys_row_id, 'inner')
 ANH_LKP_2.join(ANH_LKP_3, ANH_LKP_2.sys_row_id == ANH_LKP_3.sys_row_id, 'inner')
  ANH_LKP_3.join(ANH_LKP_4, ANH_LKP_3.sys_row_id == ANH_LKP_4.sys_row_id, 'inner')
HASH_OUT_1_PO1 = HASH_OUT_1_PO1_joined
HASH_OUT_1_PO1 = HASH_OUT_1_PO1.withColumn("ADDRID", when((length(ANH_LKP_1.ADDR_ID) > lit(0) ),(ANH_LKP_1.ADDR_ID )).otherwise(KeyMgtGetNextValueConcurrent((lit('addr_id')))))
	
HASH_OUT_1_PO1 = HASH_OUT_1_PO1.withColumn("ADDRIDPO1", when((length(ANH_LKP_2.ADDR_ID) > lit(0) ),(ANH_LKP_2.ADDR_ID )).otherwise(KeyMgtGetNextValueConcurrent((lit('addr_id')))))
	
HASH_OUT_1_PO1 = HASH_OUT_1_PO1.withColumn("ADDRIDPO2", when((length(ANH_LKP_3.ADDR_ID) > lit(0) ),(ANH_LKP_3.ADDR_ID )).otherwise(KeyMgtGetNextValueConcurrent((lit('addr_id')))))
	
HASH_OUT_1_PO1 = HASH_OUT_1_PO1.withColumn("ADDRIDPO3", when((length(ANH_LKP_4.ADDR_ID) > lit(0) ),(ANH_LKP_4.ADDR_ID )).otherwise(KeyMgtGetNextValueConcurrent((lit('addr_id')))))
	
HASH_OUT_1_PO1 = HASH_OUT_1_PO1.withColumn("POBOXLINE1", when((((substring(PASS.ADDR_LINE_1 , lit(1) ).otherwise(lit(2)) == lit('P.')) |(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(2)) == lit('P ')) |(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(11)) == lit('POST OFFICE')) |(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(3)) == lit('PO '))) &(length(trim(PASS.ADDR_LINE_2)) > lit(0)) &((substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('P.')) &(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('PO')) &(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('P '))) &(TRY_substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(1)).cast(NUMERIC) IS NOT None) )).otherwise((lit('Y') )).otherwise(lit('N')))
	
HASH_OUT_1_PO1 = HASH_OUT_1_PO1.withColumn("POBOXLINE2", when((((substring(PASS.ADDR_LINE_2 , lit(1) ).otherwise(lit(2)) == lit('P.')) |(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(2)) == lit('P ')) |(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(11)) == lit('POST OFFICE')) |(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(3)) == lit('PO '))) &((substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('P.')) &(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('PO')) &(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('P '))) &(TRY_substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(1)).cast(NUMERIC) IS NOT None) )).otherwise((lit('Y') )).otherwise(lit('N')))
	
HASH_OUT_1_PO1 = HASH_OUT_1_PO1.withColumn("POBOXLINE3", when((((substring(PASS.ADDR_LINE_3 , lit(1) ).otherwise(lit(2)) == lit('P.')) |(substring(PASS.ADDR_LINE_3 ).otherwise(lit(1) ).otherwise(lit(2)) == lit('P ')) |(substring(PASS.ADDR_LINE_3 ).otherwise(lit(1) ).otherwise(lit(11)) == lit('POST OFFICE')) |(substring(PASS.ADDR_LINE_3 ).otherwise(lit(1) ).otherwise(lit(3)) == lit('PO '))) &((TRY_substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(1)) AS NUMERIC) IS NOT None) |(TRY_CAST(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(1)).cast(NUMERIC) IS NOT None)) )).otherwise((lit('Y') )).otherwise(lit('N'))).select(
	HASH_OUT_1_PO1_joined.ADDRIDPO1.alias('ADDR_ID'),
	(trim(PASS.ADDR_LINE_2)).alias('ADDR_LINE_1'),
	(trim(PASS.ADDR_LINE_3)).alias('ADDR_LINE_2'),
	(lit(None).cast(NullType())).alias('ADDR_LINE_3'),
	(trim(PASS.CITY)).alias('CITY'),
	(trim(PASS.ST_CD)).alias('ST_CD'),
	(trim(PASS.PRVNC_CD)).alias('PRVNC_CD'),
	(trim(PASS.PRVNC_DESC)).alias('PRVNC_DESC'),
	(trim(PASS.US_ZIP_5_CD)).alias('US_ZIP_5_CD'),
	(trim(PASS.US_ZIP_4_CD)).alias('US_ZIP_4_CD'),
	(trim(PASS.POSTAL_CD)).alias('POSTAL_CD'),
	(trim(PASS.CTRY_CD)).alias('CTRY_CD'),
	(trim(PASS.CTRY_DESC)).alias('CTRY_DESC')
).filter("POBOXLINE1 = 'Y'")

# Processing node HASH_OUT_2_PO2, type TRANSFORMATION
# COLUMN COUNT: 13
# Original node name XFM_ADDR, link HASH_OUT_2_PO2

# Joining dataframes ANH_LKP_1, ANH_LKP_2, ANH_LKP_3, ANH_LKP_4 to form HASH_OUT_2_PO2
HASH_OUT_2_PO2_joined = ANH_LKP_1.join(ANH_LKP_2, ANH_LKP_1.sys_row_id == ANH_LKP_2.sys_row_id, 'inner')
 ANH_LKP_2.join(ANH_LKP_3, ANH_LKP_2.sys_row_id == ANH_LKP_3.sys_row_id, 'inner')
  ANH_LKP_3.join(ANH_LKP_4, ANH_LKP_3.sys_row_id == ANH_LKP_4.sys_row_id, 'inner')
HASH_OUT_2_PO2 = HASH_OUT_2_PO2_joined
HASH_OUT_2_PO2 = HASH_OUT_2_PO2.withColumn("ADDRID", when((length(ANH_LKP_1.ADDR_ID) > lit(0) ),(ANH_LKP_1.ADDR_ID )).otherwise(KeyMgtGetNextValueConcurrent((lit('addr_id')))))
	
HASH_OUT_2_PO2 = HASH_OUT_2_PO2.withColumn("ADDRIDPO1", when((length(ANH_LKP_2.ADDR_ID) > lit(0) ),(ANH_LKP_2.ADDR_ID )).otherwise(KeyMgtGetNextValueConcurrent((lit('addr_id')))))
	
HASH_OUT_2_PO2 = HASH_OUT_2_PO2.withColumn("ADDRIDPO2", when((length(ANH_LKP_3.ADDR_ID) > lit(0) ),(ANH_LKP_3.ADDR_ID )).otherwise(KeyMgtGetNextValueConcurrent((lit('addr_id')))))
	
HASH_OUT_2_PO2 = HASH_OUT_2_PO2.withColumn("ADDRIDPO3", when((length(ANH_LKP_4.ADDR_ID) > lit(0) ),(ANH_LKP_4.ADDR_ID )).otherwise(KeyMgtGetNextValueConcurrent((lit('addr_id')))))
	
HASH_OUT_2_PO2 = HASH_OUT_2_PO2.withColumn("POBOXLINE1", when((((substring(PASS.ADDR_LINE_1 , lit(1) ).otherwise(lit(2)) == lit('P.')) |(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(2)) == lit('P ')) |(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(11)) == lit('POST OFFICE')) |(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(3)) == lit('PO '))) &(length(trim(PASS.ADDR_LINE_2)) > lit(0)) &((substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('P.')) &(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('PO')) &(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('P '))) &(TRY_substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(1)).cast(NUMERIC) IS NOT None) )).otherwise((lit('Y') )).otherwise(lit('N')))
	
HASH_OUT_2_PO2 = HASH_OUT_2_PO2.withColumn("POBOXLINE2", when((((substring(PASS.ADDR_LINE_2 , lit(1) ).otherwise(lit(2)) == lit('P.')) |(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(2)) == lit('P ')) |(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(11)) == lit('POST OFFICE')) |(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(3)) == lit('PO '))) &((substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('P.')) &(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('PO')) &(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('P '))) &(TRY_substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(1)).cast(NUMERIC) IS NOT None) )).otherwise((lit('Y') )).otherwise(lit('N')))
	
HASH_OUT_2_PO2 = HASH_OUT_2_PO2.withColumn("POBOXLINE3", when((((substring(PASS.ADDR_LINE_3 , lit(1) ).otherwise(lit(2)) == lit('P.')) |(substring(PASS.ADDR_LINE_3 ).otherwise(lit(1) ).otherwise(lit(2)) == lit('P ')) |(substring(PASS.ADDR_LINE_3 ).otherwise(lit(1) ).otherwise(lit(11)) == lit('POST OFFICE')) |(substring(PASS.ADDR_LINE_3 ).otherwise(lit(1) ).otherwise(lit(3)) == lit('PO '))) &((TRY_substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(1)) AS NUMERIC) IS NOT None) |(TRY_CAST(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(1)).cast(NUMERIC) IS NOT None)) )).otherwise((lit('Y') )).otherwise(lit('N'))).select(
	(when((length(trim(PASS.ADDR_LINE_1)) > lit(0) ),(trim(PASS.ADDR_LINE_1) )).otherwise(lit('None'))).alias('ADDR_LINE_1'),
	(when((length(trim(PASS.ADDR_LINE_3)) > lit(0) ),(trim(PASS.ADDR_LINE_3) )).otherwise(lit('None'))).alias('ADDR_LINE_2'),
	(lit('None')).alias('ADDR_LINE_3'),
	(when((length(trim(PASS.CITY)) > lit(0) ),(trim(PASS.CITY) )).otherwise(lit('None'))).alias('CITY'),
	(when((length(trim(PASS.ST_CD)) > lit(0) ),(trim(PASS.ST_CD) )).otherwise(lit('None'))).alias('ST_CD'),
	(when((length(trim(PASS.PRVNC_CD)) > lit(0) ),(trim(PASS.PRVNC_CD) )).otherwise(lit('None'))).alias('PRVNC_CD'),
	(when((length(trim(PASS.PRVNC_DESC)) > lit(0) ),(trim(PASS.PRVNC_DESC) )).otherwise(lit('None'))).alias('PRVNC_DESC'),
	(when((length(trim(PASS.US_ZIP_5_CD)) > lit(0) ),(trim(PASS.US_ZIP_5_CD) )).otherwise(lit('None'))).alias('US_ZIP_5_CD'),
	(when((length(trim(PASS.US_ZIP_4_CD)) > lit(0) ),(trim(PASS.US_ZIP_4_CD) )).otherwise(lit('None'))).alias('US_ZIP_4_CD'),
	(when((length(trim(PASS.POSTAL_CD)) > lit(0) ),(trim(PASS.POSTAL_CD) )).otherwise(lit('None'))).alias('POSTAL_CD'),
	(when((length(trim(PASS.CTRY_CD)) > lit(0) ),(trim(PASS.CTRY_CD) )).otherwise(lit('None'))).alias('CTRY_CD'),
	(when((length(trim(PASS.CTRY_DESC)) > lit(0) ),(trim(PASS.CTRY_DESC) )).otherwise(lit('None'))).alias('CTRY_DESC'),
	HASH_OUT_2_PO2_joined.ADDRIDPO2.alias('ADDR_ID')
).filter("POBOXLINE2 = 'Y' AND LEN(ADDR_ID) = 0")

# Processing node HASH_OUT_2_PO1, type TRANSFORMATION
# COLUMN COUNT: 13
# Original node name XFM_ADDR, link HASH_OUT_2_PO1

# Joining dataframes ANH_LKP_1, ANH_LKP_2, ANH_LKP_3, ANH_LKP_4 to form HASH_OUT_2_PO1
HASH_OUT_2_PO1_joined = ANH_LKP_1.join(ANH_LKP_2, ANH_LKP_1.sys_row_id == ANH_LKP_2.sys_row_id, 'inner')
 ANH_LKP_2.join(ANH_LKP_3, ANH_LKP_2.sys_row_id == ANH_LKP_3.sys_row_id, 'inner')
  ANH_LKP_3.join(ANH_LKP_4, ANH_LKP_3.sys_row_id == ANH_LKP_4.sys_row_id, 'inner')
HASH_OUT_2_PO1 = HASH_OUT_2_PO1_joined
HASH_OUT_2_PO1 = HASH_OUT_2_PO1.withColumn("ADDRID", when((length(ANH_LKP_1.ADDR_ID) > lit(0) ),(ANH_LKP_1.ADDR_ID )).otherwise(KeyMgtGetNextValueConcurrent((lit('addr_id')))))
	
HASH_OUT_2_PO1 = HASH_OUT_2_PO1.withColumn("ADDRIDPO1", when((length(ANH_LKP_2.ADDR_ID) > lit(0) ),(ANH_LKP_2.ADDR_ID )).otherwise(KeyMgtGetNextValueConcurrent((lit('addr_id')))))
	
HASH_OUT_2_PO1 = HASH_OUT_2_PO1.withColumn("ADDRIDPO2", when((length(ANH_LKP_3.ADDR_ID) > lit(0) ),(ANH_LKP_3.ADDR_ID )).otherwise(KeyMgtGetNextValueConcurrent((lit('addr_id')))))
	
HASH_OUT_2_PO1 = HASH_OUT_2_PO1.withColumn("ADDRIDPO3", when((length(ANH_LKP_4.ADDR_ID) > lit(0) ),(ANH_LKP_4.ADDR_ID )).otherwise(KeyMgtGetNextValueConcurrent((lit('addr_id')))))
	
HASH_OUT_2_PO1 = HASH_OUT_2_PO1.withColumn("POBOXLINE1", when((((substring(PASS.ADDR_LINE_1 , lit(1) ).otherwise(lit(2)) == lit('P.')) |(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(2)) == lit('P ')) |(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(11)) == lit('POST OFFICE')) |(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(3)) == lit('PO '))) &(length(trim(PASS.ADDR_LINE_2)) > lit(0)) &((substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('P.')) &(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('PO')) &(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('P '))) &(TRY_substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(1)).cast(NUMERIC) IS NOT None) )).otherwise((lit('Y') )).otherwise(lit('N')))
	
HASH_OUT_2_PO1 = HASH_OUT_2_PO1.withColumn("POBOXLINE2", when((((substring(PASS.ADDR_LINE_2 , lit(1) ).otherwise(lit(2)) == lit('P.')) |(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(2)) == lit('P ')) |(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(11)) == lit('POST OFFICE')) |(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(3)) == lit('PO '))) &((substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('P.')) &(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('PO')) &(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('P '))) &(TRY_substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(1)).cast(NUMERIC) IS NOT None) )).otherwise((lit('Y') )).otherwise(lit('N')))
	
HASH_OUT_2_PO1 = HASH_OUT_2_PO1.withColumn("POBOXLINE3", when((((substring(PASS.ADDR_LINE_3 , lit(1) ).otherwise(lit(2)) == lit('P.')) |(substring(PASS.ADDR_LINE_3 ).otherwise(lit(1) ).otherwise(lit(2)) == lit('P ')) |(substring(PASS.ADDR_LINE_3 ).otherwise(lit(1) ).otherwise(lit(11)) == lit('POST OFFICE')) |(substring(PASS.ADDR_LINE_3 ).otherwise(lit(1) ).otherwise(lit(3)) == lit('PO '))) &((TRY_substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(1)) AS NUMERIC) IS NOT None) |(TRY_CAST(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(1)).cast(NUMERIC) IS NOT None)) )).otherwise((lit('Y') )).otherwise(lit('N'))).select(
	(when((length(trim(PASS.ADDR_LINE_2)) > lit(0) ),(trim(PASS.ADDR_LINE_2) )).otherwise(lit('None'))).alias('ADDR_LINE_1'),
	(when((length(trim(PASS.ADDR_LINE_3)) > lit(0) ),(trim(PASS.ADDR_LINE_3) )).otherwise(lit('None'))).alias('ADDR_LINE_2'),
	(lit('None')).alias('ADDR_LINE_3'),
	(when((length(trim(PASS.CITY)) > lit(0) ),(trim(PASS.CITY) )).otherwise(lit('None'))).alias('CITY'),
	(when((length(trim(PASS.ST_CD)) > lit(0) ),(trim(PASS.ST_CD) )).otherwise(lit('None'))).alias('ST_CD'),
	(when((length(trim(PASS.PRVNC_CD)) > lit(0) ),(trim(PASS.PRVNC_CD) )).otherwise(lit('None'))).alias('PRVNC_CD'),
	(when((length(trim(PASS.PRVNC_DESC)) > lit(0) ),(trim(PASS.PRVNC_DESC) )).otherwise(lit('None'))).alias('PRVNC_DESC'),
	(when((length(trim(PASS.US_ZIP_5_CD)) > lit(0) ),(trim(PASS.US_ZIP_5_CD) )).otherwise(lit('None'))).alias('US_ZIP_5_CD'),
	(when((length(trim(PASS.US_ZIP_4_CD)) > lit(0) ),(trim(PASS.US_ZIP_4_CD) )).otherwise(lit('None'))).alias('US_ZIP_4_CD'),
	(when((length(trim(PASS.POSTAL_CD)) > lit(0) ),(trim(PASS.POSTAL_CD) )).otherwise(lit('None'))).alias('POSTAL_CD'),
	(when((length(trim(PASS.CTRY_CD)) > lit(0) ),(trim(PASS.CTRY_CD) )).otherwise(lit('None'))).alias('CTRY_CD'),
	(when((length(trim(PASS.CTRY_DESC)) > lit(0) ),(trim(PASS.CTRY_DESC) )).otherwise(lit('None'))).alias('CTRY_DESC'),
	HASH_OUT_2_PO1_joined.ADDRIDPO1.alias('ADDR_ID')
).filter("POBOXLINE1 = 'Y' AND LEN(ADDR_ID) = 0")

# Processing node HASH_OUT_1_PO3, type TRANSFORMATION
# COLUMN COUNT: 13
# Original node name XFM_ADDR, link HASH_OUT_1_PO3

# Joining dataframes ANH_LKP_1, ANH_LKP_2, ANH_LKP_3, ANH_LKP_4 to form HASH_OUT_1_PO3
HASH_OUT_1_PO3_joined = ANH_LKP_1.join(ANH_LKP_2, ANH_LKP_1.sys_row_id == ANH_LKP_2.sys_row_id, 'inner')
 ANH_LKP_2.join(ANH_LKP_3, ANH_LKP_2.sys_row_id == ANH_LKP_3.sys_row_id, 'inner')
  ANH_LKP_3.join(ANH_LKP_4, ANH_LKP_3.sys_row_id == ANH_LKP_4.sys_row_id, 'inner')
HASH_OUT_1_PO3 = HASH_OUT_1_PO3_joined
HASH_OUT_1_PO3 = HASH_OUT_1_PO3.withColumn("ADDRID", when((length(ANH_LKP_1.ADDR_ID) > lit(0) ),(ANH_LKP_1.ADDR_ID )).otherwise(KeyMgtGetNextValueConcurrent((lit('addr_id')))))
	
HASH_OUT_1_PO3 = HASH_OUT_1_PO3.withColumn("ADDRIDPO1", when((length(ANH_LKP_2.ADDR_ID) > lit(0) ),(ANH_LKP_2.ADDR_ID )).otherwise(KeyMgtGetNextValueConcurrent((lit('addr_id')))))
	
HASH_OUT_1_PO3 = HASH_OUT_1_PO3.withColumn("ADDRIDPO2", when((length(ANH_LKP_3.ADDR_ID) > lit(0) ),(ANH_LKP_3.ADDR_ID )).otherwise(KeyMgtGetNextValueConcurrent((lit('addr_id')))))
	
HASH_OUT_1_PO3 = HASH_OUT_1_PO3.withColumn("ADDRIDPO3", when((length(ANH_LKP_4.ADDR_ID) > lit(0) ),(ANH_LKP_4.ADDR_ID )).otherwise(KeyMgtGetNextValueConcurrent((lit('addr_id')))))
	
HASH_OUT_1_PO3 = HASH_OUT_1_PO3.withColumn("POBOXLINE1", when((((substring(PASS.ADDR_LINE_1 , lit(1) ).otherwise(lit(2)) == lit('P.')) |(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(2)) == lit('P ')) |(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(11)) == lit('POST OFFICE')) |(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(3)) == lit('PO '))) &(length(trim(PASS.ADDR_LINE_2)) > lit(0)) &((substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('P.')) &(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('PO')) &(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('P '))) &(TRY_substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(1)).cast(NUMERIC) IS NOT None) )).otherwise((lit('Y') )).otherwise(lit('N')))
	
HASH_OUT_1_PO3 = HASH_OUT_1_PO3.withColumn("POBOXLINE2", when((((substring(PASS.ADDR_LINE_2 , lit(1) ).otherwise(lit(2)) == lit('P.')) |(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(2)) == lit('P ')) |(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(11)) == lit('POST OFFICE')) |(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(3)) == lit('PO '))) &((substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('P.')) &(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('PO')) &(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('P '))) &(TRY_substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(1)).cast(NUMERIC) IS NOT None) )).otherwise((lit('Y') )).otherwise(lit('N')))
	
HASH_OUT_1_PO3 = HASH_OUT_1_PO3.withColumn("POBOXLINE3", when((((substring(PASS.ADDR_LINE_3 , lit(1) ).otherwise(lit(2)) == lit('P.')) |(substring(PASS.ADDR_LINE_3 ).otherwise(lit(1) ).otherwise(lit(2)) == lit('P ')) |(substring(PASS.ADDR_LINE_3 ).otherwise(lit(1) ).otherwise(lit(11)) == lit('POST OFFICE')) |(substring(PASS.ADDR_LINE_3 ).otherwise(lit(1) ).otherwise(lit(3)) == lit('PO '))) &((TRY_substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(1)) AS NUMERIC) IS NOT None) |(TRY_CAST(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(1)).cast(NUMERIC) IS NOT None)) )).otherwise((lit('Y') )).otherwise(lit('N'))).select(
	HASH_OUT_1_PO3_joined.ADDRIDPO3.alias('ADDR_ID'),
	(trim(PASS.ADDR_LINE_1)).alias('ADDR_LINE_1'),
	(trim(PASS.ADDR_LINE_2)).alias('ADDR_LINE_2'),
	(lit(None).cast(NullType())).alias('ADDR_LINE_3'),
	(trim(PASS.CITY)).alias('CITY'),
	(trim(PASS.ST_CD)).alias('ST_CD'),
	(trim(PASS.PRVNC_CD)).alias('PRVNC_CD'),
	(trim(PASS.PRVNC_DESC)).alias('PRVNC_DESC'),
	(trim(PASS.US_ZIP_5_CD)).alias('US_ZIP_5_CD'),
	(trim(PASS.US_ZIP_4_CD)).alias('US_ZIP_4_CD'),
	(trim(PASS.POSTAL_CD)).alias('POSTAL_CD'),
	(trim(PASS.CTRY_CD)).alias('CTRY_CD'),
	(trim(PASS.CTRY_DESC)).alias('CTRY_DESC')
).filter("POBOXLINE3 = 'Y'")

# Processing node HASH_OUT_1_PO2, type TRANSFORMATION
# COLUMN COUNT: 13
# Original node name XFM_ADDR, link HASH_OUT_1_PO2

# Joining dataframes ANH_LKP_1, ANH_LKP_2, ANH_LKP_3, ANH_LKP_4 to form HASH_OUT_1_PO2
HASH_OUT_1_PO2_joined = ANH_LKP_1.join(ANH_LKP_2, ANH_LKP_1.sys_row_id == ANH_LKP_2.sys_row_id, 'inner')
 ANH_LKP_2.join(ANH_LKP_3, ANH_LKP_2.sys_row_id == ANH_LKP_3.sys_row_id, 'inner')
  ANH_LKP_3.join(ANH_LKP_4, ANH_LKP_3.sys_row_id == ANH_LKP_4.sys_row_id, 'inner')
HASH_OUT_1_PO2 = HASH_OUT_1_PO2_joined
HASH_OUT_1_PO2 = HASH_OUT_1_PO2.withColumn("ADDRID", when((length(ANH_LKP_1.ADDR_ID) > lit(0) ),(ANH_LKP_1.ADDR_ID )).otherwise(KeyMgtGetNextValueConcurrent((lit('addr_id')))))
	
HASH_OUT_1_PO2 = HASH_OUT_1_PO2.withColumn("ADDRIDPO1", when((length(ANH_LKP_2.ADDR_ID) > lit(0) ),(ANH_LKP_2.ADDR_ID )).otherwise(KeyMgtGetNextValueConcurrent((lit('addr_id')))))
	
HASH_OUT_1_PO2 = HASH_OUT_1_PO2.withColumn("ADDRIDPO2", when((length(ANH_LKP_3.ADDR_ID) > lit(0) ),(ANH_LKP_3.ADDR_ID )).otherwise(KeyMgtGetNextValueConcurrent((lit('addr_id')))))
	
HASH_OUT_1_PO2 = HASH_OUT_1_PO2.withColumn("ADDRIDPO3", when((length(ANH_LKP_4.ADDR_ID) > lit(0) ),(ANH_LKP_4.ADDR_ID )).otherwise(KeyMgtGetNextValueConcurrent((lit('addr_id')))))
	
HASH_OUT_1_PO2 = HASH_OUT_1_PO2.withColumn("POBOXLINE1", when((((substring(PASS.ADDR_LINE_1 , lit(1) ).otherwise(lit(2)) == lit('P.')) |(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(2)) == lit('P ')) |(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(11)) == lit('POST OFFICE')) |(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(3)) == lit('PO '))) &(length(trim(PASS.ADDR_LINE_2)) > lit(0)) &((substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('P.')) &(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('PO')) &(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('P '))) &(TRY_substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(1)).cast(NUMERIC) IS NOT None) )).otherwise((lit('Y') )).otherwise(lit('N')))
	
HASH_OUT_1_PO2 = HASH_OUT_1_PO2.withColumn("POBOXLINE2", when((((substring(PASS.ADDR_LINE_2 , lit(1) ).otherwise(lit(2)) == lit('P.')) |(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(2)) == lit('P ')) |(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(11)) == lit('POST OFFICE')) |(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(3)) == lit('PO '))) &((substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('P.')) &(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('PO')) &(substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(2)) != lit('P '))) &(TRY_substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(1)).cast(NUMERIC) IS NOT None) )).otherwise((lit('Y') )).otherwise(lit('N')))
	
HASH_OUT_1_PO2 = HASH_OUT_1_PO2.withColumn("POBOXLINE3", when((((substring(PASS.ADDR_LINE_3 , lit(1) ).otherwise(lit(2)) == lit('P.')) |(substring(PASS.ADDR_LINE_3 ).otherwise(lit(1) ).otherwise(lit(2)) == lit('P ')) |(substring(PASS.ADDR_LINE_3 ).otherwise(lit(1) ).otherwise(lit(11)) == lit('POST OFFICE')) |(substring(PASS.ADDR_LINE_3 ).otherwise(lit(1) ).otherwise(lit(3)) == lit('PO '))) &((TRY_substring(PASS.ADDR_LINE_1 ).otherwise(lit(1) ).otherwise(lit(1)) AS NUMERIC) IS NOT None) |(TRY_CAST(substring(PASS.ADDR_LINE_2 ).otherwise(lit(1) ).otherwise(lit(1)).cast(NUMERIC) IS NOT None)) )).otherwise((lit('Y') )).otherwise(lit('N'))).select(
	HASH_OUT_1_PO2_joined.ADDRIDPO2.alias('ADDR_ID'),
	(trim(PASS.ADDR_LINE_1)).alias('ADDR_LINE_1'),
	(trim(PASS.ADDR_LINE_3)).alias('ADDR_LINE_2'),
	(lit(None).cast(NullType())).alias('ADDR_LINE_3'),
	(trim(PASS.CITY)).alias('CITY'),
	(trim(PASS.ST_CD)).alias('ST_CD'),
	(trim(PASS.PRVNC_CD)).alias('PRVNC_CD'),
	(trim(PASS.PRVNC_DESC)).alias('PRVNC_DESC'),
	(trim(PASS.US_ZIP_5_CD)).alias('US_ZIP_5_CD'),
	(trim(PASS.US_ZIP_4_CD)).alias('US_ZIP_4_CD'),
	(trim(PASS.POSTAL_CD)).alias('POSTAL_CD'),
	(trim(PASS.CTRY_CD)).alias('CTRY_CD'),
	(trim(PASS.CTRY_DESC)).alias('CTRY_DESC')
).filter("POBOXLINE2 = 'Y'")

# Processing node AddressHash_PO2, type TARGET
# COLUMN COUNT: 13

AddressHash_PO2 = HASH_OUT_1_PO2.select('*')
AddressHash_PO2.write.format('csv').option('header','false').mode('overwrite').option('sep',',').csv('AddressHash')

# Processing node AddressHash_PO1, type TARGET
# COLUMN COUNT: 13

AddressHash_PO1 = HASH_OUT_1_PO1.select('*')
AddressHash_PO1.write.format('csv').option('header','false').mode('overwrite').option('sep',',').csv('AddressHash')

# Processing node AddressNaturalHash_PO3, type TARGET
# COLUMN COUNT: 13

AddressNaturalHash_PO3 = HASH_OUT_2_PO3.select('*')
AddressNaturalHash_PO3.write.format('csv').option('header','false').mode('overwrite').option('sep',',').csv('AddressNaturalHash')

# Processing node AddressNaturalHash_PO2, type TARGET
# COLUMN COUNT: 13

AddressNaturalHash_PO2 = HASH_OUT_2_PO2.select('*')
AddressNaturalHash_PO2.write.format('csv').option('header','false').mode('overwrite').option('sep',',').csv('AddressNaturalHash')

# Processing node AddressNaturalHash_PO1, type TARGET
# COLUMN COUNT: 13

AddressNaturalHash_PO1 = HASH_OUT_2_PO1.select('*')
AddressNaturalHash_PO1.write.format('csv').option('header','false').mode('overwrite').option('sep',',').csv('AddressNaturalHash')

# Processing node AddressHash_PO3, type TARGET
# COLUMN COUNT: 13

AddressHash_PO3 = HASH_OUT_1_PO3.select('*')
AddressHash_PO3.write.format('csv').option('header','false').mode('overwrite').option('sep',',').csv('AddressHash')