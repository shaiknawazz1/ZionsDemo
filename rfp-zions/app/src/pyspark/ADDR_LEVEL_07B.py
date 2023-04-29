#Code converted on 2023-04-28 10:30:35
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

os.environ['FMT_DIR'] = '/nas_pp/dev/dw/fmt'
FMT_DIR = os.getenv('FMT_DIR')

os.environ['BANK_NUM'] = '100'
BANK_NUM = os.getenv('BANK_NUM')

os.environ['DATA_DATE'] = '20151130'
DATA_DATE = os.getenv('DATA_DATE')

os.environ['REJECT_DIR'] = '/nas_pp/dev/dw/rejects'
REJECT_DIR = os.getenv('REJECT_DIR')


# Processing node ECF_CNTRY, type SOURCE
# COLUMN COUNT: 2
# Original node name ECF_COUNTRY_Hash_1, link ECF_CNTRY

ECF_CNTRY = spark.read.csv("ECF_COUNTRY_Hash", sep=',', header='false')

# Processing node SEQ_IN_CONTAINER_INPUT, type CONTAINER
# COLUMN COUNT: 190
# Original node name SboMlSrcCntr, link SEQ_IN_CONTAINER_INPUT

#test123

# Processing node SEQ_OUT, type TRANSFORMATION
# COLUMN COUNT: 17
# Original node name xfm_Mort_PA, link SEQ_OUT

# Joining dataframes ECF_CNTRY, SEQ_IN_CONTAINER_INPUT to form SEQ_OUT
SEQ_OUT_joined = ECF_CNTRY.join(SEQ_IN_CONTAINER_INPUT, ECF_CNTRY.sys_row_id == SEQ_IN_CONTAINER_INPUT.sys_row_id, 'inner')
SEQ_OUT = SEQ_OUT_joined
SEQ_OUT = SEQ_OUT.withColumn("AddrLine1", when(((length(SEQ_IN.PROPERTY_STREET_NUMBER) > lit(0)) &(length(SEQ_IN.PROPERTY_STREET_DIRECTION) > lit(0)) &(length(SEQ_IN.PROPERTY_STREET_NAME) > lit(0)) , SEQ_IN.PROPERTY_STREET_NUMBER + lit(' ') + SEQ_IN.PROPERTY_STREET_DIRECTION + lit(' ') + SEQ_IN.PROPERTY_STREET_NAME ).otherwise(when(((length(SEQ_IN.PROPERTY_STREET_NUMBER) > lit(0)) &(length(SEQ_IN.PROPERTY_STREET_DIRECTION) > lit(0)) &(length(SEQ_IN.PROPERTY_STREET_NAME) == lit(0)) ).otherwise(SEQ_IN.PROPERTY_STREET_NUMBER + lit(' ') + SEQ_IN.PROPERTY_STREET_DIRECTION ).otherwise(when(((length(SEQ_IN.PROPERTY_STREET_NUMBER) > lit(0)) &(length(SEQ_IN.PROPERTY_STREET_DIRECTION) == lit(0)) &(length(SEQ_IN.PROPERTY_STREET_NAME) > lit(0)) ).otherwise(SEQ_IN.PROPERTY_STREET_NUMBER + lit(' ') + SEQ_IN.PROPERTY_STREET_NAME ).otherwise(when(((length(SEQ_IN.PROPERTY_STREET_NUMBER) == lit(0)) &(length(SEQ_IN.PROPERTY_STREET_DIRECTION) > lit(0)) &(length(SEQ_IN.PROPERTY_STREET_NAME) > lit(0)) ).otherwise(SEQ_IN.PROPERTY_STREET_DIRECTION + lit(' ') + SEQ_IN.PROPERTY_STREET_NAME ).otherwise(when(((length(SEQ_IN.PROPERTY_STREET_NUMBER) > lit(0)) &(length(SEQ_IN.PROPERTY_STREET_DIRECTION) == lit(0)) &(length(SEQ_IN.PROPERTY_STREET_NAME) == lit(0)) )).otherwise((SEQ_IN.PROPERTY_STREET_NUMBER ))).otherwise((IFF((length(SEQ_IN.PROPERTY_STREET_NUMBER) == lit(0)) &(length(SEQ_IN.PROPERTY_STREET_DIRECTION) > lit(0)) &(length(SEQ_IN.PROPERTY_STREET_NAME) == lit(0)) ))).otherwise((SEQ_IN.PROPERTY_STREET_DIRECTION ))).otherwise((IFF((length(SEQ_IN.PROPERTY_STREET_NUMBER) == lit(0)) &(length(SEQ_IN.PROPERTY_STREET_DIRECTION) == lit(0)) &(length(SEQ_IN.PROPERTY_STREET_NAME) > lit(0)) ))).otherwise((SEQ_IN.PROPERTY_STREET_NAME )).otherwise(None)))))))).select(
	SEQ_IN.LOAN_NO.alias('LOAN_NO'),
	SEQ_IN.CALC_MTG_BANK.alias('CALC_MTG_BANK'),
	SEQ_IN.MTG_BANK.alias('MTG_BANK'),
	SEQ_IN.MTG_LOAN_NO.alias('MTG_LOAN_NO'),
	SEQ_OUT_joined.AddrLine1.alias('SECONDARY_ADDR'),
	SEQ_OUT_joined.AddrLine1.alias('ADDR_LINE_1'),
	(lit(None).cast(NullType())).alias('ADDR_LINE_2'),
	(lit(None).cast(NullType())).alias('ADDR_LINE_3'),
	SEQ_IN.PROPERTY_CITY.alias('CITY'),
	SEQ_IN.STATE_CODE.alias('ST_CD'),
	(lit(None).cast(NullType())).alias('PRVNC_CD'),
	(lit(None).cast(NullType())).alias('PRVNC_DESC'),
	SEQ_IN.ZIP_CODE.alias('US_ZIP_5_CD'),
	(lit(None).cast(NullType())).alias('US_ZIP_4_CD'),
	(lit(None).cast(NullType())).alias('POSTAL_CD'),
	ECF_CNTRY.CTRY_CD.alias('CTRY_CD'),
	ECF_CNTRY.CTRY_DESC.alias('CTRY_DESC')
).filter("LEN(TRIM(AddrLine1)) > 0")

# Processing node PASS, type TRANSFORMATION
# COLUMN COUNT: 12
# Original node name xfm_Mort_PA, link PASS

# Joining dataframes ECF_CNTRY, SEQ_IN_CONTAINER_INPUT to form PASS
PASS_joined = ECF_CNTRY.join(SEQ_IN_CONTAINER_INPUT, ECF_CNTRY.sys_row_id == SEQ_IN_CONTAINER_INPUT.sys_row_id, 'inner')
PASS = PASS_joined
PASS = PASS.withColumn("AddrLine1", when(((length(SEQ_IN.PROPERTY_STREET_NUMBER) > lit(0)) &(length(SEQ_IN.PROPERTY_STREET_DIRECTION) > lit(0)) &(length(SEQ_IN.PROPERTY_STREET_NAME) > lit(0)) , SEQ_IN.PROPERTY_STREET_NUMBER + lit(' ') + SEQ_IN.PROPERTY_STREET_DIRECTION + lit(' ') + SEQ_IN.PROPERTY_STREET_NAME ).otherwise(when(((length(SEQ_IN.PROPERTY_STREET_NUMBER) > lit(0)) &(length(SEQ_IN.PROPERTY_STREET_DIRECTION) > lit(0)) &(length(SEQ_IN.PROPERTY_STREET_NAME) == lit(0)) ).otherwise(SEQ_IN.PROPERTY_STREET_NUMBER + lit(' ') + SEQ_IN.PROPERTY_STREET_DIRECTION ).otherwise(when(((length(SEQ_IN.PROPERTY_STREET_NUMBER) > lit(0)) &(length(SEQ_IN.PROPERTY_STREET_DIRECTION) == lit(0)) &(length(SEQ_IN.PROPERTY_STREET_NAME) > lit(0)) ).otherwise(SEQ_IN.PROPERTY_STREET_NUMBER + lit(' ') + SEQ_IN.PROPERTY_STREET_NAME ).otherwise(when(((length(SEQ_IN.PROPERTY_STREET_NUMBER) == lit(0)) &(length(SEQ_IN.PROPERTY_STREET_DIRECTION) > lit(0)) &(length(SEQ_IN.PROPERTY_STREET_NAME) > lit(0)) ).otherwise(SEQ_IN.PROPERTY_STREET_DIRECTION + lit(' ') + SEQ_IN.PROPERTY_STREET_NAME ).otherwise(when(((length(SEQ_IN.PROPERTY_STREET_NUMBER) > lit(0)) &(length(SEQ_IN.PROPERTY_STREET_DIRECTION) == lit(0)) &(length(SEQ_IN.PROPERTY_STREET_NAME) == lit(0)) )).otherwise((SEQ_IN.PROPERTY_STREET_NUMBER ))).otherwise((IFF((length(SEQ_IN.PROPERTY_STREET_NUMBER) == lit(0)) &(length(SEQ_IN.PROPERTY_STREET_DIRECTION) > lit(0)) &(length(SEQ_IN.PROPERTY_STREET_NAME) == lit(0)) ))).otherwise((SEQ_IN.PROPERTY_STREET_DIRECTION ))).otherwise((IFF((length(SEQ_IN.PROPERTY_STREET_NUMBER) == lit(0)) &(length(SEQ_IN.PROPERTY_STREET_DIRECTION) == lit(0)) &(length(SEQ_IN.PROPERTY_STREET_NAME) > lit(0)) ))).otherwise((SEQ_IN.PROPERTY_STREET_NAME )).otherwise(None)))))))).select(
	PASS_joined.AddrLine1.alias('ADDR_LINE_1'),
	(lit(None).cast(NullType())).alias('ADDR_LINE_2'),
	(lit(None).cast(NullType())).alias('ADDR_LINE_3'),
	SEQ_IN.PROPERTY_CITY.alias('CITY'),
	SEQ_IN.STATE_CODE.alias('ST_CD'),
	(lit(None).cast(NullType())).alias('PRVNC_CD'),
	(lit(None).cast(NullType())).alias('PRVNC_DESC'),
	SEQ_IN.ZIP_CODE.alias('US_ZIP_5_CD'),
	(lit(None).cast(NullType())).alias('US_ZIP_4_CD'),
	(lit(None).cast(NullType())).alias('POSTAL_CD'),
	ECF_CNTRY.CTRY_CD.alias('CTRY_CD'),
	ECF_CNTRY.CTRY_DESC.alias('CTRY_DESC')
).filter("LEN(TRIM(AddrLine1)) > 0")

# Processing node SboMortSecondaryAddrStg, type TARGET
# COLUMN COUNT: 17

SboMortSecondaryAddrStg = SEQ_OUT.select('*')
SboMortSecondaryAddrStg.write.format('csv').option('header','false').mode('overwrite').option('sep',',').csv('{getArgument('STG_DIR')}/addr/sbo_mort_secondary_addr.stg')

# Processing node HASH_OUT_1_CONTAINER_INPUT, type CONTAINER
# COLUMN COUNT: 13
# Original node name AddrCntr, link HASH_OUT_1_CONTAINER_INPUT

#test123

# Processing node HASH_OUT_2_CONTAINER_INPUT, type CONTAINER
# COLUMN COUNT: 13
# Original node name AddrCntr, link HASH_OUT_2_CONTAINER_INPUT

#test123

# Processing node AddressHash, type TARGET
# COLUMN COUNT: 13

AddressHash = HASH_OUT_1_CONTAINER_INPUT.select('*')
AddressHash.write.format('csv').option('header','false').mode('overwrite').option('sep',',').csv('')

# Processing node AddressNaturalHash, type TARGET
# COLUMN COUNT: 13

AddressNaturalHash = HASH_OUT_2_CONTAINER_INPUT.select('*')
AddressNaturalHash.write.format('csv').option('header','false').mode('overwrite').option('sep',',').csv('')