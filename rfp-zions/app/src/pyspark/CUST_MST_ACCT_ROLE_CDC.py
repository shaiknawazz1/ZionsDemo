# Code converted on 2023-04-27 14:43:58
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
os.environ['STG_DIR'] = '/nas_pp/dev/dw/stg'
STG_DIR = os.getenv('STG_DIR')


# Processing node Lnk_cust_addr_role_prev_IN, type SOURCE
# COLUMN COUNT: 14
# Original node name cust_addr_role_prev, link Lnk_cust_addr_role_prev_IN

Lnk_cust_addr_role_prev_IN = spark.read.csv(
    ""+STG_DIR+"/customer/cust_mst_acct_role_prev.txt", sep=',', header='false')

# Processing node Lnk_cust_addr_role_curr_IN, type SOURCE
# COLUMN COUNT: 14
# Original node name cust_addr_role_curr, link Lnk_cust_addr_role_curr_IN

Lnk_cust_addr_role_curr_IN = spark.read.csv(
    ""+STG_DIR+"/customer/cust_mst_acct_role_curr.txt", sep=',', header='false')

# Processing node Lnk_xfm_cust_addr_role, type CHANGE_CAPTURE
# COLUMN COUNT: 15
# Original node name cdc_cust_mst_acct_role, link Lnk_xfm_cust_addr_role

Lnk_xfm_cust_addr_role = Lnk_cust_addr_role_curr_IN.join(Lnk_cust_addr_role_prev_IN, ["CUST_MST_ACCT_ROLE_ID"], "fullouter").select("*",
                                                                                                                                    when(
                                                                                                                                        (Lnk_cust_addr_role_curr_IN.CUST_MST_ACCT_ROLE_ID.isNotNull()) &
                                                                                                                                        (Lnk_cust_addr_role_prev_IN.CUST_MST_ACCT_ROLE_ID.isNotNull()) &
                                                                                                                                        (sha2(concat_ws("||", *[]), 256) ==
                                                                                                                                            sha2(concat_ws("||", *[]), 256)), 0
                                                                                                                                    ).when(
                                                                                                                                        (Lnk_cust_addr_role_curr_IN.CUST_MST_ACCT_ROLE_ID.isNotNull()) &
                                                                                                                                        (Lnk_cust_addr_role_prev_IN.CUST_MST_ACCT_ROLE_ID.isNull(
                                                                                                                                        )), 1
                                                                                                                                    ).when(
                                                                                                                                        (Lnk_cust_addr_role_curr_IN.CUST_MST_ACCT_ROLE_ID.isNull()) &
                                                                                                                                        (Lnk_cust_addr_role_prev_IN.CUST_MST_ACCT_ROLE_ID.isNotNull(
                                                                                                                                        )), 2
                                                                                                                                    ).when(
                                                                                                                                        (Lnk_cust_addr_role_curr_IN.CUST_MST_ACCT_ROLE_ID.isNotNull()) &
                                                                                                                                        (Lnk_cust_addr_role_prev_IN.CUST_MST_ACCT_ROLE_ID.isNotNull()) &
                                                                                                                                        (sha2(concat_ws("||", *[]), 256) !=
                                                                                                                                            sha2(concat_ws("||", *[]), 256)), 3
                                                                                                                                    ).alias("change_code")
                                                                                                                                    )

# Processing node cust_addr_role_update, type TRANSFORMATION
# COLUMN COUNT: 14
# Original node name xfm_cust_addr_role, link cust_addr_role_update

cust_addr_role_update = Lnk_xfm_cust_addr_role.select(
    Lnk_xfm_cust_addr_role.CUST_MST_ACCT_ROLE_ID.alias(
        'CUST_MST_ACCT_ROLE_ID'),
    Lnk_xfm_cust_addr_role.MST_ACCT_ID.alias('MST_ACCT_ID'),
    Lnk_xfm_cust_addr_role.MST_ACCT_EFF_DT.alias('MST_ACCT_EFF_DT'),
    Lnk_xfm_cust_addr_role.CUST_ID.alias('CUST_ID'),
    Lnk_xfm_cust_addr_role.CUST_EFF_DT.alias('CUST_EFF_DT'),
    Lnk_xfm_cust_addr_role.CUST_TO_ACCT_REL_CD.alias('CUST_TO_ACCT_REL_CD'),
    Lnk_xfm_cust_addr_role.CUST_TO_ACCT_REL_DESC.alias(
        'CUST_TO_ACCT_REL_DESC'),
    Lnk_xfm_cust_addr_role.PRMY_SCNDY_CD.alias('PRMY_SCNDY_CD'),
    Lnk_xfm_cust_addr_role.CIF_ACCT_BRNCH.alias('CIF_ACCT_BRNCH'),
    Lnk_xfm_cust_addr_role.CIF_ACCT_NBR.alias('CIF_ACCT_NBR'),
    Lnk_xfm_cust_addr_role.CIF_CLOSE_DT.alias('CIF_CLOSE_DT'),
    Lnk_xfm_cust_addr_role.CIF_OPEN_DT.alias('CIF_OPEN_DT'),
    Lnk_xfm_cust_addr_role.CIF_PROD_CD.alias('CIF_PROD_CD'),
    Lnk_xfm_cust_addr_role.CIF_PROD_DESC.alias('CIF_PROD_DESC')
).filter("change_code = 3")

# Processing node cust_addr_role_insert, type TRANSFORMATION
# COLUMN COUNT: 14
# Original node name xfm_cust_addr_role, link cust_addr_role_insert

cust_addr_role_insert = Lnk_xfm_cust_addr_role.select(
    Lnk_xfm_cust_addr_role.CUST_MST_ACCT_ROLE_ID.alias(
        'CUST_MST_ACCT_ROLE_ID'),
    Lnk_xfm_cust_addr_role.MST_ACCT_ID.alias('MST_ACCT_ID'),
    Lnk_xfm_cust_addr_role.MST_ACCT_EFF_DT.alias('MST_ACCT_EFF_DT'),
    Lnk_xfm_cust_addr_role.CUST_ID.alias('CUST_ID'),
    Lnk_xfm_cust_addr_role.CUST_EFF_DT.alias('CUST_EFF_DT'),
    Lnk_xfm_cust_addr_role.CUST_TO_ACCT_REL_CD.alias('CUST_TO_ACCT_REL_CD'),
    Lnk_xfm_cust_addr_role.CUST_TO_ACCT_REL_DESC.alias(
        'CUST_TO_ACCT_REL_DESC'),
    Lnk_xfm_cust_addr_role.PRMY_SCNDY_CD.alias('PRMY_SCNDY_CD'),
    Lnk_xfm_cust_addr_role.CIF_ACCT_BRNCH.alias('CIF_ACCT_BRNCH'),
    Lnk_xfm_cust_addr_role.CIF_ACCT_NBR.alias('CIF_ACCT_NBR'),
    Lnk_xfm_cust_addr_role.CIF_CLOSE_DT.alias('CIF_CLOSE_DT'),
    Lnk_xfm_cust_addr_role.CIF_OPEN_DT.alias('CIF_OPEN_DT'),
    Lnk_xfm_cust_addr_role.CIF_PROD_CD.alias('CIF_PROD_CD'),
    Lnk_xfm_cust_addr_role.CIF_PROD_DESC.alias('CIF_PROD_DESC')
).filter("change_code = 1")

# Processing node cust_addr_role_delete, type TRANSFORMATION
# COLUMN COUNT: 14
# Original node name xfm_cust_addr_role, link cust_addr_role_delete

cust_addr_role_delete = Lnk_xfm_cust_addr_role.select(
    Lnk_xfm_cust_addr_role.CUST_MST_ACCT_ROLE_ID.alias(
        'CUST_MST_ACCT_ROLE_ID'),
    Lnk_xfm_cust_addr_role.MST_ACCT_ID.alias('MST_ACCT_ID'),
    Lnk_xfm_cust_addr_role.MST_ACCT_EFF_DT.alias('MST_ACCT_EFF_DT'),
    Lnk_xfm_cust_addr_role.CUST_ID.alias('CUST_ID'),
    Lnk_xfm_cust_addr_role.CUST_EFF_DT.alias('CUST_EFF_DT'),
    Lnk_xfm_cust_addr_role.CUST_TO_ACCT_REL_CD.alias('CUST_TO_ACCT_REL_CD'),
    Lnk_xfm_cust_addr_role.CUST_TO_ACCT_REL_DESC.alias(
        'CUST_TO_ACCT_REL_DESC'),
    Lnk_xfm_cust_addr_role.PRMY_SCNDY_CD.alias('PRMY_SCNDY_CD'),
    Lnk_xfm_cust_addr_role.CIF_ACCT_BRNCH.alias('CIF_ACCT_BRNCH'),
    Lnk_xfm_cust_addr_role.CIF_ACCT_NBR.alias('CIF_ACCT_NBR'),
    Lnk_xfm_cust_addr_role.CIF_CLOSE_DT.alias('CIF_CLOSE_DT'),
    Lnk_xfm_cust_addr_role.CIF_OPEN_DT.alias('CIF_OPEN_DT'),
    Lnk_xfm_cust_addr_role.CIF_PROD_CD.alias('CIF_PROD_CD'),
    Lnk_xfm_cust_addr_role.CIF_PROD_DESC.alias('CIF_PROD_DESC')
).filter("change_code = 2")

# Processing node cust_addr_role_update_2, type TARGET
# COLUMN COUNT: 14

cust_addr_role_update_2 = cust_addr_role_update.select('*')
cust_addr_role_update_2.write.format('csv').option('header', 'false').mode('overwrite').option('sep', ',').csv('{getArgument('STG_DIR')}/customer/cust_mst_acct_role_updates.txt')

# Processing node cust_addr_role_insert_2, type TARGET
# COLUMN COUNT: 14

cust_addr_role_insert_2 = cust_addr_role_insert.select('*')
cust_addr_role_insert_2.write.format('csv').option('header', 'false').mode('overwrite').option('sep', ',').csv('{getArgument('STG_DIR')}/customer/cust_mst_acct_role_inserts.txt')

# Processing node cust_addr_role_delete_2, type TARGET
# COLUMN COUNT: 14

cust_addr_role_delete_2 = cust_addr_role_delete.select('*')
cust_addr_role_delete_2.write.format('csv').option('header', 'false').mode('overwrite').option('sep', ',').csv('{getArgument('STG_DIR')}/customer/cust_mst_acct_role_deletes.txt')

sc = SparkContext('local')
spark = SparkSession(sc)
