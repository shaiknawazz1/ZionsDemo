#Code converted on 2023-04-26 15:38:17
import os
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)


# COMMAND ----------
# Variable_declaration_comment
dbutils.widgets.text(name = 'STG_DIR', defaultValue = '/nas_pp/dev/dw/stg')
STG_DIR = dbutils.widgets.get("STG_DIR")


# COMMAND ----------
# Processing node Lnk_cust_addr_role_prev_IN, type SOURCE
# COLUMN COUNT: 14
# Original node name cust_addr_role_prev, link Lnk_cust_addr_role_prev_IN

Lnk_cust_addr_role_prev_IN = spark.read.csv(""+STG_DIR+"/customer/cust_mst_acct_role_prev.txt", sep=',', header='false')

# COMMAND ----------
# Processing node Lnk_cust_addr_role_curr_IN, type SOURCE
# COLUMN COUNT: 14
# Original node name cust_addr_role_curr, link Lnk_cust_addr_role_curr_IN

Lnk_cust_addr_role_curr_IN = spark.read.csv(""+STG_DIR+"/customer/cust_mst_acct_role_curr.txt", sep=',', header='false')

# COMMAND ----------
# Processing node Lnk_xfm_cust_addr_role, type CHANGE_CAPTURE
# COLUMN COUNT: 15
# Original node name cdc_cust_mst_acct_role, link Lnk_xfm_cust_addr_role

Lnk_xfm_cust_addr_role = Lnk_cust_addr_role_curr_IN.join(Lnk_cust_addr_role_prev_IN,["CUST_MST_ACCT_ROLE_ID"], "fullouter").select("*",
	when(
		(Lnk_cust_addr_role_curr_IN.CUST_MST_ACCT_ROLE_ID.isNotNull()) &
		(Lnk_cust_addr_role_prev_IN.CUST_MST_ACCT_ROLE_ID.isNotNull()) &
		(sha2(concat_ws("||", *[]), 256) == sha2(concat_ws("||", *[]), 256)), 0
	).when(
		(Lnk_cust_addr_role_curr_IN.CUST_MST_ACCT_ROLE_ID.isNotNull()) &
		(Lnk_cust_addr_role_prev_IN.CUST_MST_ACCT_ROLE_ID.isNull()), 1
	).when(
		(Lnk_cust_addr_role_curr_IN.CUST_MST_ACCT_ROLE_ID.isNull()) &
		(Lnk_cust_addr_role_prev_IN.CUST_MST_ACCT_ROLE_ID.isNotNull()), 2
	).when(
		(Lnk_cust_addr_role_curr_IN.CUST_MST_ACCT_ROLE_ID.isNotNull()) &
		(Lnk_cust_addr_role_prev_IN.CUST_MST_ACCT_ROLE_ID.isNotNull()) &
		(sha2(concat_ws("||", *[]), 256) != sha2(concat_ws("||", *[]), 256)), 3
	).alias("change_code")
)

# COMMAND ----------
# Processing node cust_addr_role_update, type TRANSFORMATION
# COLUMN COUNT: 14
# Original node name xfm_cust_addr_role, link cust_addr_role_update

cust_addr_role_update = Lnk_xfm_cust_addr_role.select(
	Lnk_xfm_cust_addr_role.CUST_MST_ACCT_ROLE_ID.alias('CUST_MST_ACCT_ROLE_ID'),
	Lnk_xfm_cust_addr_role.MST_ACCT_ID.alias('MST_ACCT_ID'),
	Lnk_xfm_cust_addr_role.MST_ACCT_EFF_DT.alias('MST_ACCT_EFF_DT'),
	Lnk_xfm_cust_addr_role.CUST_ID.alias('CUST_ID'),
	Lnk_xfm_cust_addr_role.CUST_EFF_DT.alias('CUST_EFF_DT'),
	Lnk_xfm_cust_addr_role.CUST_TO_ACCT_REL_CD.alias('CUST_TO_ACCT_REL_CD'),
	Lnk_xfm_cust_addr_role.CUST_TO_ACCT_REL_DESC.alias('CUST_TO_ACCT_REL_DESC'),
	Lnk_xfm_cust_addr_role.PRMY_SCNDY_CD.alias('PRMY_SCNDY_CD'),
	Lnk_xfm_cust_addr_role.CIF_ACCT_BRNCH.alias('CIF_ACCT_BRNCH'),
	Lnk_xfm_cust_addr_role.CIF_ACCT_NBR.alias('CIF_ACCT_NBR'),
	Lnk_xfm_cust_addr_role.CIF_CLOSE_DT.alias('CIF_CLOSE_DT'),
	Lnk_xfm_cust_addr_role.CIF_OPEN_DT.alias('CIF_OPEN_DT'),
	Lnk_xfm_cust_addr_role.CIF_PROD_CD.alias('CIF_PROD_CD'),
	Lnk_xfm_cust_addr_role.CIF_PROD_DESC.alias('CIF_PROD_DESC')
).filter("change_code = 3")

# COMMAND ----------
# Processing node cust_addr_role_insert, type TRANSFORMATION
# COLUMN COUNT: 14
# Original node name xfm_cust_addr_role, link cust_addr_role_insert

cust_addr_role_insert = Lnk_xfm_cust_addr_role.select(
	Lnk_xfm_cust_addr_role.CUST_MST_ACCT_ROLE_ID.alias('CUST_MST_ACCT_ROLE_ID'),
	Lnk_xfm_cust_addr_role.MST_ACCT_ID.alias('MST_ACCT_ID'),
	Lnk_xfm_cust_addr_role.MST_ACCT_EFF_DT.alias('MST_ACCT_EFF_DT'),
	Lnk_xfm_cust_addr_role.CUST_ID.alias('CUST_ID'),
	Lnk_xfm_cust_addr_role.CUST_EFF_DT.alias('CUST_EFF_DT'),
	Lnk_xfm_cust_addr_role.CUST_TO_ACCT_REL_CD.alias('CUST_TO_ACCT_REL_CD'),
	Lnk_xfm_cust_addr_role.CUST_TO_ACCT_REL_DESC.alias('CUST_TO_ACCT_REL_DESC'),
	Lnk_xfm_cust_addr_role.PRMY_SCNDY_CD.alias('PRMY_SCNDY_CD'),
	Lnk_xfm_cust_addr_role.CIF_ACCT_BRNCH.alias('CIF_ACCT_BRNCH'),
	Lnk_xfm_cust_addr_role.CIF_ACCT_NBR.alias('CIF_ACCT_NBR'),
	Lnk_xfm_cust_addr_role.CIF_CLOSE_DT.alias('CIF_CLOSE_DT'),
	Lnk_xfm_cust_addr_role.CIF_OPEN_DT.alias('CIF_OPEN_DT'),
	Lnk_xfm_cust_addr_role.CIF_PROD_CD.alias('CIF_PROD_CD'),
	Lnk_xfm_cust_addr_role.CIF_PROD_DESC.alias('CIF_PROD_DESC')
).filter("change_code = 1")

# COMMAND ----------
# Processing node cust_addr_role_delete, type TRANSFORMATION
# COLUMN COUNT: 14
# Original node name xfm_cust_addr_role, link cust_addr_role_delete

cust_addr_role_delete = Lnk_xfm_cust_addr_role.select(
	Lnk_xfm_cust_addr_role.CUST_MST_ACCT_ROLE_ID.alias('CUST_MST_ACCT_ROLE_ID'),
	Lnk_xfm_cust_addr_role.MST_ACCT_ID.alias('MST_ACCT_ID'),
	Lnk_xfm_cust_addr_role.MST_ACCT_EFF_DT.alias('MST_ACCT_EFF_DT'),
	Lnk_xfm_cust_addr_role.CUST_ID.alias('CUST_ID'),
	Lnk_xfm_cust_addr_role.CUST_EFF_DT.alias('CUST_EFF_DT'),
	Lnk_xfm_cust_addr_role.CUST_TO_ACCT_REL_CD.alias('CUST_TO_ACCT_REL_CD'),
	Lnk_xfm_cust_addr_role.CUST_TO_ACCT_REL_DESC.alias('CUST_TO_ACCT_REL_DESC'),
	Lnk_xfm_cust_addr_role.PRMY_SCNDY_CD.alias('PRMY_SCNDY_CD'),
	Lnk_xfm_cust_addr_role.CIF_ACCT_BRNCH.alias('CIF_ACCT_BRNCH'),
	Lnk_xfm_cust_addr_role.CIF_ACCT_NBR.alias('CIF_ACCT_NBR'),
	Lnk_xfm_cust_addr_role.CIF_CLOSE_DT.alias('CIF_CLOSE_DT'),
	Lnk_xfm_cust_addr_role.CIF_OPEN_DT.alias('CIF_OPEN_DT'),
	Lnk_xfm_cust_addr_role.CIF_PROD_CD.alias('CIF_PROD_CD'),
	Lnk_xfm_cust_addr_role.CIF_PROD_DESC.alias('CIF_PROD_DESC')
).filter("change_code = 2")

# COMMAND ----------
# Processing node cust_addr_role_update_2, type TARGET
# COLUMN COUNT: 14

cust_addr_role_update_2 = cust_addr_role_update.select('*')
spark.sql('drop table if exists cust_addr_role_update_2')
SA_CUSTOMER_DS.write.saveAsTable(cust_addr_role_update_2)

# COMMAND ----------
# Processing node cust_addr_role_insert_2, type TARGET
# COLUMN COUNT: 14

cust_addr_role_insert_2 = cust_addr_role_insert.select('*')
spark.sql('drop table if exists cust_addr_role_insert_2')
SA_CUSTOMER_DS.write.saveAsTable(cust_addr_role_insert_2)

# COMMAND ----------
# Processing node cust_addr_role_delete_2, type TARGET
# COLUMN COUNT: 14

cust_addr_role_delete_2 = cust_addr_role_delete.select('*')
spark.sql('drop table if exists cust_addr_role_delete_2')
SA_CUSTOMER_DS.write.saveAsTable(cust_addr_role_delete_2)