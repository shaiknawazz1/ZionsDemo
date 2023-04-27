# Code converted on 2023-04-26 15:38:19
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
dbutils.widgets.text(name='Appl', defaultValue='fah_fls')
Appl = dbutils.widgets.get("Appl")

dbutils.widgets.text(name='Src_Sys_Ref_Name', defaultValue='FLS170131D.TXT')
Src_Sys_Ref_Name = dbutils.widgets.get("Src_Sys_Ref_Name")

dbutils.widgets.text(name='PS_FAH', defaultValue='')
PS_FAH = dbutils.widgets.get("PS_FAH")

dbutils.widgets.text(name='FileName', defaultValue='FLS20170131D.TXT')
FileName = dbutils.widgets.get("FileName")

dbutils.widgets.text(name='ACCT_DATE', defaultValue='20170131')
ACCT_DATE = dbutils.widgets.get("ACCT_DATE")


# COMMAND ----------
# Processing node FLS_Extr_in_lnk, type SOURCE
# COLUMN COUNT: 1
# Original node name FLS_Ext, link FLS_Extr_in_lnk

FLS_Extr_in_lnk = spark.read.csv(
    "#PS_FAH.RECEIVE_DIR#/"+Appl+"/FLS"+ACCT_DATE+"D.TXT", sep=',', header='false')

# COMMAND ----------
# Processing node RowGen_lnk, type ROW_GENERATOR
# COLUMN COUNT: 1
# Original node name Process_control_rg, link RowGen_lnk

RowGen_lnk_schema = StructType([
    StructField('LKP_KEY', IntegerType(), True)
])

RowGen_lnk = spark.createDataFrame(data=[
    (0,)
],
    schema=RowGen_lnk_schema)

# COMMAND ----------
# Processing node Agg_In, type TRANSFORMATION
# COLUMN COUNT: 6
# Original node name FLS_Xfm, link Agg_In

Agg_In = FLS_Extr_in_lnk.select(
    (lit(1)).alias('LKP_KEY'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(57), lit(1))).alias('DR_CR_INDICATOR'),
    (when((substring(FLS_Extr_in_lnk.RECORD, lit(57)).otherwise(lit(1)) == lit('C'), trim(substring(
        FLS_Extr_in_lnk.RECORD, lit(42)), (lit(15))) / lit(100))).otherwise(lit(0))).alias('CREDIT_AMOUNT'),
    (when((substring(FLS_Extr_in_lnk.RECORD, lit(57)).otherwise(lit(1)) == lit('D'), trim(substring(
        FLS_Extr_in_lnk.RECORD, lit(42)), (lit(15))) / lit(100))).otherwise(lit(0))).alias('DEBIT_AMOUNT'),
    (lit('{ACCT_DATE}')).alias('ACCOUNTING_DATE'),
    (lit(1)).alias('COUNT')
).filter("SUBSTRING ( RECORD , 1 , 1 ) = 'L'")

# COMMAND ----------
# Processing node fls_hdr, type TRANSFORMATION
# COLUMN COUNT: 5
# Original node name FLS_Xfm, link fls_hdr

fls_hdr = FLS_Extr_in_lnk.select(
    (substring(FLS_Extr_in_lnk.RECORD, lit(1), lit(1))).alias('RECORD_TYPE'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(2), lit(3))).alias('SRC_SYS_CD'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(5), lit(7))).alias('TOTAL_LINES'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(12), lit(15))).alias('TOTAL_DEBITS'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(27), lit(15))).alias('TOTAL_CREDITS')
).filter("SUBSTRING ( RECORD , 1 , 1 ) = 'H'")

# COMMAND ----------
# Processing node fls_ln, type TRANSFORMATION
# COLUMN COUNT: 21
# Original node name FLS_Xfm, link fls_ln

fls_ln = FLS_Extr_in_lnk.select(
    (substring(FLS_Extr_in_lnk.RECORD, lit(1), lit(1))).alias('RECORD_TYPE'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(2), lit(8))).alias('EFFECTIVE_DATE'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(10), lit(4))).alias('CURRENCY'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(14), lit(10))).alias('FX_TYPE'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(24), lit(10))).alias('FX_RATE'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(34), lit(8))).alias('FX_DATE'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(42), lit(15))).alias('AMOUNT'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(57), lit(1))).alias('CR_DR_IND'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(58), lit(3))).alias('COMPANY'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(61), lit(6))).alias('ACCOUNT'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(67), lit(5))).alias('COST_CENTER'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(72), lit(4))).alias('LOCATION'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(76), lit(3))).alias('DIVISION'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(79), lit(5))).alias('PRODUCT'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(84), lit(3))).alias('INTERCOMPANY'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(87), lit(5))).alias('FUTURE'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(192), lit(30))).alias('LOAN'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(272), lit(50))).alias('TRAN_DESC'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(222), lit(50))).alias('TRAN_CODE'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(92), lit(100))).alias('LINE_DESC'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(322), lit(50))).alias('INTERNAL_GL_NUM')
).filter("SUBSTRING ( RECORD , 1 , 1 ) = 'L'")

# COMMAND ----------
# Processing node fls_Ext_ld_ds, type TRANSFORMATION
# COLUMN COUNT: 29
# Original node name FLS_Xfm, link fls_Ext_ld_ds

fls_Ext_ld_ds = FLS_Extr_in_lnk.select(
    @ OUTROWNUM.alias('LINE_NUM'),
    (lit('{Src_Sys_Ref_Name}')).alias('SOURCE_SYSTEM_REF_NAME'),
    (lit('FLS')).alias('SYS_CODE'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(1), lit(1))).alias('RECORD_TYPE'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(2), lit(4)) + lit('-') + substring(FLS_Extr_in_lnk.RECORD,
     lit(6), lit(2)) + lit('-') + substring(FLS_Extr_in_lnk.RECORD, lit(8), lit(2))).alias('EFFECTIVE_DATE'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(2), lit(4)) + lit('-') + substring(FLS_Extr_in_lnk.RECORD, lit(6),
     lit(2)) + lit('-') + substring(FLS_Extr_in_lnk.RECORD, lit(8), lit(2))).alias('ACCOUNTING_DATE'),
    (when((trim(substring(FLS_Extr_in_lnk.RECORD, lit(10)).otherwise(lit(4))) == lit('')).otherwise(lit('USD')).otherwise(
        trim(substring(FLS_Extr_in_lnk.RECORD)).otherwise((lit(10))).otherwise(lit(4))))).alias('CURRENCY_CODE'),
    (when((trim(substring(FLS_Extr_in_lnk.RECORD, lit(34)).otherwise(lit(8))) == lit(''), None, substring(
        FLS_Extr_in_lnk.RECORD), (lit(34))).otherwise(lit(8)) .cast(date))).alias('CURRENCY_CONVERSION_DATE'),
    (when((trim(substring(FLS_Extr_in_lnk.RECORD, lit(24)).otherwise(lit(10))) == lit(''), None, trim(
        substring(FLS_Extr_in_lnk.RECORD), (lit(24))).otherwise(lit(10))))).alias('CURRENCY_CONVERSION_RATE'),
    (trim(substring(FLS_Extr_in_lnk.RECORD, lit(14), lit(10)))).alias(
        'CURRENCY_CONVERSION_TYPE'),
    (trim(substring(FLS_Extr_in_lnk.RECORD, lit(58), lit(3)))).alias('COMPANY'),
    (trim(substring(FLS_Extr_in_lnk.RECORD, lit(61), lit(6)))).alias('NATURAL_ACCOUNT'),
    (trim(substring(FLS_Extr_in_lnk.RECORD, lit(67), lit(5)))).alias('COST_CENTER'),
    (trim(substring(FLS_Extr_in_lnk.RECORD, lit(76), lit(3)))).alias('DIVISION'),
    (trim(substring(FLS_Extr_in_lnk.RECORD, lit(79), lit(5)))).alias('PRODUCT'),
    (trim(substring(FLS_Extr_in_lnk.RECORD, lit(72), lit(4)))).alias('LOCATION'),
    (trim(substring(FLS_Extr_in_lnk.RECORD, lit(84), lit(3)))).alias('INTERCOMPANY'),
    (trim(substring(FLS_Extr_in_lnk.RECORD, lit(87), lit(5)))).alias('FUTURE'),
    (trim(substring(FLS_Extr_in_lnk.RECORD, lit(42), lit(15))) / lit(100)).alias('AMOUNT'),
    (trim(substring(FLS_Extr_in_lnk.RECORD, lit(57), lit(1)))).alias('DR_CR_INDICATOR'),
    (trim(substring(FLS_Extr_in_lnk.RECORD, lit(92), lit(100)))).alias(
        'LINE_DESCRIPTION'),
    (trim(substring(FLS_Extr_in_lnk.RECORD, lit(192), lit(30)))).alias('LOAN_NUMBER'),
    (trim(substring(FLS_Extr_in_lnk.RECORD, lit(272), lit(50)))).alias('TRAN_DESC'),
    (trim(substring(FLS_Extr_in_lnk.RECORD, lit(222), lit(50)))).alias('TRAN_CODE'),
    (trim(substring(FLS_Extr_in_lnk.RECORD, lit(322), lit(50)))).alias(
        'INTERNAL_GL_NUM'),
    (lit('I')).alias('STATUS_CODE'),
    FLS_Extr_in_lnk.CURRENT_DATE.alias('DATE_CREATED'),
    (- lit(999)).alias('CREATED_BY'),
    (lit(1)).alias('LKP_KEY')
).filter("SUBSTRING ( RECORD , 1 , 1 ) = 'L'")

# COMMAND ----------
# Processing node Hdr_Lkp_lnk, type TRANSFORMATION
# COLUMN COUNT: 4
# Original node name FLS_Xfm, link Hdr_Lkp_lnk

Hdr_Lkp_lnk = FLS_Extr_in_lnk.select(
    (lit(1)).alias('LKP_KEY'),
    (substring(FLS_Extr_in_lnk.RECORD, lit(5), lit(7))).alias('RECORD_COUNT'),
    (trim(substring(FLS_Extr_in_lnk.RECORD, lit(27), lit(15))) /
     lit(100)).alias('CREDIT_AMOUNT'),
    (trim(substring(FLS_Extr_in_lnk.RECORD, lit(12), lit(15))) /
     lit(100)).alias('DEBIT_AMOUNT')
).filter("SUBSTRING ( RECORD , 1 , 1 ) = 'H'")

# COMMAND ----------
# Processing node FLS_Extr_ds, type TARGET
# COLUMN COUNT: 21

FLS_Extr_ds = fls_ln.select('*')
spark.sql('drop table if exists FLS_Extr_ds')
SA_CUSTOMER_DS.write.saveAsTable(FLS_Extr_ds)

# COMMAND ----------
# Processing node processctlsp_lnk, type AGGREGATOR
# COLUMN COUNT: 5
# Original node name FLS_Agg, link processctlsp_lnk

processctlsp_lnk = Agg_In.groupBy("LKP_KEY", "ACCOUNTING_DATE").agg(
    sum("COUNT").alias("TOTAL_RECORDS"),
    sum("CREDIT_AMOUNT").alias("TOTAL_CREDIT_AMOUNT"),
    sum("DEBIT_AMOUNT").alias("TOTAL_DEBIT_AMOUNT")).select(
    Agg_In.LKP_KEY.alias('LKP_KEY'),
    Agg_In.ACCOUNTING_DATE.alias('ACCOUNTING_DATE'),
    'TOTAL_RECORDS',
    'TOTAL_CREDIT_AMOUNT',
    'TOTAL_DEBIT_AMOUNT'
)

# COMMAND ----------
# Processing node FLS_hdr, type TARGET
# COLUMN COUNT: 5

FLS_hdr = fls_hdr.select('*')
spark.sql('drop table if exists FLS_hdr')
SA_CUSTOMER_DS.write.saveAsTable(FLS_hdr)

# COMMAND ----------
# Processing node FLS_Ext_ld_ds, type TARGET
# COLUMN COUNT: 29

FLS_Ext_ld_ds = fls_Ext_ld_ds.select('*')
spark.sql('drop table if exists FLS_Ext_ld_ds')
SA_CUSTOMER_DS.write.saveAsTable(FLS_Ext_ld_ds)

# COMMAND ----------
# Processing node Processid_lnk, type MERGE
# COLUMN COUNT: 7
# Original node name Processid_lkp, link Processid_lnk

Processid_lnk = RowGen_lnk.join(processctlsp_lnk, [processctlsp_lnk.LKP_KEY == RowGen_lnk.LKP_KEY], 'LEFT_OUTER').join(Hdr_Lkp_lnk, [Hdr_Lkp_lnk.LKP_KEY == RowGen_lnk.LKP_KEY], 'LEFT_OUTER').select(
    processctlsp_lnk.ACCOUNTING_DATE.alias('ACCOUNTING_DATE'),
    processctlsp_lnk.TOTAL_RECORDS.alias('TOTAL_RECORDS_LINES'),
    Hdr_Lkp_lnk.RECORD_COUNT.alias('TOTAL_RECORDS_HDR'),
    processctlsp_lnk.TOTAL_DEBIT_AMOUNT.alias('TOTAL_DEBIT_AMOUNT_LINES'),
    processctlsp_lnk.TOTAL_CREDIT_AMOUNT.alias('TOTAL_CREDIT_AMOUNT_LINES'),
    Hdr_Lkp_lnk.DEBIT_AMOUNT.alias('DEBIT_AMOUNT_HDR'),
    Hdr_Lkp_lnk.CREDIT_AMOUNT.alias('CREDIT_AMOUNT_HDR')
)

# COMMAND ----------
# Processing node Ind_Out_lnk, type TRANSFORMATION
# COLUMN COUNT: 1
# Original node name FLS_bal_trn, link Ind_Out_lnk

Ind_Out_lnk = Processid_lnk
Ind_Out_lnk = Ind_Out_lnk.withColumn(
    "LINESCREDIT", Processid_lnk.TOTAL_CREDIT_AMOUNT_LINES)

Ind_Out_lnk = Ind_Out_lnk.withColumn(
    "LINESDEBIT", Processid_lnk.TOTAL_DEBIT_AMOUNT_LINES)

Ind_Out_lnk = Ind_Out_lnk.withColumn(
    "HDRCREDIT", Processid_lnk.CREDIT_AMOUNT_HDR)

Ind_Out_lnk = Ind_Out_lnk.withColumn(
    "HDRDEBIT", Processid_lnk.DEBIT_AMOUNT_HDR)

Ind_Out_lnk = Ind_Out_lnk.withColumn(
    "LINESCOUNT", Processid_lnk.TOTAL_RECORDS_LINES)

Ind_Out_lnk = Ind_Out_lnk.withColumn(
    "HDRCOUNT", Processid_lnk.TOTAL_RECORDS_HDR)

Ind_Out_lnk = Ind_Out_lnk.withColumn("BALDESC", when(((LINESCREDIT == HDRCREDIT) & (LINESDEBIT == HDRDEBIT) & (HDRCREDIT == HDRDEBIT) & (LINESCREDIT == LINESDEBIT) & (HDRCOUNT == LINESCOUNT)), (lit('BAL'))).otherwise(lit(' Validation failed for file: ') + Src_Sys_Ref_Name + char(lit(10)) + lit('Header counts:') + HDRCOUNT AS STRING) + lit('    Lines count:') + LINESCOUNT AS STRING) + char(lit(10)) + lit('Header credit amount:') + HDRCREDIT AS STRING) + lit('  Lines credit amount:') + CAST(LINESCREDIT .cast(STRING) + char(lit(10)) + lit('Header debit amount:') + CAST(HDRDEBIT .cast(STRING) + lit('  Lines debit amount:') + CAST(LINESDEBIT .cast(STRING))).select(
    Processid_lnk.BALDESC.alias('BAL_DESC')
)

# COMMAND ----------
# Processing node process_control_lnk, type TRANSFORMATION
# COLUMN COUNT: 9
# Original node name FLS_bal_trn, link process_control_lnk

process_control_lnk = Processid_lnk
process_control_lnk = process_control_lnk.withColumn(
    "LINESCREDIT", Processid_lnk.TOTAL_CREDIT_AMOUNT_LINES)

process_control_lnk = process_control_lnk.withColumn(
    "LINESDEBIT", Processid_lnk.TOTAL_DEBIT_AMOUNT_LINES)

process_control_lnk = process_control_lnk.withColumn(
    "HDRCREDIT", Processid_lnk.CREDIT_AMOUNT_HDR)

process_control_lnk = process_control_lnk.withColumn(
    "HDRDEBIT", Processid_lnk.DEBIT_AMOUNT_HDR)

process_control_lnk = process_control_lnk.withColumn(
    "LINESCOUNT", Processid_lnk.TOTAL_RECORDS_LINES)

process_control_lnk = process_control_lnk.withColumn(
    "HDRCOUNT", Processid_lnk.TOTAL_RECORDS_HDR)

process_control_lnk = process_control_lnk.withColumn("BALDESC", when(((LINESCREDIT == HDRCREDIT) & (LINESDEBIT == HDRDEBIT) & (HDRCREDIT == HDRDEBIT) & (LINESCREDIT == LINESDEBIT) & (HDRCOUNT == LINESCOUNT)), (lit('BAL'))).otherwise(lit(' Validation failed for file: ') + Src_Sys_Ref_Name + char(lit(10)) + lit('Header counts:') + HDRCOUNT AS STRING) + lit('    Lines count:') + LINESCOUNT AS STRING) + char(lit(10)) + lit('Header credit amount:') + HDRCREDIT AS STRING) + lit('  Lines credit amount:') + CAST(LINESCREDIT .cast(STRING) + char(lit(10)) + lit('Header debit amount:') + CAST(HDRDEBIT .cast(STRING) + lit('  Lines debit amount:') + CAST(LINESDEBIT .cast(STRING))).select(
    (when((Processid_lnk.ACCOUNTING_DATE == None, lit('{ACCT_DATE}') .cast(date))).otherwise(
        (DecimalToDate(Processid_lnk.ACCOUNTING_DATE)).otherwise(lit('YYYYMMDD')))).alias('ACCOUNTING_DATE'),
    (lit('{Src_Sys_Ref_Name}')).alias('FILE_NAME'),
    (lit('FLS')).alias('SYS_CD'),
    (lit('U')).alias('STATUS'),
    Processid_lnk.TOTAL_CREDIT_AMOUNT_LINES.alias('TOTAL_AMOUNT'),
    Processid_lnk.TOTAL_RECORDS_LINES.alias('TOTAL_RECORDS'),
    (lit('I')).alias('STAGE'),
    (lit('-999')).alias('CREATED_BY'),
    (lit(1)).alias('LKP_KEY')
)

# COMMAND ----------
# Processing node process_control_ds, type TARGET
# COLUMN COUNT: 9

process_control_ds = process_control_lnk.select('*')
spark.sql('drop table if exists process_control_ds')
SA_CUSTOMER_DS.write.saveAsTable(process_control_ds)

# COMMAND ----------
# Processing node FLS_BAL_STATUS_Seq, type TARGET
# COLUMN COUNT: 1

FLS_BAL_STATUS_Seq = Ind_Out_lnk.select('*')
spark.sql('drop table if exists FLS_BAL_STATUS_Seq')
SA_CUSTOMER_DS.write.saveAsTable(FLS_BAL_STATUS_Seq)
