#Code converted on 2023-04-26 15:38:26
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
dbutils.widgets.text(name = 'SOURCE_DIR', defaultValue = '/nas_pp/dev/dw/data/conv_253')
SOURCE_DIR = dbutils.widgets.get("SOURCE_DIR")

dbutils.widgets.text(name = 'STG_DIR', defaultValue = '/nas_pp/dev/dw/stg')
STG_DIR = dbutils.widgets.get("STG_DIR")

dbutils.widgets.text(name = 'HASH_DIR', defaultValue = '/nas_pp/dev/dw/hash')
HASH_DIR = dbutils.widgets.get("HASH_DIR")

dbutils.widgets.text(name = 'FMT_DIR', defaultValue = '/nas_pp/dev/dw/fmt')
FMT_DIR = dbutils.widgets.get("FMT_DIR")

dbutils.widgets.text(name = 'BANK_NUM', defaultValue = '101')
BANK_NUM = dbutils.widgets.get("BANK_NUM")

dbutils.widgets.text(name = 'DATA_DATE', defaultValue = '20060806')
DATA_DATE = dbutils.widgets.get("DATA_DATE")


# COMMAND ----------
# Processing node LkpTIMSTNaturalHash, type SOURCE
# COLUMN COUNT: 3
# Original node name DepositTIMSTNaturalHash_REF, link LkpTIMSTNaturalHash

LkpTIMSTNaturalHash = spark.read.csv("DepositTIMSTNaturalHash", sep=',', header='false')

# COMMAND ----------
# Processing node ESTMT_LKP, type SOURCE
# COLUMN COUNT: 5
# Original node name ABalEstmtDPSTHash, link ESTMT_LKP

ESTMT_LKP = spark.read.csv("ABalEstmtDPSTHash", sep=',', header='false')

# COMMAND ----------
# Processing node NSTMT_LKP, type SOURCE
# COLUMN COUNT: 5
# Original node name ABalNstmtDPSTHash, link NSTMT_LKP

NSTMT_LKP = spark.read.csv("ABalNstmtDPSTHash", sep=',', header='false')

# COMMAND ----------
# Processing node SEQ_IN, type SOURCE
# COLUMN COUNT: 386
# Original node name TIMST, link SEQ_IN

SEQ_IN = spark.read.csv(""+SOURCE_DIR+"/timeV8.3.1/time_tismst_"+BANK_NUM+"_"+DATA_DATE+".253", sep='ý', header='true')

# COMMAND ----------
# Processing node TSTMT_LKP, type SOURCE
# COLUMN COUNT: 5
# Original node name ABalTstmtDPSTHash, link TSTMT_LKP

TSTMT_LKP = spark.read.csv("ABalTstmtDPSTHash", sep=',', header='false')

# COMMAND ----------
# Processing node CSTMT_LKP, type SOURCE
# COLUMN COUNT: 5
# Original node name ABalCstmtDPSTHash, link CSTMT_LKP

CSTMT_LKP = spark.read.csv("ABalCstmtDPSTHash", sep=',', header='false')

# COMMAND ----------
# Processing node ACCT_Lkp, type SOURCE
# COLUMN COUNT: 91
# Original node name DepositTIMSTHash_REF, link ACCT_Lkp

ACCT_Lkp = spark.read.csv("DepositTIMSTHash", sep=',', header='false')

# COMMAND ----------
# Processing node LkpECF_STATUSCD, type SOURCE
# COLUMN COUNT: 6
# Original node name ECF_STATUSCD_IN2_Hash, link LkpECF_STATUSCD

LkpECF_STATUSCD = spark.read.csv("ECF_STATUSCD_IN2_Hash", sep=',', header='false')

# COMMAND ----------
# Processing node NULLSTMT_LKP, type SOURCE
# COLUMN COUNT: 5
# Original node name ABalNULLstmtDPSTHash, link NULLSTMT_LKP

NULLSTMT_LKP = spark.read.csv("ABalNULLstmtDPSTHash", sep=',', header='false')

# COMMAND ----------
# Processing node ODH_LKP, type SOURCE
# COLUMN COUNT: 3
# Original node name OdateHash_REF, link ODH_LKP

ODH_LKP = spark.read.csv("OdateHash", sep=',', header='false')

# COMMAND ----------
# Processing node ABCDDH_LKP, type SOURCE
# COLUMN COUNT: 29
# Original node name AcctBalTimstDpstDailyHash_Lkp, link ABCDDH_LKP

ABCDDH_LKP = spark.read.csv("AcctBalTimstDpstDailyHash", sep=',', header='false')

# COMMAND ----------
# Processing node Timst_INTRATE_Lkup, type SOURCE
# COLUMN COUNT: 5
# Original node name AcctBal_Timst_INTRATE_Dpst_Hash_REF, link Timst_INTRATE_Lkup

Timst_INTRATE_Lkup = spark.read.csv("AcctBal_Timst_INTRATE_Dpst_Hash", sep=',', header='false')

# COMMAND ----------
# Processing node BSTMT_LKP, type SOURCE
# COLUMN COUNT: 5
# Original node name ABalBstmtDPSTHash, link BSTMT_LKP

BSTMT_LKP = spark.read.csv("ABalBstmtDPSTHash", sep=',', header='false')

# COMMAND ----------
# Processing node PSTMT_LKP, type SOURCE
# COLUMN COUNT: 5
# Original node name ABalPstmtDPSTHash, link PSTMT_LKP

PSTMT_LKP = spark.read.csv("ABalPstmtDPSTHash", sep=',', header='false')

# COMMAND ----------
# Processing node PASS_IN, type TRANSFORMATION
# COLUMN COUNT: 55
# Original node name ACCT_BAL_TIMST, link PASS_IN

# Joining dataframes BSTMT_LKP, CSTMT_LKP, ESTMT_LKP, LkpECF_STATUSCD, LkpTIMSTNaturalHash, NSTMT_LKP, NULLSTMT_LKP, PSTMT_LKP, SEQ_IN, TSTMT_LKP, Timst_INTRATE_Lkup to form PASS_IN
PASS_IN_joined = BSTMT_LKP.join(CSTMT_LKP, BSTMT_LKP.sys_row_id == CSTMT_LKP.sys_row_id, 'inner')
 CSTMT_LKP.join(ESTMT_LKP, CSTMT_LKP.sys_row_id == ESTMT_LKP.sys_row_id, 'inner')
  ESTMT_LKP.join(LkpECF_STATUSCD, ESTMT_LKP.sys_row_id == LkpECF_STATUSCD.sys_row_id, 'inner')
   LkpECF_STATUSCD.join(LkpTIMSTNaturalHash, LkpECF_STATUSCD.sys_row_id == LkpTIMSTNaturalHash.sys_row_id, 'inner')
    LkpTIMSTNaturalHash.join(NSTMT_LKP, LkpTIMSTNaturalHash.sys_row_id == NSTMT_LKP.sys_row_id, 'inner')
     NSTMT_LKP.join(NULLSTMT_LKP, NSTMT_LKP.sys_row_id == NULLSTMT_LKP.sys_row_id, 'inner')
      NULLSTMT_LKP.join(PSTMT_LKP, NULLSTMT_LKP.sys_row_id == PSTMT_LKP.sys_row_id, 'inner')
       PSTMT_LKP.join(SEQ_IN, PSTMT_LKP.sys_row_id == SEQ_IN.sys_row_id, 'inner')
        SEQ_IN.join(TSTMT_LKP, SEQ_IN.sys_row_id == TSTMT_LKP.sys_row_id, 'inner')
         TSTMT_LKP.join(Timst_INTRATE_Lkup, TSTMT_LKP.sys_row_id == Timst_INTRATE_Lkup.sys_row_id, 'inner')
PASS_IN = PASS_IN_joined
PASS_IN = PASS_IN.withColumn("ACCTBALID", when((length(LkpTIMSTNaturalHash.ACCT_ID) == lit(0) ),(KeyMgtGetNextValueConcurrent((lit('acct_bal_id'))) )).otherwise(LkpTIMSTNaturalHash.ACCT_ID))
	
PASS_IN = PASS_IN.withColumn("CDLastIntReprcDt", when((NOT(Timst_INTRATE_Lkup.NOTFOUND) , when((trim(SEQ_IN.TIMST_INTRATE) == Timst_INTRATE_Lkup.TIMST_INTRATE )).otherwise((Timst_INTRATE_Lkup.LAST_INT_REPRC_DT ))).otherwise((DATA_DATE) )).otherwise(trim(SEQ_IN.TIMST_RENLAST))).select(
	LkpTIMSTNaturalHash.ACCT_ID.alias('ACCT_ID'),
	(lit('{DATA_DATE}')).alias('AS_OF_DT'),
	(lit(None).cast(NullType())).alias('DATA_DT'),
	(when((length(SEQ_IN.TIMST_BALCUR) == lit(0) ),(lit(0) )).otherwise(SEQ_IN.TIMST_BALCUR)).alias('CUR_BAL'),
	(when((length(SEQ_IN.TIMST_BALCUR) == lit(0) ),(lit(0) )).otherwise(SEQ_IN.TIMST_BALCUR)).alias('BANK_SHR_BAL'),
	(when((length(SEQ_IN.TIMST_INTRATE) == lit(0) ),(lit(0) )).otherwise(SEQ_IN.TIMST_INTRATE * lit(100))).alias('INT_RATE'),
	(lit(None).cast(NullType())).alias('INT_DT'),
	(lit(None).cast(NullType())).alias('INT_INDX_BASE_RATE'),
	(when(((SEQ_IN.TIMST_BALCUR != lit(0)) &(SEQ_IN.TIMST_INTRATE != lit(0)) , when((SEQ_IN.TIMST_INTCODE == lit('B') ).otherwise(SEQ_IN.TIMST_BALCUR *(SEQ_IN.TIMST_INTRATE) /(NoOfDaysInYear(lit('{DATA_DATE}'))) ).otherwise(when((SEQ_IN.TIMST_INTCODE == lit('C') )).otherwise(((SEQ_IN.TIMST_BALCAGR / substring((LastDayCurrMonth(lit('{DATA_DATE}'))) ))).otherwise((lit(2))) *(SEQ_IN.TIMST_INTRATE) /(NoOfDaysInYear(lit('{DATA_DATE}'))) ))).otherwise((lit(0)) )).otherwise(lit(0)))).alias('INT_PERDIEM_AMT'),
	(lit(0)).alias('MTD_LOWEST_CUR_ACCT_BAL'),
	(lit(0)).alias('YTD_LOWEST_CUR_ACCT_BAL'),
	(lit(0)).alias('MTD_HIGHEST_CUR_ACCT_BAL'),
	(lit(0)).alias('YTD_HIGHEST_CUR_ACCT_BAL'),
	(when((LkpECF_STATUSCD.OUT_1 == lit('C') , None ).otherwise(when((Iconv(trim(SEQ_IN.TIMST_RATENEXT) ).otherwise(lit('D')) > Iconv(trim(SEQ_IN.TIMST_ISSDATE) )).otherwise((lit('D')) ))).otherwise((trim(SEQ_IN.TIMST_RATENEXT) )).otherwise(trim(SEQ_IN.TIMST_RENNEXT)))).alias('NEXT_INT_REPRC_DT'),
	(date_format(current_timestamp , lit('y-MM-dd'))).alias('ENTRY_DT'),
	(lit(0)).alias('MTD_AGGR_BANK_SHR_BAL'),
	(lit(0)).alias('MTD_AGGR_BAL'),
	(when((length(SEQ_IN.TIMST_INTENP) == lit(0) ),(lit(0) )).otherwise(SEQ_IN.TIMST_INTENP)).alias('MTD_INT_ACCR_AMT'),
	(lit(0)).alias('MTD_AVG_BAL'),
	(lit(0)).alias('MTD_AVG_BANK_BAL'),
	(lit(0)).alias('MTD_AVG_WT_INT_RATE'),
	(when((Iconv(trim(SEQ_IN.TIMST_INTDTCHG) , lit('D')) > Iconv(trim(SEQ_IN.TIMST_ISSDATE) ).otherwise(lit('D')) )).otherwise((trim(SEQ_IN.TIMST_INTDTCHG) )).otherwise(None)).alias('LAST_INT_REPRC_DT'),
	(lit(0)).alias('MTD_AGGR_INT_RATE'),
	(lit(0)).alias('YTD_INT_ACCR_AMT'),
	(lit(0)).alias('YTD_AGGR_BAL'),
	(lit(0)).alias('YTD_AVG_BAL'),
	(lit(0)).alias('YTD_AVG_BANK_SHR_BAL'),
	(lit(0)).alias('YTD_AGGR_BANK_SHR_BAL'),
	(lit(0)).alias('YTD_AGGR_INT_RATE'),
	(when((length(trim(SEQ_IN.TIMST_INTENP)) > lit(0) ),(SEQ_IN.TIMST_INTENP )).otherwise(lit(0))).alias('INT_ACCR_BAL'),
	(when(((SEQ_IN.TIMST_BALCUR != lit(0)) &(SEQ_IN.TIMST_INTRATE != lit(0)) , when((SEQ_IN.TIMST_INTCODE == lit('B') ).otherwise(SEQ_IN.TIMST_BALCUR *(SEQ_IN.TIMST_INTRATE) /(NoOfDaysInYear(lit('{DATA_DATE}'))) ).otherwise(when((SEQ_IN.TIMST_INTCODE == lit('C') )).otherwise(((SEQ_IN.TIMST_BALCAGR / substring((LastDayCurrMonth(lit('{DATA_DATE}'))) ))).otherwise((lit(2))) *(SEQ_IN.TIMST_INTRATE) /(NoOfDaysInYear(lit('{DATA_DATE}'))) ))).otherwise((lit(0)) )).otherwise(lit(0)))).alias('BANK_SHR_INT_PERDIEM_AMT'),
	(when((length(trim(SEQ_IN.TIMST_INTENP)) > lit(0) ),(SEQ_IN.TIMST_INTENP )).otherwise(lit(0))).alias('BANK_SHR_INT_ACCR_BAL'),
	(lit(0)).alias('GL_YTD_AGGR_BANK_SHR_INT_AMT'),
	(lit(0)).alias('GL_MTD_AGGR_BANK_SHR_INT_AMT'),
	(lit(0)).alias('GL_MTD_AGGR_BANK_SHR_BAL'),
	(lit(0)).alias('GL_YTD_AGGR_CUR_BAL'),
	(lit(0)).alias('GL_MTD_AGGR_CUR_BAL'),
	(lit(0)).alias('GL_YTD_AGGR_BANK_SHR_BAL'),
	(lit(0)).alias('GL_YTD_AGGR_INT_AMT'),
	(lit(0)).alias('GL_MTD_AGGR_INT_AMT'),
	(lit(0)).alias('GL_YTD_AVG_CUR_BAL'),
	(lit(0)).alias('GL_MTD_AVG_CUR_BAL'),
	(lit(0)).alias('GL_YTD_AVG_BANK_SHR_BAL'),
	(lit(0)).alias('GL_MTD_AVG_BANK_BAL'),
	(lit(0)).alias('GL_QTD_AGGR_BANK_SHR_BAL'),
	(lit(0)).alias('GL_QTD_AVG_BANK_SHR_BAL'),
	(lit(0)).alias('GL_QTD_AGGR_CUR_BAL'),
	(lit(0)).alias('GL_QTD_AVG_CUR_BAL'),
	SEQ_IN.TIMST_INST.alias('TIMST_INST'),
	SEQ_IN.TIMST_ACCOUNT.alias('TIMST_ACCOUNT'),
	(when(((length(trim(SEQ_IN.TIMST_INTRATE)) > lit(0)) &(TRY_trim(SEQ_IN.TIMST_INTRATE) .cast(NUMERIC) IS NOT None) ),(trim(SEQ_IN.TIMST_INTRATE) )).otherwise(lit(0))).alias('TIMST_INTRATE'),
	LkpECF_STATUSCD.OUT_1.alias('OPEN_CLOSE_CD'),
	(when(((length(trim(ESTMT_LKP.SUM_OF_E_STMT)) == lit(0)) &(length(trim(TSTMT_LKP.SUM_OF_T_STMT)) == lit(0)) &(length(trim(BSTMT_LKP.SUM_OF_B_STMT)) == lit(0)) , lit(0) ).otherwise(when(((ESTMT_LKP.SUM_OF_E_STMT > lit(0)) |(BSTMT_LKP.SUM_OF_B_STMT > lit(0)) ).otherwise(when((TSTMT_LKP.SUM_OF_T_STMT > lit(0) ).otherwise(TSTMT_LKP.SUM_OF_T_STMT + lit(1) )).otherwise((lit(1)) ))).otherwise((IFF(TSTMT_LKP.SUM_OF_T_STMT > lit(0) ))).otherwise((TSTMT_LKP.SUM_OF_T_STMT )).otherwise(lit(0))))).alias('ESTMT_CNT'),
	(when(((length(trim(NULLSTMT_LKP.RFACO_ACCT)) == lit(0)) &(length(trim(BSTMT_LKP.RFACO_ACCT)) == lit(0)) &(length(trim(CSTMT_LKP.RFACO_ACCT)) == lit(0)) &(length(trim(PSTMT_LKP.RFACO_ACCT)) == lit(0)) &(length(trim(ESTMT_LKP.RFACO_ACCT)) == lit(0)) &(length(trim(NSTMT_LKP.RFACO_ACCT)) == lit(0)) &(length(trim(TSTMT_LKP.RFACO_ACCT)) == lit(0)) , lit(1) ).otherwise(when((BSTMT_LKP.RFACO_OPTION == lit('B') ).otherwise(when(((length(trim(PSTMT_LKP.SUM_OF_P_STMT)) > lit(0)) &(PSTMT_LKP.SUM_OF_P_STMT > lit(0)) ).otherwise(PSTMT_LKP.SUM_OF_P_STMT + BSTMT_LKP.SUM_OF_B_STMT ).otherwise(when((BSTMT_LKP.SUM_OF_B_STMT > lit(0) ).otherwise(BSTMT_LKP.SUM_OF_B_STMT ).otherwise(lit(1)) ).otherwise(when((PSTMT_LKP.RFACO_OPTION == lit('P') ).otherwise(PSTMT_LKP.SUM_OF_P_STMT ).otherwise(IFF((length(trim(NULLSTMT_LKP.RFACO_ACCT)) > lit(0)) &(length(trim(BSTMT_LKP.RFACO_ACCT)) == lit(0)) &(length(trim(PSTMT_LKP.RFACO_ACCT)) == lit(0)) &(length(trim(ESTMT_LKP.RFACO_ACCT)) == lit(0)) )).otherwise((IFF(NULLSTMT_LKP.SUM_OF_NULL_STMT > lit(0) ))).otherwise((NULLSTMT_LKP.SUM_OF_NULL_STMT ))).otherwise((lit(1)) ))).otherwise((IFF((CSTMT_LKP.RFACO_OPTION == lit('C')) |(TSTMT_LKP.RFACO_OPTION == lit('T')) |(NSTMT_LKP.RFACO_OPTION == lit('N')) ))).otherwise((lit(0) )).otherwise(lit(0)))))))).alias('PAPER_STMT_CNT'),
	(when(((length(trim(SEQ_IN.TIMST_INTINDEX)) > lit(0)) &(TRY_trim(SEQ_IN.TIMST_INTINDEX) .cast(NUMERIC) IS NOT None) ),(trim(SEQ_IN.TIMST_INTINDEX) )).otherwise(lit(0))).alias('TIMST_INTINDEX')
)

# COMMAND ----------
# Processing node ACCT_BAL_OUT, type TRANSFORMATION
# COLUMN COUNT: 41
# Original node name Acct_bal, link ACCT_BAL_OUT

# Joining dataframes ACCT_Lkp, ODH_LKP, PASS_IN to form ACCT_BAL_OUT
ACCT_BAL_OUT_joined = ACCT_Lkp.join(ODH_LKP, ACCT_Lkp.sys_row_id == ODH_LKP.sys_row_id, 'inner')
 ODH_LKP.join(PASS_IN, ODH_LKP.sys_row_id == PASS_IN.sys_row_id, 'inner')
ACCT_BAL_OUT = ACCT_BAL_OUT_joined
ACCT_BAL_OUT = ACCT_BAL_OUT.withColumn("RptRecInd", SetRptRecInd(ACCT_Lkp.OPEN_CLOSE_CD , ACCT_Lkp.CLOSE_DT , PASS_IN.CUR_BAL , PASS_IN.BANK_SHR_BAL , .DATA_DATE)).select(
	PASS_IN.ACCT_ID.alias('ACCT_ID'),
	PASS_IN.AS_OF_DT.alias('AS_OF_DT'),
	PASS_IN.DATA_DT.alias('DATA_DT'),
	ODH_LKP.ODATE.alias('ODATE'),
	(when(((length(PASS_IN.CUR_BAL) > lit(0)) &(TRY_PASS_IN.CUR_BAL .cast(NUMERIC) IS NOT None) ),(PASS_IN.CUR_BAL )).otherwise(lit(0))).alias('CUR_BAL'),
	(when(((length(PASS_IN.BANK_SHR_BAL) > lit(0)) &(TRY_PASS_IN.BANK_SHR_BAL .cast(NUMERIC) IS NOT None) ),(PASS_IN.BANK_SHR_BAL )).otherwise(lit(0))).alias('BANK_SHR_BAL'),
	PASS_IN.INT_RATE.alias('INT_RATE'),
	PASS_IN.INT_DT.alias('INT_DT'),
	(when((trim(ACCT_Lkp.INT_RATE_TYP) == lit('V') ),((PASS_IN.TIMST_INTRATE - PASS_IN.TIMST_INTINDEX) * lit(100) )).otherwise(lit(0))).alias('INT_INDX_BASE_RATE'),
	PASS_IN.INT_PERDIEM_AMT.alias('INT_PERDIEM_AMT'),
	PASS_IN.MTD_LOWEST_CUR_ACCT_BAL.alias('MTD_LOWEST_CUR_ACCT_BAL'),
	PASS_IN.YTD_LOWEST_CUR_ACCT_BAL.alias('YTD_LOWEST_CUR_ACCT_BAL'),
	PASS_IN.MTD_HIGHEST_CUR_ACCT_BAL.alias('MTD_HIGHEST_CUR_ACCT_BAL'),
	PASS_IN.YTD_HIGHEST_CUR_ACCT_BAL.alias('YTD_HIGHEST_CUR_ACCT_BAL'),
	PASS_IN.NEXT_INT_REPRC_DT.alias('NEXT_INT_REPRC_DT'),
	PASS_IN.ENTRY_DT.alias('ENTRY_DT'),
	PASS_IN.MTD_AVG_WT_INT_RATE.alias('MTD_AVG_WT_INT_RATE'),
	PASS_IN.LAST_INT_REPRC_DT.alias('LAST_INT_REPRC_DT'),
	PASS_IN.MTD_AGGR_INT_RATE.alias('MTD_AGGR_INT_RATE'),
	PASS_IN.YTD_AGGR_INT_RATE.alias('YTD_AGGR_INT_RATE'),
	PASS_IN.INT_ACCR_BAL.alias('INT_ACCR_BAL'),
	PASS_IN.BANK_SHR_INT_PERDIEM_AMT.alias('BANK_SHR_INT_PERDIEM_AMT'),
	PASS_IN.BANK_SHR_INT_ACCR_BAL.alias('BANK_SHR_INT_ACCR_BAL'),
	PASS_IN.GL_YTD_AGGR_BANK_SHR_INT_AMT.alias('GL_YTD_AGGR_BANK_SHR_INT_AMT'),
	PASS_IN.GL_MTD_AGGR_BANK_SHR_INT_AMT.alias('GL_MTD_AGGR_BANK_SHR_INT_AMT'),
	PASS_IN.GL_MTD_AGGR_BANK_SHR_BAL.alias('GL_MTD_AGGR_BANK_SHR_BAL'),
	PASS_IN.GL_YTD_AGGR_CUR_BAL.alias('GL_YTD_AGGR_CUR_BAL'),
	PASS_IN.GL_MTD_AGGR_CUR_BAL.alias('GL_MTD_AGGR_CUR_BAL'),
	PASS_IN.GL_YTD_AGGR_BANK_SHR_BAL.alias('GL_YTD_AGGR_BANK_SHR_BAL'),
	PASS_IN.GL_YTD_AGGR_INT_AMT.alias('GL_YTD_AGGR_INT_AMT'),
	PASS_IN.GL_MTD_AGGR_INT_AMT.alias('GL_MTD_AGGR_INT_AMT'),
	PASS_IN.GL_YTD_AVG_CUR_BAL.alias('GL_YTD_AVG_CUR_BAL'),
	PASS_IN.GL_MTD_AVG_CUR_BAL.alias('GL_MTD_AVG_CUR_BAL'),
	PASS_IN.GL_YTD_AVG_BANK_SHR_BAL.alias('GL_YTD_AVG_BANK_SHR_BAL'),
	PASS_IN.GL_MTD_AVG_BANK_BAL.alias('GL_MTD_AVG_BANK_BAL'),
	PASS_IN.GL_QTD_AGGR_BANK_SHR_BAL.alias('GL_QTD_AGGR_BANK_SHR_BAL'),
	PASS_IN.GL_QTD_AVG_BANK_SHR_BAL.alias('GL_QTD_AVG_BANK_SHR_BAL'),
	PASS_IN.GL_QTD_AGGR_CUR_BAL.alias('GL_QTD_AGGR_CUR_BAL'),
	PASS_IN.GL_QTD_AVG_CUR_BAL.alias('GL_QTD_AVG_CUR_BAL'),
	(when((ACCT_Lkp.OPEN_CLOSE_CD == lit('C') ),(lit(0) )).otherwise(PASS_IN.ESTMT_CNT)).alias('ESTMT_CNT'),
	(when((ACCT_Lkp.OPEN_CLOSE_CD == lit('C') ),(lit(0) )).otherwise(PASS_IN.PAPER_STMT_CNT)).alias('PAPER_STMT_CNT')
)

# COMMAND ----------
# Processing node ACCT_OUT, type TRANSFORMATION
# COLUMN COUNT: 85
# Original node name Acct_bal, link ACCT_OUT

# Joining dataframes ACCT_Lkp, ODH_LKP, PASS_IN to form ACCT_OUT
ACCT_OUT_joined = ACCT_Lkp.join(ODH_LKP, ACCT_Lkp.sys_row_id == ODH_LKP.sys_row_id, 'inner')
 ODH_LKP.join(PASS_IN, ODH_LKP.sys_row_id == PASS_IN.sys_row_id, 'inner')
ACCT_OUT = ACCT_OUT_joined
ACCT_OUT = ACCT_OUT.withColumn("RptRecInd", SetRptRecInd(ACCT_Lkp.OPEN_CLOSE_CD , ACCT_Lkp.CLOSE_DT , PASS_IN.CUR_BAL , PASS_IN.BANK_SHR_BAL , .DATA_DATE)).select(
	ACCT_Lkp.ACCT_ID.alias('ACCT_ID'),
	ACCT_Lkp.AS_OF_DT.alias('AS_OF_DT'),
	ACCT_Lkp.COMP_ID.alias('COMP_ID'),
	(lit(None).cast(NullType())).alias('GL_LDGR_ACCT_ID'),
	ACCT_Lkp.LAST_INT_RATE_CHG_DT.alias('LAST_INT_RATE_CHG_DT'),
	ACCT_Lkp.NEXT_INT_RATE_CHG_DT.alias('NEXT_INT_RATE_CHG_DT'),
	ACCT_Lkp.LAST_STMT_DT.alias('LAST_STMT_DT'),
	ACCT_Lkp.NEXT_STMT_DT.alias('NEXT_STMT_DT'),
	ACCT_Lkp.RATE_INDX_ID.alias('RATE_INDX_ID'),
	(lit(None).cast(NullType())).alias('RPT_CC_ID'),
	ACCT_Lkp.ACCT_TYP.alias('ACCT_TYP'),
	ACCT_Lkp.SRC_BANK_NBR.alias('SRC_BANK_NBR'),
	ACCT_Lkp.SRC_BANK_NBR_DESC.alias('SRC_BANK_NBR_DESC'),
	ACCT_Lkp.APPL_CD.alias('APPL_CD'),
	ACCT_Lkp.APPL_DESC.alias('APPL_DESC'),
	ACCT_Lkp.SRC_ACCT_NBR.alias('SRC_ACCT_NBR'),
	ACCT_Lkp.PRGD_IND.alias('PRGD_IND'),
	ACCT_Lkp.OPEN_CLOSE_CD.alias('OPEN_CLOSE_CD'),
	ACCT_Lkp.OPEN_CLOSE_DESC.alias('OPEN_CLOSE_DESC'),
	ACCT_Lkp.SRC_DTL_STAT_CD.alias('SRC_DTL_STAT_CD'),
	ACCT_Lkp.SRC_DTL_STAT_DESC.alias('SRC_DTL_STAT_DESC'),
	ACCT_Lkp.ORIG_OPEN_DT.alias('ORIG_OPEN_DT'),
	ACCT_Lkp.OPEN_DT.alias('OPEN_DT'),
	ACCT_Lkp.CLOSE_DT.alias('CLOSE_DT'),
	ACCT_Lkp.PRCS_DT.alias('PRCS_DT'),
	ACCT_Lkp.ORIG_INT_BEG_DT.alias('ORIG_INT_BEG_DT'),
	ACCT_Lkp.INT_BEG_DT.alias('INT_BEG_DT'),
	ACCT_Lkp.RNWL_PRCS_DT.alias('RNWL_PRCS_DT'),
	ACCT_Lkp.RNWL_DT.alias('RNWL_DT'),
	ACCT_Lkp.ORIG_MAT_DT.alias('ORIG_MAT_DT'),
	ACCT_Lkp.MAT_DT.alias('MAT_DT'),
	ACCT_Lkp.OPEN_AMT.alias('OPEN_AMT'),
	ACCT_Lkp.ORIG_BANK_SHR_AMT.alias('ORIG_BANK_SHR_AMT'),
	ACCT_Lkp.ORIG_INT_RATE.alias('ORIG_INT_RATE'),
	ACCT_Lkp.CUR_TERM.alias('CUR_TERM'),
	ACCT_Lkp.INT_MRGN_RATE.alias('INT_MRGN_RATE'),
	ACCT_Lkp.ORIG_TERM.alias('ORIG_TERM'),
	ACCT_Lkp.ORIG_INT_INDX_BASE_RATE.alias('ORIG_INT_INDX_BASE_RATE'),
	ACCT_Lkp.SHORT_NAME.alias('SHORT_NAME'),
	ACCT_Lkp.SRC_TAX_ID_TYP.alias('SRC_TAX_ID_TYP'),
	ACCT_Lkp.TAX_ID_TYP.alias('TAX_ID_TYP'),
	ACCT_Lkp.TIN.alias('TIN'),
	ACCT_Lkp.SRC_EMP_CD.alias('SRC_EMP_CD'),
	ACCT_Lkp.SRC_EMP_DESC.alias('SRC_EMP_DESC'),
	ACCT_Lkp.EMP_CD.alias('EMP_CD'),
	ACCT_Lkp.EMP_DESC.alias('EMP_DESC'),
	ACCT_Lkp.INT_RATE_TYP.alias('INT_RATE_TYP'),
	ACCT_Lkp.SRC_INT_MRGN_ACTION_CD.alias('SRC_INT_MRGN_ACTION_CD'),
	ACCT_Lkp.INT_MRGN_ACTION_CD.alias('INT_MRGN_ACTION_CD'),
	ACCT_Lkp.SRC_INT_INDX_CD.alias('SRC_INT_INDX_CD'),
	ACCT_Lkp.INT_INDX_CD.alias('INT_INDX_CD'),
	ACCT_Lkp.INT_INDX_DESC.alias('INT_INDX_DESC'),
	ACCT_Lkp.SRC_INT_REPRC_FREQ_CD.alias('SRC_INT_REPRC_FREQ_CD'),
	ACCT_Lkp.SRC_INT_REPRC_DESC.alias('SRC_INT_REPRC_DESC'),
	ACCT_Lkp.SRC_INT_REPRC_FREQ_TERM.alias('SRC_INT_REPRC_FREQ_TERM'),
	ACCT_Lkp.INT_REPRC_FREQ_TERM.alias('INT_REPRC_FREQ_TERM'),
	ACCT_Lkp.SRC_INT_ACCR_BASIS_CD.alias('SRC_INT_ACCR_BASIS_CD'),
	ACCT_Lkp.INT_ACCR_BASIS_CD.alias('INT_ACCR_BASIS_CD'),
	ACCT_Lkp.INT_ACCR_BASIS_DESC.alias('INT_ACCR_BASIS_DESC'),
	ACCT_Lkp.NAICS_CD.alias('NAICS_CD'),
	ACCT_Lkp.SIC_CD.alias('SIC_CD'),
	ACCT_Lkp.SCNDY_LVL_INT_MRGN_RATE.alias('SCNDY_LVL_INT_MRGN_RATE'),
	ACCT_Lkp.SCNDY_LVL_INT_MRGN_ACTION_CD.alias('SCNDY_LVL_INT_MRGN_ACTION_CD'),
	ACCT_Lkp.ENTRY_DT.alias('ENTRY_DT'),
	ACCT_Lkp.SRC_RPT_BRNCH_NBR.alias('SRC_RPT_BRNCH_NBR'),
	ACCT_Lkp.SRC_PRMY_OFCR_CD.alias('SRC_PRMY_OFCR_CD'),
	ACCT_Lkp.SRC_SCNDY_OFCR_CD.alias('SRC_SCNDY_OFCR_CD'),
	ACCT_Lkp.NAICS_DESC.alias('NAICS_DESC'),
	(when((RptRecInd == lit('N') ),(RptRecInd )).otherwise(ACCT_Lkp.RPT_REC_IND)).alias('RPT_REC_IND'),
	ACCT_Lkp.CONVERTED_REC_OLD_ACCT_NBR.alias('CONVERTED_REC_OLD_ACCT_NBR'),
	ACCT_Lkp.CONVERTED_REC_CD.alias('CONVERTED_REC_CD'),
	ACCT_Lkp.SRC_CONVERTED_REC_CD.alias('SRC_CONVERTED_REC_CD'),
	ACCT_Lkp.GL_CC_ID.alias('GL_CC_ID'),
	ACCT_Lkp.MST_ACCT_ID.alias('MST_ACCT_ID'),
	ACCT_Lkp.MST_ACCT_EFF_DT.alias('MST_ACCT_EFF_DT'),
	ACCT_Lkp.STMT_ACCT_NBR.alias('STMT_ACCT_NBR'),
	ACCT_Lkp.CUTOFF_STMT_FREQ_CD.alias('CUTOFF_STMT_FREQ_CD'),
	ACCT_Lkp.CUTOFF_STMT_TERM.alias('CUTOFF_STMT_TERM'),
	ACCT_Lkp.STMT_IMAGE_CD.alias('STMT_IMAGE_CD'),
	ACCT_Lkp.STMT_IMAGE_DESC.alias('STMT_IMAGE_DESC'),
	ACCT_Lkp.PROD_DTL_ID.alias('PROD_DTL_ID'),
	ACCT_Lkp.INT_RATE_RESET_DAY_CNT.alias('INT_RATE_RESET_DAY_CNT'),
	ACCT_Lkp.ALLOW_RP_IND.alias('ALLOW_RP_IND'),
	ACCT_Lkp.RP_IND.alias('RP_IND'),
	ACCT_Lkp.ENOTICE_IND.alias('ENOTICE_IND')
).filter("NOT(ACCT_Lkp.NOTFOUND) AND LEN(ACCT_ID) > 0")

# COMMAND ----------
# Processing node Timst_INTRATE_OUT, type TRANSFORMATION
# COLUMN COUNT: 5
# Original node name Acct_bal, link Timst_INTRATE_OUT

# Joining dataframes ACCT_Lkp, ODH_LKP, PASS_IN to form Timst_INTRATE_OUT
Timst_INTRATE_OUT_joined = ACCT_Lkp.join(ODH_LKP, ACCT_Lkp.sys_row_id == ODH_LKP.sys_row_id, 'inner')
 ODH_LKP.join(PASS_IN, ODH_LKP.sys_row_id == PASS_IN.sys_row_id, 'inner')
Timst_INTRATE_OUT = Timst_INTRATE_OUT_joined
Timst_INTRATE_OUT = Timst_INTRATE_OUT.withColumn("RptRecInd", SetRptRecInd(ACCT_Lkp.OPEN_CLOSE_CD , ACCT_Lkp.CLOSE_DT , PASS_IN.CUR_BAL , PASS_IN.BANK_SHR_BAL , .DATA_DATE)).select(
	PASS_IN.TIMST_INST.alias('TIMST_INST'),
	PASS_IN.TIMST_ACCOUNT.alias('TIMST_ACCOUNT'),
	PASS_IN.TIMST_INTRATE.alias('TIMST_INTRATE'),
	PASS_IN.LAST_INT_REPRC_DT.alias('LAST_INT_REPRC_DT'),
	PASS_IN.NEXT_INT_REPRC_DT.alias('NEXT_INT_REPRC_DT')
).filter("OPEN_CLOSE_CD = 'O'")

# COMMAND ----------
# Processing node DepositTIMSTHash, type TARGET
# COLUMN COUNT: 85

DepositTIMSTHash = ACCT_OUT.select('*')
spark.sql('drop table if exists DepositTIMSTHash')
SA_CUSTOMER_DS.write.saveAsTable(DepositTIMSTHash)

# COMMAND ----------
# Processing node SEQ_IN_CONV2, type TRANSFORMATION
# COLUMN COUNT: 49
# Original node name Acct_bal_Final, link SEQ_IN_CONV2

# Joining dataframes ABCDDH_LKP, ACCT_BAL_OUT to form SEQ_IN_CONV2
SEQ_IN_CONV2_joined = ABCDDH_LKP.join(ACCT_BAL_OUT, ABCDDH_LKP.sys_row_id == ACCT_BAL_OUT.sys_row_id, 'inner')
SEQ_IN_CONV2 = SEQ_IN_CONV2_joined.select(
	ACCT_BAL_OUT.ACCT_ID.alias('ACCT_ID'),
	ACCT_BAL_OUT.AS_OF_DT.alias('AS_OF_DT'),
	ACCT_BAL_OUT.DATA_DT.alias('DATA_DT'),
	ACCT_BAL_OUT.CUR_BAL.alias('CUR_BAL'),
	ACCT_BAL_OUT.BANK_SHR_BAL.alias('BANK_SHR_BAL'),
	ACCT_BAL_OUT.INT_RATE.alias('INT_RATE'),
	ACCT_BAL_OUT.INT_DT.alias('INT_DT'),
	ACCT_BAL_OUT.INT_INDX_BASE_RATE.alias('INT_INDX_BASE_RATE'),
	ACCT_BAL_OUT.INT_PERDIEM_AMT.alias('INT_PERDIEM_AMT'),
	ABCDDH_LKP.MTD_LOWEST_CUR_ACCT_BAL.alias('MTD_LOWEST_CUR_ACCT_BAL'),
	ABCDDH_LKP.YTD_LOWEST_CUR_ACCT_BAL.alias('YTD_LOWEST_CUR_ACCT_BAL'),
	ABCDDH_LKP.MTD_HIGHEST_CUR_ACCT_BAL.alias('MTD_HIGHEST_CUR_ACCT_BAL'),
	ABCDDH_LKP.YTD_HIGHEST_CUR_ACCT_BAL.alias('YTD_HIGHEST_CUR_ACCT_BAL'),
	ACCT_BAL_OUT.NEXT_INT_REPRC_DT.alias('NEXT_INT_REPRC_DT'),
	ACCT_BAL_OUT.ENTRY_DT.alias('ENTRY_DT'),
	ACCT_BAL_OUT.MTD_AVG_WT_INT_RATE.alias('MTD_AVG_WT_INT_RATE'),
	ACCT_BAL_OUT.LAST_INT_REPRC_DT.alias('LAST_INT_REPRC_DT'),
	ACCT_BAL_OUT.MTD_AGGR_INT_RATE.alias('MTD_AGGR_INT_RATE'),
	ACCT_BAL_OUT.YTD_AGGR_INT_RATE.alias('YTD_AGGR_INT_RATE'),
	ACCT_BAL_OUT.INT_ACCR_BAL.alias('INT_ACCR_BAL'),
	ACCT_BAL_OUT.BANK_SHR_INT_PERDIEM_AMT.alias('BANK_SHR_INT_PERDIEM_AMT'),
	ACCT_BAL_OUT.BANK_SHR_INT_ACCR_BAL.alias('BANK_SHR_INT_ACCR_BAL'),
	(when(((length(ABCDDH_LKP.GL_YTD_AGGR_BANK_SHR_INT_AMT) > lit(0)) &(TRY_ABCDDH_LKP.GL_YTD_AGGR_BANK_SHR_INT_AMT .cast(NUMERIC) IS NOT None) ),(ABCDDH_LKP.GL_YTD_AGGR_BANK_SHR_INT_AMT )).otherwise(lit(0))).alias('GL_YTD_AGGR_BANK_SHR_INT_AMT'),
	(when(((length(ABCDDH_LKP.GL_MTD_AGGR_BANK_SHR_INT_AMT) > lit(0)) &(TRY_ABCDDH_LKP.GL_MTD_AGGR_BANK_SHR_INT_AMT .cast(NUMERIC) IS NOT None) ),(ABCDDH_LKP.GL_MTD_AGGR_BANK_SHR_INT_AMT )).otherwise(lit(0))).alias('GL_MTD_AGGR_BANK_SHR_INT_AMT'),
	(when(((length(ABCDDH_LKP.GL_MTD_AGGR_BANK_SHR_BAL)) &(TRY_ABCDDH_LKP.GL_MTD_AGGR_BANK_SHR_BAL .cast(NUMERIC) IS NOT None) ),(ABCDDH_LKP.GL_MTD_AGGR_BANK_SHR_BAL )).otherwise(lit(0))).alias('GL_MTD_AGGR_BANK_SHR_BAL'),
	(when(((length(ABCDDH_LKP.GL_YTD_AGGR_CUR_BAL) > lit(0)) &(TRY_ABCDDH_LKP.GL_YTD_AGGR_CUR_BAL .cast(NUMERIC) IS NOT None) ),(ABCDDH_LKP.GL_YTD_AGGR_CUR_BAL )).otherwise(lit(0))).alias('GL_YTD_AGGR_CUR_BAL'),
	(when(((length(ABCDDH_LKP.GL_MTD_AGGR_CUR_BAL) > lit(0)) &(TRY_ABCDDH_LKP.GL_MTD_AGGR_CUR_BAL .cast(NUMERIC) IS NOT None) ),(ABCDDH_LKP.GL_MTD_AGGR_CUR_BAL )).otherwise(lit(0))).alias('GL_MTD_AGGR_CUR_BAL'),
	(when(((length(ABCDDH_LKP.GL_YTD_AGGR_BANK_SHR_BAL) > lit(0)) &(TRY_ABCDDH_LKP.GL_YTD_AGGR_BANK_SHR_BAL .cast(NUMERIC) IS NOT None) ),(ABCDDH_LKP.GL_YTD_AGGR_BANK_SHR_BAL )).otherwise(lit(0))).alias('GL_YTD_AGGR_BANK_SHR_BAL'),
	(when(((length(ABCDDH_LKP.GL_YTD_AGGR_INT_AMT) > lit(0)) &(TRY_ABCDDH_LKP.GL_YTD_AGGR_INT_AMT .cast(NUMERIC) IS NOT None) ),(ABCDDH_LKP.GL_YTD_AGGR_INT_AMT )).otherwise(lit(0))).alias('GL_YTD_AGGR_INT_AMT'),
	(when(((length(ABCDDH_LKP.GL_MTD_AGGR_INT_AMT) > lit(0)) &(TRY_ABCDDH_LKP.GL_MTD_AGGR_INT_AMT .cast(NUMERIC) IS NOT None) ),(ABCDDH_LKP.GL_MTD_AGGR_INT_AMT )).otherwise(lit(0))).alias('GL_MTD_AGGR_INT_AMT'),
	(when(((length(ABCDDH_LKP.GL_YTD_AVG_CUR_BAL) > lit(0)) &(TRY_ABCDDH_LKP.GL_YTD_AVG_CUR_BAL .cast(NUMERIC) IS NOT None) ),(ABCDDH_LKP.GL_YTD_AVG_CUR_BAL )).otherwise(lit(0))).alias('GL_YTD_AVG_CUR_BAL'),
	(when(((length(ABCDDH_LKP.GL_MTD_AVG_CUR_BAL) > lit(0)) &(TRY_ABCDDH_LKP.GL_MTD_AVG_CUR_BAL .cast(NUMERIC) IS NOT None) ),(ABCDDH_LKP.GL_MTD_AVG_CUR_BAL )).otherwise(lit(0))).alias('GL_MTD_AVG_CUR_BAL'),
	(when(((length(ABCDDH_LKP.GL_YTD_AVG_BANK_SHR_BAL) > lit(0)) &(TRY_ABCDDH_LKP.GL_YTD_AVG_BANK_SHR_BAL .cast(NUMERIC) IS NOT None) ),(ABCDDH_LKP.GL_YTD_AVG_BANK_SHR_BAL )).otherwise(lit(0))).alias('GL_YTD_AVG_BANK_SHR_BAL'),
	(when(((length(ABCDDH_LKP.GL_MTD_AVG_BANK_BAL) > lit(0)) &(TRY_ABCDDH_LKP.GL_MTD_AVG_BANK_BAL .cast(NUMERIC) IS NOT None) ),(ABCDDH_LKP.GL_MTD_AVG_BANK_BAL )).otherwise(lit(0))).alias('GL_MTD_AVG_BANK_BAL'),
	(when(((length(ABCDDH_LKP.GL_QTD_AGGR_BANK_SHR_BAL) > lit(0)) &(TRY_ABCDDH_LKP.GL_QTD_AGGR_BANK_SHR_BAL .cast(NUMERIC) IS NOT None) ),(ABCDDH_LKP.GL_QTD_AGGR_BANK_SHR_BAL )).otherwise(lit(0))).alias('GL_QTD_AGGR_BANK_SHR_BAL'),
	(when(((length(ABCDDH_LKP.GL_QTD_AVG_BANK_SHR_BAL) > lit(0)) &(TRY_ABCDDH_LKP.GL_QTD_AVG_BANK_SHR_BAL .cast(NUMERIC) IS NOT None) ),(ABCDDH_LKP.GL_QTD_AVG_BANK_SHR_BAL )).otherwise(lit(0))).alias('GL_QTD_AVG_BANK_SHR_BAL'),
	(when(((length(ABCDDH_LKP.GL_QTD_AGGR_CUR_BAL) > lit(0)) &(TRY_ABCDDH_LKP.GL_QTD_AGGR_CUR_BAL .cast(NUMERIC) IS NOT None) ),(ABCDDH_LKP.GL_QTD_AGGR_CUR_BAL )).otherwise(lit(0))).alias('GL_QTD_AGGR_CUR_BAL'),
	(when(((length(ABCDDH_LKP.GL_QTD_AVG_CUR_BAL) > lit(0)) &(TRY_ABCDDH_LKP.GL_QTD_AVG_CUR_BAL .cast(NUMERIC) IS NOT None) ),(ABCDDH_LKP.GL_QTD_AVG_CUR_BAL )).otherwise(lit(0))).alias('GL_QTD_AVG_CUR_BAL'),
	(lit(0)).alias('PR_1_CAL_DAY_CUR_BAL'),
	(lit(0)).alias('PR_2_CAL_DAY_CUR_BAL'),
	(lit(0)).alias('PR_3_CAL_DAY_CUR_BAL'),
	(lit(0)).alias('PR_4_CAL_DAY_CUR_BAL'),
	(lit(0)).alias('PR_5_CAL_DAY_CUR_BAL'),
	(lit(0)).alias('PR_6_CAL_DAY_CUR_BAL'),
	(lit(0)).alias('PR_7_CAL_DAY_CUR_BAL'),
	(lit(0)).alias('PR_8_CAL_DAY_CUR_BAL'),
	(lit(0)).alias('PR_9_CAL_DAY_CUR_BAL'),
	ACCT_BAL_OUT.ESTMT_CNT.alias('ESTMT_CNT'),
	ACCT_BAL_OUT.PAPER_STMT_CNT.alias('PAPER_STMT_CNT')
)

# COMMAND ----------
# Processing node HASH_IN, type TARGET
# COLUMN COUNT: 49
# Original node name Timst_Daily_stg, link HASH_IN


HASH_IN = SEQ_IN_CONV2.select('*')
spark.sql('drop table if exists HASH_IN')
SA_CUSTOMER_DS.write.saveAsTable(HASH_IN)

# COMMAND ----------
# Processing node AcctBal_Timst_INTRATE_Dpst_Hash, type TARGET
# COLUMN COUNT: 5

AcctBal_Timst_INTRATE_Dpst_Hash = Timst_INTRATE_OUT.select('*')
spark.sql('drop table if exists AcctBal_Timst_INTRATE_Dpst_Hash')
SA_CUSTOMER_DS.write.saveAsTable(AcctBal_Timst_INTRATE_Dpst_Hash)

# COMMAND ----------
# Processing node AcctBal_Timst_Dpst_Hash, type TARGET
# COLUMN COUNT: 49

AcctBal_Timst_Dpst_Hash = HASH_IN.select('*')
spark.sql('drop table if exists AcctBal_Timst_Dpst_Hash')
SA_CUSTOMER_DS.write.saveAsTable(AcctBal_Timst_Dpst_Hash)