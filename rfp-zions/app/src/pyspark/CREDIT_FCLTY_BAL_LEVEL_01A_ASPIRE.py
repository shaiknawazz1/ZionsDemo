# Code converted on 2023-04-27 14:43:10
import os
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext('local')
spark = SparkSession(sc)


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
os.environ['SOURCE_DIR'] = '/nas_pp/test/dw/data/conv_253'
SOURCE_DIR = os.getenv('SOURCE_DIR')

os.environ['STG_DIR'] = '/nas_pp/test/dw/stg'
STG_DIR = os.getenv('STG_DIR')

os.environ['DS_DIR'] = '/nas_pp/test/dw/stg_ds'
DS_DIR = os.getenv('DS_DIR')

os.environ['FMT_DIR'] = '/nas_pp/test/dw/fmt'
FMT_DIR = os.getenv('FMT_DIR')

os.environ['ECF_DIR'] = '/nas_pp/test/dw/data/ecf'
ECF_DIR = os.getenv('ECF_DIR')

os.environ['TEMP_DIR'] = '/nas_pp/test/dw/temp'
TEMP_DIR = os.getenv('TEMP_DIR')

os.environ['REJECT_DIR'] = '/nas_pp/test/dw/rejects'
REJECT_DIR = os.getenv('REJECT_DIR')

os.environ['REC_DIR'] = '/nas_pp/test/dw/recovery'
REC_DIR = os.getenv('REC_DIR')

os.environ['SKEY_DIR'] = '/nas_pp/test/dw/stg_keys'
SKEY_DIR = os.getenv('SKEY_DIR')

os.environ['APPL_CD'] = 'LS'
APPL_CD = os.getenv('APPL_CD')

os.environ['BANK_NUM'] = '100'
BANK_NUM = os.getenv('BANK_NUM')

os.environ['DATA_DATE'] = '20220831'
DATA_DATE = os.getenv('DATA_DATE')


# Processing node CONTRACT_GENERAL_LEDGER_IN, type SOURCE
# COLUMN COUNT: 42
# Original node name ASPIRE_ADHOCREPORT_CONTRACT_GENERAL_LEDGER_DS, link CONTRACT_GENERAL_LEDGER_IN

CONTRACT_GENERAL_LEDGER_IN = spark.read.csv(
    ""+DS_DIR+"/aspire/aspire_adhocreport_contract_general_ledger.ds", sep=',', header='false')

# Processing node aspire_adhocreport_contract_payment_summary_IN, type SOURCE
# COLUMN COUNT: 23
# Original node name DS_aspire_adhocreport_con_pmnt_summcomp, link aspire_adhocreport_contract_payment_summary_IN

aspire_adhocreport_contract_payment_summary_IN = spark.read.csv(
    ""+DS_DIR+"/aspire/aspire_adhocreport_contract_payment_summary.ds", sep=',', header='false')

# Processing node spire_adhocreport_contract_equipment_summary_IN, type SOURCE
# COLUMN COUNT: 7
# Original node name ASPIRE_ADHOCREPORT_CONTRACT_EQUIPMENT_SUMMARY_DS, link spire_adhocreport_contract_equipment_summary_IN

spire_adhocreport_contract_equipment_summary_IN = spark.read.csv(
    ""+DS_DIR+"/aspire/aspire_adhocreport_contract_equipment_summary.ds", sep=',', header='false')

# Processing node CONTRACT_MASTER_IN, type SOURCE
# COLUMN COUNT: 141
# Original node name aspire_acct_driver_file_ds, link CONTRACT_MASTER_IN

CONTRACT_MASTER_IN = spark.read.csv(
    ""+DS_DIR+"/aspire/aspire_acct_driver_file.ds", sep=',', header='false')

# Processing node aspire_dbo_doc_gen_con_idc_zcc_idc_IN, type SOURCE
# COLUMN COUNT: 4
# Original node name aspire_dbo_doc_gen_con_idc_zcc_idc_ds, link aspire_dbo_doc_gen_con_idc_zcc_idc_IN

aspire_dbo_doc_gen_con_idc_zcc_idc_IN = spark.read.csv(
    ""+DS_DIR+"/aspire/aspire_dbo_doc_gen_con_idc_zcc_idc.ds", sep=',', header='false')

# Processing node aspire_dbo_lti_rpt_renewal_status_IN, type SOURCE
# COLUMN COUNT: 20
# Original node name ASPIRE_DBO_LTI_RPT_RENEWAL_STATUS_DS, link aspire_dbo_lti_rpt_renewal_status_IN

aspire_dbo_lti_rpt_renewal_status_IN = spark.read.csv(
    ""+DS_DIR+"/aspire/aspire_dbo_lti_rpt_renewal_status.ds", sep=',', header='false')

# Processing node earnings_schedule_IN, type SOURCE
# COLUMN COUNT: 19
# Original node name aspire_dbo_earnings_schedule, link earnings_schedule_IN

earnings_schedule_IN = spark.read.csv(
    ""+DS_DIR+"/aspire/aspire_dbo_earnings_schedule.ds", sep=',', header='false')

# Processing node ODATE, type SOURCE
# COLUMN COUNT: 4
# Original node name ODATE_DS1, link ODATE

ODATE = spark.read.csv(""+DS_DIR+"/Odate.ds", sep=',', header='false')

# Processing node bookvalue_booked_contract_component_IN, type SOURCE
# COLUMN COUNT: 40
# Original node name ASPIRE_BOOKVALUE_BOOKED_CONTRACT_COMPONENT_DS, link bookvalue_booked_contract_component_IN

bookvalue_booked_contract_component_IN = spark.read.csv(
    ""+DS_DIR+"/aspire/aspire_bookvalue_booked_contract_component.ds", sep=',', header='false')

# Processing node aspire_dbo_doc_gen_con_contract_IN, type SOURCE
# COLUMN COUNT: 12
# Original node name ASPIRE_DBO_DOC_GEN_CON_CONTRACT_DS, link aspire_dbo_doc_gen_con_contract_IN

aspire_dbo_doc_gen_con_contract_IN = spark.read.csv(
    ""+DS_DIR+"/aspire/aspire_dbo_doc_gen_con_contract.ds", sep=',', header='false')

# Processing node LKP_NATL_ACCT, type SOURCE
# COLUMN COUNT: 2
# Original node name ASPIRE_ACCT_NATL_KEY_DS, link LKP_NATL_ACCT

LKP_NATL_ACCT = spark.read.csv(
    ""+DS_DIR+"/aspire/natl_key/aspire_acct_natl_key.ds", sep=',', header='false')

# Processing node aspire_dbo_doc_gen_con_contract_udf_extensions_IN, type SOURCE
# COLUMN COUNT: 3
# Original node name aspire_dbo_doc_gen_con_contract_udf_extensions_ds, link aspire_dbo_doc_gen_con_contract_udf_extensions_IN

aspire_dbo_doc_gen_con_contract_udf_extensions_IN = spark.read.csv(
    ""+DS_DIR+"/aspire/aspire_dbo_doc_gen_con_contract_udf_extensions.ds", sep=',', header='false')

# Processing node CONTRACT_AGING_DS, type SOURCE
# COLUMN COUNT: 64
# Original node name ASPIRE_ADHOCREPORT_CONTRACT_AGING_DS, link CONTRACT_AGING_DS

CONTRACT_AGING_DS = spark.read.csv(
    ""+DS_DIR+"/aspire/aspire_adhocreport_contract_aging.ds", sep=',', header='false')

# Processing node ASPIRE_DBO_LTI_VALUES_IN, type SOURCE
# COLUMN COUNT: 6
# Original node name ASPIRE_DBO_LTI_VALUES_DS, link ASPIRE_DBO_LTI_VALUES_IN

ASPIRE_DBO_LTI_VALUES_IN = spark.read.csv(
    ""+DS_DIR+"/aspire/aspire_dbo_lti_values.ds", sep=',', header='false')

# Processing node CONTRACT_MASTER, type TRANSFORMATION
# COLUMN COUNT: 148
# Original node name TXF_CONTRACT_MASTER, link CONTRACT_MASTER

CONTRACT_MASTER = CONTRACT_MASTER_IN.select(
    (lit(APPL_CD) + lit('|') + CONTRACT_MASTER_IN.contract_id).alias('MST_ACCT_NATL_KEY'),
    (lit(APPL_CD) + lit('|') + CONTRACT_MASTER_IN.product_code + lit('|') +
     substring(CONTRACT_MASTER_IN.contract_type_code, lit(1), lit(3))).alias('PROD_DTL_NATL_KEY'),
    (lit(APPL_CD)).alias('APPL_CD_LX'),
    (when((trim(CONTRACT_MASTER_IN.is_terminated) == lit('True')),
     (lit('C'))).otherwise(lit('O'))).alias('OPEN_CLOSE_CD'),
    (lit('LX')).alias('APPLICATIONS'),
    (lit('103')).alias('IN_2'),
    CONTRACT_MASTER_IN.months_renewal_maturity_date.alias(
        'months_renewal_maturity_date'),
    CONTRACT_MASTER_IN.acceptance_date.alias('acceptance_date'),
    CONTRACT_MASTER_IN.account_dist_code.alias('account_dist_code'),
    CONTRACT_MASTER_IN.account_dist_code_description.alias(
        'account_dist_code_description'),
    CONTRACT_MASTER_IN.bill_to_address_1.alias('bill_to_address_1'),
    CONTRACT_MASTER_IN.bill_to_address_2.alias('bill_to_address_2'),
    CONTRACT_MASTER_IN.bill_to_address_3.alias('bill_to_address_3'),
    CONTRACT_MASTER_IN.bill_to_attention.alias('bill_to_attention'),
    CONTRACT_MASTER_IN.bill_to_city.alias('bill_to_city'),
    CONTRACT_MASTER_IN.bill_to_country.alias('bill_to_country'),
    CONTRACT_MASTER_IN.bill_to_country_code.alias('bill_to_country_code'),
    CONTRACT_MASTER_IN.bill_to_county.alias('bill_to_county'),
    CONTRACT_MASTER_IN.bill_to_email_address.alias('bill_to_email_address'),
    CONTRACT_MASTER_IN.bill_to_fax_number.alias('bill_to_fax_number'),
    CONTRACT_MASTER_IN.bill_to_invoice_code.alias('bill_to_invoice_code'),
    CONTRACT_MASTER_IN.bill_to_invoice_group_description.alias(
        'bill_to_invoice_group_description'),
    CONTRACT_MASTER_IN.bill_to_is_invoice_group.alias(
        'bill_to_is_invoice_group'),
    CONTRACT_MASTER_IN.bill_to_location_code.alias('bill_to_location_code'),
    CONTRACT_MASTER_IN.bill_to_location_oid.alias('bill_to_location_oid'),
    CONTRACT_MASTER_IN.bill_to_name.alias('bill_to_name'),
    CONTRACT_MASTER_IN.bill_to_phone_ext.alias('bill_to_phone_ext'),
    CONTRACT_MASTER_IN.bill_to_phone_number.alias('bill_to_phone_number'),
    CONTRACT_MASTER_IN.bill_to_postal_code.alias('bill_to_postal_code'),
    CONTRACT_MASTER_IN.bill_to_state.alias('bill_to_state'),
    CONTRACT_MASTER_IN.bill_to_state_code.alias('bill_to_state_code'),
    CONTRACT_MASTER_IN.book_treatment.alias('book_treatment'),
    CONTRACT_MASTER_IN.booked_post_date.alias('booked_post_date'),
    CONTRACT_MASTER_IN.chargeoff_effective_date.alias(
        'chargeoff_effective_date'),
    CONTRACT_MASTER_IN.commencement_date.alias('commencement_date'),
    CONTRACT_MASTER_IN.compounding_period.alias('compounding_period'),
    CONTRACT_MASTER_IN.compute_method.alias('compute_method'),
    CONTRACT_MASTER_IN.contract_id.alias('contract_id'),
    CONTRACT_MASTER_IN.contract_manager_id.alias('contract_manager_id'),
    CONTRACT_MASTER_IN.contract_manager_name.alias('contract_manager_name'),
    CONTRACT_MASTER_IN.contract_manager_oid.alias('contract_manager_oid'),
    CONTRACT_MASTER_IN.contract_oid.alias('contract_oid'),
    CONTRACT_MASTER_IN.contract_type_code.alias('contract_type_code'),
    CONTRACT_MASTER_IN.contract_type_description.alias(
        'contract_type_description'),
    CONTRACT_MASTER_IN.customer_alt_name.alias('customer_alt_name'),
    CONTRACT_MASTER_IN.customer_business_individual_indicator.alias(
        'customer_business_individual_indicator'),
    CONTRACT_MASTER_IN.customer_dba.alias('customer_dba'),
    CONTRACT_MASTER_IN.customer_first_name.alias('customer_first_name'),
    CONTRACT_MASTER_IN.customer_id.alias('customer_id'),
    CONTRACT_MASTER_IN.customer_last_name.alias('customer_last_name'),
    CONTRACT_MASTER_IN.customer_middle_name.alias('customer_middle_name'),
    CONTRACT_MASTER_IN.customer_name.alias('customer_name'),
    CONTRACT_MASTER_IN.customer_oid.alias('customer_oid'),
    CONTRACT_MASTER_IN.customer_salutaion.alias('customer_salutaion'),
    CONTRACT_MASTER_IN.delinquency_code_code.alias('delinquency_code_code'),
    CONTRACT_MASTER_IN.delinquency_code_description.alias(
        'delinquency_code_description'),
    CONTRACT_MASTER_IN.delinquency_code_oid.alias('delinquency_code_oid'),
    CONTRACT_MASTER_IN.finance_company_name.alias('finance_company_name'),
    CONTRACT_MASTER_IN.finance_company_oid.alias('finance_company_oid'),
    CONTRACT_MASTER_IN.funding_date.alias('funding_date'),
    CONTRACT_MASTER_IN.funding_reference.alias('funding_reference'),
    CONTRACT_MASTER_IN.funding_source_id.alias('funding_source_id'),
    CONTRACT_MASTER_IN.funding_source_name.alias('funding_source_name'),
    CONTRACT_MASTER_IN.funding_source_oid.alias('funding_source_oid'),
    CONTRACT_MASTER_IN.implicit_rate.alias('implicit_rate'),
    CONTRACT_MASTER_IN.interest_calculation.alias('interest_calculation'),
    CONTRACT_MASTER_IN.invoice_code_code.alias('invoice_code_code'),
    CONTRACT_MASTER_IN.invoice_code_description.alias(
        'invoice_code_description'),
    CONTRACT_MASTER_IN.invoice_code_oid.alias('invoice_code_oid'),
    CONTRACT_MASTER_IN.is_booked.alias('is_booked'),
    CONTRACT_MASTER_IN.is_chargeoff.alias('is_chargeoff'),
    CONTRACT_MASTER_IN.is_invoicing_suspended.alias('is_invoicing_suspended'),
    CONTRACT_MASTER_IN.is_nonaccrual.alias('is_nonaccrual'),
    CONTRACT_MASTER_IN.is_same_as_cash.alias('is_same_as_cash'),
    CONTRACT_MASTER_IN.is_terminated.alias('is_terminated'),
    CONTRACT_MASTER_IN.is_variable_rate.alias('is_variable_rate'),
    CONTRACT_MASTER_IN.maturity_date.alias('maturity_date'),
    CONTRACT_MASTER_IN.nonaccrual_effective_date.alias(
        'nonaccrual_effective_date'),
    CONTRACT_MASTER_IN.payment_adjustment_method.alias(
        'payment_adjustment_method'),
    CONTRACT_MASTER_IN.payment_frequency.alias('payment_frequency'),
    CONTRACT_MASTER_IN.payments_in_advance.alias('payments_in_advance'),
    CONTRACT_MASTER_IN.primary_contact_name.alias('primary_contact_name'),
    CONTRACT_MASTER_IN.primary_contact_title.alias('primary_contact_title'),
    CONTRACT_MASTER_IN.product_code.alias('product_code'),
    CONTRACT_MASTER_IN.product_description.alias('product_description'),
    CONTRACT_MASTER_IN.product_oid.alias('product_oid'),
    CONTRACT_MASTER_IN.purchase_order_number.alias('purchase_order_number'),
    CONTRACT_MASTER_IN.rent_transaction_code_code.alias(
        'rent_transaction_code_code'),
    CONTRACT_MASTER_IN.rent_transaction_code_description.alias(
        'rent_transaction_code_description'),
    CONTRACT_MASTER_IN.rent_transaction_code_invoice_description.alias(
        'rent_transaction_code_invoice_description'),
    CONTRACT_MASTER_IN.rent_transaction_code_oid.alias(
        'rent_transaction_code_oid'),
    CONTRACT_MASTER_IN.sales_rep_address_1.alias('sales_rep_address_1'),
    CONTRACT_MASTER_IN.sales_rep_address_2.alias('sales_rep_address_2'),
    CONTRACT_MASTER_IN.sales_rep_address_3.alias('sales_rep_address_3'),
    CONTRACT_MASTER_IN.sales_rep_city.alias('sales_rep_city'),
    CONTRACT_MASTER_IN.sales_rep_county.alias('sales_rep_county'),
    CONTRACT_MASTER_IN.sales_rep_email_address.alias(
        'sales_rep_email_address'),
    CONTRACT_MASTER_IN.sales_rep_fax_ext.alias('sales_rep_fax_ext'),
    CONTRACT_MASTER_IN.sales_rep_fax_number.alias('sales_rep_fax_number'),
    CONTRACT_MASTER_IN.sales_rep_id.alias('sales_rep_id'),
    CONTRACT_MASTER_IN.sales_rep_name.alias('sales_rep_name'),
    CONTRACT_MASTER_IN.sales_rep_oid.alias('sales_rep_oid'),
    CONTRACT_MASTER_IN.sales_rep_phone_ext.alias('sales_rep_phone_ext'),
    CONTRACT_MASTER_IN.sales_rep_phone_number.alias('sales_rep_phone_number'),
    CONTRACT_MASTER_IN.sales_rep_postal_code.alias('sales_rep_postal_code'),
    CONTRACT_MASTER_IN.sales_rep_state_code.alias('sales_rep_state_code'),
    CONTRACT_MASTER_IN.same_as_cash_duration.alias('same_as_cash_duration'),
    CONTRACT_MASTER_IN.same_as_cash_end_date.alias('same_as_cash_end_date'),
    CONTRACT_MASTER_IN.same_as_cash_interval.alias('same_as_cash_interval'),
    CONTRACT_MASTER_IN.sign_date.alias('sign_date'),
    CONTRACT_MASTER_IN.start_date.alias('start_date'),
    CONTRACT_MASTER_IN.suffix.alias('suffix'),
    CONTRACT_MASTER_IN.tax_treatment.alias('tax_treatment'),
    CONTRACT_MASTER_IN.term.alias('term'),
    CONTRACT_MASTER_IN.termination_date.alias('termination_date'),
    CONTRACT_MASTER_IN.transaction_number.alias('transaction_number'),
    CONTRACT_MASTER_IN.vendor_manager_id.alias('vendor_manager_id'),
    CONTRACT_MASTER_IN.vendor_manager_name.alias('vendor_manager_name'),
    CONTRACT_MASTER_IN.vendor_manager_oid.alias('vendor_manager_oid'),
    CONTRACT_MASTER_IN.year_length.alias('year_length'),
    CONTRACT_MASTER_IN.yield_rate.alias('yield_rate'),
    CONTRACT_MASTER_IN.industry_type.alias('industry_type'),
    CONTRACT_MASTER_IN.customer_oid_from_customer.alias(
        'customer_oid_from_customer'),
    CONTRACT_MASTER_IN.value_zr.alias('value_zr'),
    CONTRACT_MASTER_IN.oid_from_chrg_dwn_amt.alias('oid_from_chrg_dwn_amt'),
    CONTRACT_MASTER_IN.contract_oid_from_eff_rate_chg.alias(
        'contract_oid_from_eff_rate_chg'),
    CONTRACT_MASTER_IN.benchmark_description.alias('benchmark_description'),
    CONTRACT_MASTER_IN.benchmark_rate.alias('benchmark_rate'),
    CONTRACT_MASTER_IN.plus_factor.alias('plus_factor'),
    CONTRACT_MASTER_IN.contract_oid_from_contract_aging.alias(
        'contract_oid_from_contract_aging'),
    CONTRACT_MASTER_IN.oldest_rent_due_date.alias('oldest_rent_due_date'),
    CONTRACT_MASTER_IN.next_payment_due_date.alias('next_payment_due_date'),
    CONTRACT_MASTER_IN.contract_oid_from_contract_general_ledger.alias(
        'contract_oid_from_contract_general_ledger'),
    CONTRACT_MASTER_IN.original_lease_receivable_balance.alias(
        'original_lease_receivable_balance'),
    CONTRACT_MASTER_IN.original_residual_receivable.alias(
        'original_residual_receivable'),
    CONTRACT_MASTER_IN.contract_oid_from_contract_equipment_summary.alias(
        'contract_oid_from_contract_equipment_summary'),
    CONTRACT_MASTER_IN.equipment_cost.alias('equipment_cost'),
    CONTRACT_MASTER_IN.contract_oid_from_rpt_renewal_status.alias(
        'contract_oid_from_rpt_renewal_status'),
    CONTRACT_MASTER_IN.is_current_renewal.alias('is_current_renewal'),
    CONTRACT_MASTER_IN.renewal_maturity_date.alias('renewal_maturity_date'),
    CONTRACT_MASTER_IN.renewal_start_date.alias('renewal_start_date'),
    CONTRACT_MASTER_IN.finance_program_oid.alias('finance_program_oid'),
    CONTRACT_MASTER_IN.customer_ssn_tax_id_number.alias(
        'customer_ssn_tax_id_number'),
    CONTRACT_MASTER_IN.entity_oid.alias('entity_oid'),
    CONTRACT_MASTER_IN.account_industry_type.alias('account_industry_type'),
    CONTRACT_MASTER_IN.benchmark_id.alias('benchmark_id'),
    CONTRACT_MASTER_IN.udfcontract_year_of_libor_used.alias(
        'udfcontract_year_of_libor_used'),
    (CONTRACT_MASTER_IN.contract_oid .cast(int)).alias('oid')
)

# Processing node ODATE_OUT, type TRANSFORMATION
# COLUMN COUNT: 20
# Original node name XFM_ODATE, link ODATE_OUT

ODATE_OUT = earnings_schedule_IN.select(
    earnings_schedule_IN.adjusted_amount.alias('adjusted_amount'),
    earnings_schedule_IN.contract_item_oid.alias('contract_item_oid'),
    earnings_schedule_IN.contract_oid.alias('contract_oid'),
    earnings_schedule_IN.custom.alias('custom'),
    earnings_schedule_IN.date_processed.alias('date_processed'),
    earnings_schedule_IN.debt_note_oid.alias('debt_note_oid'),
    earnings_schedule_IN.earnings.alias('earnings'),
    earnings_schedule_IN.earnings_date.alias('earnings_date'),
    earnings_schedule_IN.earnings_schedule_oid.alias('earnings_schedule_oid'),
    earnings_schedule_IN.earnings_source_oid.alias('earnings_source_oid'),
    earnings_schedule_IN.earnings_type_oid.alias('earnings_type_oid'),
    earnings_schedule_IN.is_charge_off.alias('is_charge_off'),
    earnings_schedule_IN.is_non_accrual.alias('is_non_accrual'),
    earnings_schedule_IN.is_pending_update.alias('is_pending_update'),
    earnings_schedule_IN.is_processed.alias('is_processed'),
    earnings_schedule_IN.is_same_as_cash.alias('is_same_as_cash'),
    earnings_schedule_IN.last_change_date_time.alias('last_change_date_time'),
    earnings_schedule_IN.last_change_operator.alias('last_change_operator'),
    earnings_schedule_IN.update_count.alias('update_count'),
    (lit(DATA_DATE) .cast(date)).alias('DATA_DATE')
)

# Processing node ASPIRE_MISC_CUR_AND_BANK_SHR_BAL_IN, type SOURCE
# COLUMN COUNT: 7
# Original node name ASPIRE_MISC_CUR_AND_BANK_SHR_BAL_DS, link ASPIRE_MISC_CUR_AND_BANK_SHR_BAL_IN

ASPIRE_MISC_CUR_AND_BANK_SHR_BAL_IN = spark.read.csv(
    ""+DS_DIR+"/aspire/acct/aspire_cur_and_bank_shr_bal.ds", sep=',', header='false')

# Processing node PMT_SCHED_IN, type SOURCE
# COLUMN COUNT: 3
# Original node name aspire_dbo_doc_gen_con_payment_ds, link PMT_SCHED_IN

PMT_SCHED_IN = spark.read.csv(
    ""+DS_DIR+"/aspire/aspire_dbo_doc_gen_con_payment_schedule.ds", sep=',', header='false')

# Processing node aspire_dbo_contract_values_IN, type SOURCE
# COLUMN COUNT: 12
# Original node name DS_aspire_dbo_contract_values, link aspire_dbo_contract_values_IN

aspire_dbo_contract_values_IN = spark.read.csv(
    ""+DS_DIR+"/aspire/aspire_dbo_contract_values.ds", sep=',', header='false')

# Processing node ASPIRE_GEN_CON_PRIN_AND_INT_TOTALS_IN, type SOURCE
# COLUMN COUNT: 8
# Original node name ASPIRE_GEN_CON_PRIN_AND_INT_TOTALS_DS, link ASPIRE_GEN_CON_PRIN_AND_INT_TOTALS_IN

ASPIRE_GEN_CON_PRIN_AND_INT_TOTALS_IN = spark.read.csv(
    ""+DS_DIR+"/aspire/aspire_dbo_doc_gen_con_prin_and_int_totals.ds", sep=',', header='false')

# Processing node earnings_schedule_IN_CONV2, type MERGE
# COLUMN COUNT: 20
# Original node name LKP_ODATE, link earnings_schedule_IN_CONV2

earnings_schedule_IN_CONV2 = ODATE_OUT.join(ODATE, [ODATE.DATA_DATE == ODATE_OUT.DATA_DATE], 'LEFT_OUTER').select(
    ODATE_OUT.adjusted_amount.alias('adjusted_amount'),
    ODATE_OUT.contract_item_oid.alias('contract_item_oid'),
    ODATE_OUT.contract_oid.alias('contract_oid'),
    ODATE_OUT.custom.alias('custom'),
    ODATE_OUT.date_processed.alias('date_processed'),
    ODATE_OUT.debt_note_oid.alias('debt_note_oid'),
    ODATE_OUT.earnings.alias('earnings'),
    ODATE_OUT.earnings_date.alias('earnings_date'),
    ODATE_OUT.earnings_schedule_oid.alias('earnings_schedule_oid'),
    ODATE_OUT.earnings_source_oid.alias('earnings_source_oid'),
    ODATE_OUT.earnings_type_oid.alias('earnings_type_oid'),
    ODATE_OUT.is_charge_off.alias('is_charge_off'),
    ODATE_OUT.is_non_accrual.alias('is_non_accrual'),
    ODATE_OUT.is_pending_update.alias('is_pending_update'),
    ODATE_OUT.is_processed.alias('is_processed'),
    ODATE_OUT.is_same_as_cash.alias('is_same_as_cash'),
    ODATE_OUT.last_change_date_time.alias('last_change_date_time'),
    ODATE_OUT.last_change_operator.alias('last_change_operator'),
    ODATE_OUT.update_count.alias('update_count'),
    ODATE.ODATE.alias('ODATE')
)

# Processing node ASPIRE_ADHOCREPORT_CONTRACT_AGING_IN, type TRANSFORMATION
# COLUMN COUNT: 18
# Original node name Transformer_318, link ASPIRE_ADHOCREPORT_CONTRACT_AGING_IN

ASPIRE_ADHOCREPORT_CONTRACT_AGING_IN = CONTRACT_AGING_DS.select(
    CONTRACT_AGING_DS.contract_id.alias('contract_id'),
    CONTRACT_AGING_DS.late_fee_due_period_2.alias('late_fee_due_period_2'),
    CONTRACT_AGING_DS.late_fee_due_period_3.alias('late_fee_due_period_3'),
    CONTRACT_AGING_DS.late_fee_due_period_4.alias('late_fee_due_period_4'),
    CONTRACT_AGING_DS.late_fee_due_period_5.alias('late_fee_due_period_5'),
    CONTRACT_AGING_DS.late_fee_due_period_6.alias('late_fee_due_period_6'),
    CONTRACT_AGING_DS.late_fee_due_period_7.alias('late_fee_due_period_7'),
    CONTRACT_AGING_DS.oldest_rent_due_date.alias('oldest_rent_due_date'),
    CONTRACT_AGING_DS.rent_due_period_1.alias('rent_due_period_1'),
    CONTRACT_AGING_DS.rent_due_period_10.alias('rent_due_period_10'),
    CONTRACT_AGING_DS.rent_due_period_2.alias('rent_due_period_2'),
    CONTRACT_AGING_DS.rent_due_period_3.alias('rent_due_period_3'),
    CONTRACT_AGING_DS.rent_due_period_4.alias('rent_due_period_4'),
    CONTRACT_AGING_DS.rent_due_period_5.alias('rent_due_period_5'),
    CONTRACT_AGING_DS.rent_due_period_6.alias('rent_due_period_6'),
    CONTRACT_AGING_DS.rent_due_period_7.alias('rent_due_period_7'),
    CONTRACT_AGING_DS.rent_due_period_8.alias('rent_due_period_8'),
    CONTRACT_AGING_DS.rent_due_period_9.alias('rent_due_period_9')
)

# Processing node Earnings_IN, type TRANSFORMATION
# COLUMN COUNT: 2
# Original node name XFM_EARNINGS, link Earnings_IN

Earnings_IN = earnings_schedule_IN_CONV2
Earnings_IN = Earnings_IN.withColumn("ODate", substring(earnings_schedule_IN.ODATE, lit(1), lit(4)) + substring(earnings_schedule_IN.ODATE, lit(6), lit(2)) + substring(earnings_schedule_IN.ODATE, lit(9), lit(2))).select(
    earnings_schedule_IN.contract_oid.alias('contract_oid'),
    (earnings_schedule_IN.earnings .cast(numeric)).alias('earnings')
).filter("Trim(earnings_schedule_IN.earnings_date) <= ODate And Trim(earnings_schedule_IN.earnings_type_oid) = '11100'")

# Processing node earnings_schedule_OUT, type TRANSFORMATION
# COLUMN COUNT: 19
# Original node name XFM_EARNINGS, link earnings_schedule_OUT

earnings_schedule_OUT = earnings_schedule_IN_CONV2
earnings_schedule_OUT = earnings_schedule_OUT.withColumn("ODate", substring(earnings_schedule_IN.ODATE, lit(1), lit(4)) + substring(earnings_schedule_IN.ODATE, lit(6), lit(2)) + substring(earnings_schedule_IN.ODATE, lit(9), lit(2))).select(
    earnings_schedule_IN.adjusted_amount.alias('adjusted_amount'),
    earnings_schedule_IN.contract_item_oid.alias('contract_item_oid'),
    earnings_schedule_IN.contract_oid.alias('contract_oid'),
    earnings_schedule_IN.custom.alias('custom'),
    earnings_schedule_IN.date_processed.alias('date_processed'),
    earnings_schedule_IN.debt_note_oid.alias('debt_note_oid'),
    earnings_schedule_IN.earnings.alias('earnings'),
    earnings_schedule_IN.earnings_date.alias('earnings_date'),
    earnings_schedule_IN.earnings_schedule_oid.alias('earnings_schedule_oid'),
    earnings_schedule_IN.earnings_source_oid.alias('earnings_source_oid'),
    earnings_schedule_IN.earnings_type_oid.alias('earnings_type_oid'),
    earnings_schedule_IN.is_charge_off.alias('is_charge_off'),
    earnings_schedule_IN.is_non_accrual.alias('is_non_accrual'),
    earnings_schedule_IN.is_pending_update.alias('is_pending_update'),
    earnings_schedule_IN.is_processed.alias('is_processed'),
    earnings_schedule_IN.is_same_as_cash.alias('is_same_as_cash'),
    earnings_schedule_IN.last_change_date_time.alias('last_change_date_time'),
    earnings_schedule_IN.last_change_operator.alias('last_change_operator'),
    earnings_schedule_IN.update_count.alias('update_count')
)

# Processing node Earnings_SUM_IN, type AGGREGATOR
# COLUMN COUNT: 2
# Original node name Aggregator_EARNINGS, link Earnings_SUM_IN

Earnings_SUM_IN = Earnings_IN.groupBy("contract_oid").agg(
    sum("earnings").alias("SUM_EARNINGS")).select(
    Earnings_IN.contract_oid.alias('contract_oid'),
    'SUM_EARNINGS'
)

# Processing node LKP_MASTER_OUT1, type MERGE
# COLUMN COUNT: 206
# Original node name LKP_01, link LKP_MASTER_OUT1

LKP_MASTER_OUT1 = CONTRACT_MASTER.join(LKP_NATL_ACCT, [LKP_NATL_ACCT.NATL_KEY == CONTRACT_MASTER.MST_ACCT_NATL_KEY], 'LEFT_OUTER').join(CONTRACT_GENERAL_LEDGER_IN, [CONTRACT_GENERAL_LEDGER_IN.contract_oid == CONTRACT_MASTER.contract_oid], 'LEFT_OUTER').join(earnings_schedule_OUT, [earnings_schedule_OUT.contract_oid == CONTRACT_MASTER.contract_oid], 'LEFT_OUTER').join(bookvalue_booked_contract_component_IN, [bookvalue_booked_contract_component_IN.contract_oid == CONTRACT_MASTER.contract_oid], 'LEFT_OUTER').join(aspire_dbo_doc_gen_con_idc_zcc_idc_IN, [aspire_dbo_doc_gen_con_idc_zcc_idc_IN.contract_oid == CONTRACT_MASTER.contract_oid], 'LEFT_OUTER').join(aspire_dbo_lti_rpt_renewal_status_IN, [aspire_dbo_lti_rpt_renewal_status_IN.contract_oid == CONTRACT_MASTER.contract_oid], 'LEFT_OUTER').join(aspire_dbo_doc_gen_con_contract_IN, [aspire_dbo_doc_gen_con_contract_IN.contract_id == CONTRACT_MASTER.contract_id], 'LEFT_OUTER').join(aspire_adhocreport_contract_payment_summary_IN, [aspire_adhocreport_contract_payment_summary_IN.contract_oid == CONTRACT_MASTER.contract_oid], 'LEFT_OUTER').join(ASPIRE_DBO_LTI_VALUES_IN, [ASPIRE_DBO_LTI_VALUES_IN.oid == CONTRACT_MASTER.oid], 'LEFT_OUTER').join(ASPIRE_GEN_CON_PRIN_AND_INT_TOTALS_IN, [ASPIRE_GEN_CON_PRIN_AND_INT_TOTALS_IN.contract_oid == CONTRACT_MASTER.contract_oid], 'LEFT_OUTER').join(spire_adhocreport_contract_equipment_summary_IN, [spire_adhocreport_contract_equipment_summary_IN.contract_oid == CONTRACT_MASTER.contract_oid], 'LEFT_OUTER').join(aspire_dbo_doc_gen_con_contract_udf_extensions_IN, [aspire_dbo_doc_gen_con_contract_udf_extensions_IN.contract_oid == CONTRACT_MASTER.contract_oid], 'LEFT_OUTER').join(aspire_dbo_contract_values_IN, [aspire_dbo_contract_values_IN.contract_oid == CONTRACT_MASTER.contract_oid], 'LEFT_OUTER').join(ASPIRE_MISC_CUR_AND_BANK_SHR_BAL_IN, [ASPIRE_MISC_CUR_AND_BANK_SHR_BAL_IN.MST_ACCT_NATL_KEY == CONTRACT_MASTER.MST_ACCT_NATL_KEY], 'LEFT_OUTER').join(PMT_SCHED_IN, [PMT_SCHED_IN.contract_oid == CONTRACT_MASTER.contract_oid], 'LEFT_OUTER').join(Earnings_SUM_IN, [Earnings_SUM_IN.contract_oid == CONTRACT_MASTER.contract_oid], 'LEFT_OUTER').join(ASPIRE_ADHOCREPORT_CONTRACT_AGING_IN, [ASPIRE_ADHOCREPORT_CONTRACT_AGING_IN.contract_id == CONTRACT_MASTER.contract_id], 'LEFT_OUTER').select(
    CONTRACT_MASTER.MST_ACCT_NATL_KEY.alias('MST_ACCT_NATL_KEY'),
    CONTRACT_MASTER.PROD_DTL_NATL_KEY.alias('PROD_DTL_NATL_KEY'),
    CONTRACT_MASTER.APPL_CD_LX.alias('APPL_CD_LX'),
    LKP_NATL_ACCT.ACCT_ID.alias('ACCT_ID'),
    CONTRACT_MASTER.OPEN_CLOSE_CD.alias('OPEN_CLOSE_CD'),
    CONTRACT_MASTER.APPLICATIONS.alias('APPLICATIONS'),
    CONTRACT_MASTER.IN_2.alias('IN_2'),
    CONTRACT_MASTER.months_renewal_maturity_date.alias(
        'months_renewal_maturity_date'),
    CONTRACT_MASTER.acceptance_date.alias('acceptance_date'),
    CONTRACT_MASTER.account_dist_code.alias('account_dist_code'),
    CONTRACT_MASTER.account_dist_code_description.alias(
        'account_dist_code_description'),
    CONTRACT_MASTER.bill_to_address_1.alias('bill_to_address_1'),
    CONTRACT_MASTER.bill_to_address_2.alias('bill_to_address_2'),
    CONTRACT_MASTER.bill_to_address_3.alias('bill_to_address_3'),
    CONTRACT_MASTER.bill_to_attention.alias('bill_to_attention'),
    CONTRACT_MASTER.bill_to_city.alias('bill_to_city'),
    CONTRACT_MASTER.bill_to_country.alias('bill_to_country'),
    CONTRACT_MASTER.bill_to_country_code.alias('bill_to_country_code'),
    CONTRACT_MASTER.bill_to_county.alias('bill_to_county'),
    CONTRACT_MASTER.bill_to_email_address.alias('bill_to_email_address'),
    CONTRACT_MASTER.bill_to_fax_number.alias('bill_to_fax_number'),
    CONTRACT_MASTER.bill_to_invoice_code.alias('bill_to_invoice_code'),
    CONTRACT_MASTER.bill_to_invoice_group_description.alias(
        'bill_to_invoice_group_description'),
    CONTRACT_MASTER.bill_to_is_invoice_group.alias('bill_to_is_invoice_group'),
    CONTRACT_MASTER.bill_to_location_code.alias('bill_to_location_code'),
    CONTRACT_MASTER.bill_to_location_oid.alias('bill_to_location_oid'),
    CONTRACT_MASTER.bill_to_name.alias('bill_to_name'),
    CONTRACT_MASTER.bill_to_phone_ext.alias('bill_to_phone_ext'),
    CONTRACT_MASTER.bill_to_phone_number.alias('bill_to_phone_number'),
    CONTRACT_MASTER.bill_to_postal_code.alias('bill_to_postal_code'),
    CONTRACT_MASTER.bill_to_state.alias('bill_to_state'),
    CONTRACT_MASTER.bill_to_state_code.alias('bill_to_state_code'),
    CONTRACT_MASTER.book_treatment.alias('book_treatment'),
    CONTRACT_MASTER.booked_post_date.alias('booked_post_date'),
    CONTRACT_MASTER.chargeoff_effective_date.alias('chargeoff_effective_date'),
    CONTRACT_MASTER.commencement_date.alias('commencement_date'),
    CONTRACT_MASTER.compounding_period.alias('compounding_period'),
    CONTRACT_MASTER.compute_method.alias('compute_method'),
    CONTRACT_MASTER.contract_id.alias('contract_id'),
    CONTRACT_MASTER.contract_manager_id.alias('contract_manager_id'),
    CONTRACT_MASTER.contract_manager_name.alias('contract_manager_name'),
    CONTRACT_MASTER.contract_manager_oid.alias('contract_manager_oid'),
    CONTRACT_MASTER.contract_oid.alias('contract_oid'),
    CONTRACT_MASTER.contract_type_code.alias('contract_type_code'),
    CONTRACT_MASTER.contract_type_description.alias(
        'contract_type_description'),
    CONTRACT_MASTER.customer_alt_name.alias('customer_alt_name'),
    CONTRACT_MASTER.customer_dba.alias('customer_dba'),
    CONTRACT_MASTER.customer_first_name.alias('customer_first_name'),
    CONTRACT_MASTER.customer_id.alias('customer_id'),
    CONTRACT_MASTER.customer_last_name.alias('customer_last_name'),
    CONTRACT_MASTER.customer_middle_name.alias('customer_middle_name'),
    CONTRACT_MASTER.customer_name.alias('customer_name'),
    CONTRACT_MASTER.customer_oid.alias('customer_oid'),
    CONTRACT_MASTER.customer_salutaion.alias('customer_salutaion'),
    CONTRACT_MASTER.delinquency_code_code.alias('delinquency_code_code'),
    CONTRACT_MASTER.delinquency_code_description.alias(
        'delinquency_code_description'),
    CONTRACT_MASTER.delinquency_code_oid.alias('delinquency_code_oid'),
    CONTRACT_MASTER.finance_company_name.alias('finance_company_name'),
    CONTRACT_MASTER.finance_company_oid.alias('finance_company_oid'),
    CONTRACT_MASTER.funding_date.alias('funding_date'),
    CONTRACT_MASTER.funding_reference.alias('funding_reference'),
    CONTRACT_MASTER.funding_source_id.alias('funding_source_id'),
    CONTRACT_MASTER.funding_source_name.alias('funding_source_name'),
    CONTRACT_MASTER.funding_source_oid.alias('funding_source_oid'),
    CONTRACT_MASTER.implicit_rate.alias('implicit_rate'),
    CONTRACT_MASTER.interest_calculation.alias('interest_calculation'),
    CONTRACT_MASTER.invoice_code_code.alias('invoice_code_code'),
    CONTRACT_MASTER.invoice_code_description.alias('invoice_code_description'),
    CONTRACT_MASTER.invoice_code_oid.alias('invoice_code_oid'),
    CONTRACT_MASTER.is_booked.alias('is_booked'),
    CONTRACT_MASTER.is_chargeoff.alias('is_chargeoff'),
    CONTRACT_MASTER.is_invoicing_suspended.alias('is_invoicing_suspended'),
    CONTRACT_MASTER.is_nonaccrual.alias('is_nonaccrual'),
    CONTRACT_MASTER.is_same_as_cash.alias('is_same_as_cash'),
    CONTRACT_MASTER.is_terminated.alias('is_terminated'),
    CONTRACT_MASTER.is_variable_rate.alias('is_variable_rate'),
    CONTRACT_MASTER.maturity_date.alias('maturity_date'),
    CONTRACT_MASTER.nonaccrual_effective_date.alias(
        'nonaccrual_effective_date'),
    CONTRACT_MASTER.payment_adjustment_method.alias(
        'payment_adjustment_method'),
    CONTRACT_MASTER.payment_frequency.alias('payment_frequency'),
    CONTRACT_MASTER.payments_in_advance.alias('payments_in_advance'),
    CONTRACT_MASTER.primary_contact_name.alias('primary_contact_name'),
    CONTRACT_MASTER.primary_contact_title.alias('primary_contact_title'),
    CONTRACT_MASTER.product_code.alias('product_code'),
    CONTRACT_MASTER.product_description.alias('product_description'),
    CONTRACT_MASTER.product_oid.alias('product_oid'),
    CONTRACT_MASTER.purchase_order_number.alias('purchase_order_number'),
    CONTRACT_MASTER.rent_transaction_code_code.alias(
        'rent_transaction_code_code'),
    CONTRACT_MASTER.rent_transaction_code_description.alias(
        'rent_transaction_code_description'),
    CONTRACT_MASTER.rent_transaction_code_invoice_description.alias(
        'rent_transaction_code_invoice_description'),
    CONTRACT_MASTER.rent_transaction_code_oid.alias(
        'rent_transaction_code_oid'),
    CONTRACT_MASTER.sales_rep_address_1.alias('sales_rep_address_1'),
    CONTRACT_MASTER.sales_rep_address_2.alias('sales_rep_address_2'),
    CONTRACT_MASTER.sales_rep_address_3.alias('sales_rep_address_3'),
    CONTRACT_MASTER.sales_rep_city.alias('sales_rep_city'),
    CONTRACT_MASTER.sales_rep_county.alias('sales_rep_county'),
    CONTRACT_MASTER.sales_rep_email_address.alias('sales_rep_email_address'),
    CONTRACT_MASTER.sales_rep_fax_ext.alias('sales_rep_fax_ext'),
    CONTRACT_MASTER.sales_rep_fax_number.alias('sales_rep_fax_number'),
    CONTRACT_MASTER.sales_rep_id.alias('sales_rep_id'),
    CONTRACT_MASTER.sales_rep_name.alias('sales_rep_name'),
    CONTRACT_MASTER.sales_rep_oid.alias('sales_rep_oid'),
    CONTRACT_MASTER.sales_rep_phone_ext.alias('sales_rep_phone_ext'),
    CONTRACT_MASTER.sales_rep_phone_number.alias('sales_rep_phone_number'),
    CONTRACT_MASTER.sales_rep_postal_code.alias('sales_rep_postal_code'),
    CONTRACT_MASTER.sales_rep_state_code.alias('sales_rep_state_code'),
    CONTRACT_MASTER.same_as_cash_duration.alias('same_as_cash_duration'),
    CONTRACT_MASTER.same_as_cash_end_date.alias('same_as_cash_end_date'),
    CONTRACT_MASTER.same_as_cash_interval.alias('same_as_cash_interval'),
    CONTRACT_MASTER.sign_date.alias('sign_date'),
    CONTRACT_MASTER.start_date.alias('start_date'),
    CONTRACT_MASTER.suffix.alias('suffix'),
    CONTRACT_MASTER.tax_treatment.alias('tax_treatment'),
    CONTRACT_MASTER.term.alias('term'),
    CONTRACT_MASTER.termination_date.alias('termination_date'),
    CONTRACT_MASTER.transaction_number.alias('transaction_number'),
    CONTRACT_MASTER.vendor_manager_id.alias('vendor_manager_id'),
    CONTRACT_MASTER.vendor_manager_name.alias('vendor_manager_name'),
    CONTRACT_MASTER.vendor_manager_oid.alias('vendor_manager_oid'),
    CONTRACT_MASTER.year_length.alias('year_length'),
    CONTRACT_MASTER.yield_rate.alias('yield_rate'),
    CONTRACT_MASTER.customer_oid_from_customer.alias(
        'customer_oid_from_customer'),
    CONTRACT_MASTER.value_zr.alias('value_zr'),
    CONTRACT_MASTER.oid_from_chrg_dwn_amt.alias('oid_from_chrg_dwn_amt'),
    CONTRACT_MASTER.contract_oid_from_eff_rate_chg.alias(
        'contract_oid_from_eff_rate_chg'),
    CONTRACT_MASTER.benchmark_description.alias('benchmark_description'),
    CONTRACT_MASTER.benchmark_rate.alias('benchmark_rate'),
    CONTRACT_MASTER.plus_factor.alias('plus_factor'),
    CONTRACT_MASTER.contract_oid_from_contract_aging.alias(
        'contract_oid_from_contract_aging'),
    CONTRACT_MASTER.oldest_rent_due_date.alias('oldest_rent_due_date'),
    CONTRACT_MASTER.next_payment_due_date.alias('next_payment_due_date'),
    CONTRACT_MASTER.contract_oid_from_contract_general_ledger.alias(
        'contract_oid_from_contract_general_ledger'),
    CONTRACT_MASTER.original_lease_receivable_balance.alias(
        'original_lease_receivable_balance'),
    CONTRACT_MASTER.original_residual_receivable.alias(
        'original_residual_receivable'),
    CONTRACT_MASTER.contract_oid_from_contract_equipment_summary.alias(
        'contract_oid_from_contract_equipment_summary'),
    CONTRACT_MASTER.equipment_cost.alias('equipment_cost'),
    CONTRACT_MASTER.contract_oid_from_rpt_renewal_status.alias(
        'contract_oid_from_rpt_renewal_status'),
    CONTRACT_MASTER.is_current_renewal.alias('is_current_renewal'),
    CONTRACT_MASTER.renewal_maturity_date.alias('renewal_maturity_date'),
    CONTRACT_MASTER.renewal_start_date.alias('renewal_start_date'),
    CONTRACT_MASTER.finance_program_oid.alias('finance_program_oid'),
    CONTRACT_MASTER.customer_ssn_tax_id_number.alias(
        'customer_ssn_tax_id_number'),
    CONTRACT_MASTER.customer_business_individual_indicator.alias(
        'customer_business_individual_indicator'),
    CONTRACT_MASTER.industry_type.alias('industry_type'),
    CONTRACT_MASTER.entity_oid.alias('entity_oid'),
    CONTRACT_MASTER.account_industry_type.alias('account_industry_type'),
    CONTRACT_MASTER.benchmark_id.alias('benchmark_id'),
    CONTRACT_MASTER.udfcontract_year_of_libor_used.alias(
        'udfcontract_year_of_libor_used'),
    CONTRACT_GENERAL_LEDGER_IN.last_month_end_date.alias(
        'last_month_end_date'),
    CONTRACT_GENERAL_LEDGER_IN.deferred_income_lease.alias(
        'deferred_income_lease'),
    CONTRACT_GENERAL_LEDGER_IN.deferred_income_residual.alias(
        'deferred_income_residual'),
    earnings_schedule_OUT.earnings_type_oid.alias('earnings_type_oid'),
    earnings_schedule_OUT.earnings.alias('earnings'),
    bookvalue_booked_contract_component_IN.accumulated_idc.alias(
        'accumulated_idc'),
    bookvalue_booked_contract_component_IN.accumulated_fee.alias(
        'accumulated_fee'),
    bookvalue_booked_contract_component_IN.idc.alias('idc'),
    aspire_dbo_doc_gen_con_idc_zcc_idc_IN.amount.alias('amount'),
    aspire_dbo_doc_gen_con_idc_zcc_idc_IN.description.alias('description'),
    aspire_dbo_lti_rpt_renewal_status_IN.renewal_maturity_date.alias(
        'renewal_maturity_date_lti_rpt'),
    aspire_dbo_doc_gen_con_contract_IN.last_payment_amount.alias(
        'last_payment_amount'),
    aspire_adhocreport_contract_payment_summary_IN.last_rental_paid_date.alias(
        'last_rental_paid_date'),
    ASPIRE_DBO_LTI_VALUES_IN.datavalue.alias('datavalue'),
    ASPIRE_GEN_CON_PRIN_AND_INT_TOTALS_IN.interest_ltd.alias('interest_ltd'),
    spire_adhocreport_contract_equipment_summary_IN.equipment_cost.alias(
        'equipment_cost_equipment_summary'),
    aspire_dbo_doc_gen_con_contract_udf_extensions_IN.value_zr.alias(
        'value_zr_udf_extn_euipment'),
    earnings_schedule_OUT.earnings_date.alias('earnings_date'),
    aspire_dbo_contract_values_IN.next_payment_date.alias('next_payment_date'),
    CONTRACT_GENERAL_LEDGER_IN.ytd_accrued_interest.alias(
        'ytd_accrued_interest'),
    CONTRACT_GENERAL_LEDGER_IN.ytd_non_accrual_interest_income.alias(
        'ytd_non_accrual_interest_income'),
    aspire_adhocreport_contract_payment_summary_IN.current_payment_amount.alias(
        'current_payment_amount'),
    CONTRACT_GENERAL_LEDGER_IN.mtd_earned_income_lease.alias(
        'mtd_earned_income_lease'),
    CONTRACT_GENERAL_LEDGER_IN.mtd_earned_income_residual.alias(
        'mtd_earned_income_residual'),
    CONTRACT_GENERAL_LEDGER_IN.mtd_interest_income.alias(
        'mtd_interest_income'),
    ASPIRE_MISC_CUR_AND_BANK_SHR_BAL_IN.CUR_BAL.alias('CUR_BAL'),
    ASPIRE_MISC_CUR_AND_BANK_SHR_BAL_IN.BANK_SHR_BAL.alias('BANK_SHR_BAL'),
    ASPIRE_ADHOCREPORT_CONTRACT_AGING_IN.rent_due_period_1.alias(
        'rent_due_period_1'),
    ASPIRE_ADHOCREPORT_CONTRACT_AGING_IN.rent_due_period_10.alias(
        'rent_due_period_10'),
    ASPIRE_ADHOCREPORT_CONTRACT_AGING_IN.rent_due_period_2.alias(
        'rent_due_period_2'),
    ASPIRE_ADHOCREPORT_CONTRACT_AGING_IN.rent_due_period_3.alias(
        'rent_due_period_3'),
    ASPIRE_ADHOCREPORT_CONTRACT_AGING_IN.rent_due_period_4.alias(
        'rent_due_period_4'),
    ASPIRE_ADHOCREPORT_CONTRACT_AGING_IN.rent_due_period_5.alias(
        'rent_due_period_5'),
    ASPIRE_ADHOCREPORT_CONTRACT_AGING_IN.rent_due_period_6.alias(
        'rent_due_period_6'),
    ASPIRE_ADHOCREPORT_CONTRACT_AGING_IN.rent_due_period_7.alias(
        'rent_due_period_7'),
    ASPIRE_ADHOCREPORT_CONTRACT_AGING_IN.rent_due_period_8.alias(
        'rent_due_period_8'),
    ASPIRE_ADHOCREPORT_CONTRACT_AGING_IN.rent_due_period_9.alias(
        'rent_due_period_9'),
    ASPIRE_ADHOCREPORT_CONTRACT_AGING_IN.oldest_rent_due_date.alias(
        'oldest_rent_due_date_contract_aging'),
    CONTRACT_GENERAL_LEDGER_IN.principal_balance.alias('principal_balance'),
    ASPIRE_MISC_CUR_AND_BANK_SHR_BAL_IN.DATA_DATE.alias('DATA_DATE'),
    PMT_SCHED_IN.amount.alias('amount_pmt_sched'),
    ASPIRE_ADHOCREPORT_CONTRACT_AGING_IN.late_fee_due_period_2.alias(
        'late_fee_due_period_2'),
    ASPIRE_ADHOCREPORT_CONTRACT_AGING_IN.late_fee_due_period_3.alias(
        'late_fee_due_period_3'),
    ASPIRE_ADHOCREPORT_CONTRACT_AGING_IN.late_fee_due_period_4.alias(
        'late_fee_due_period_4'),
    ASPIRE_ADHOCREPORT_CONTRACT_AGING_IN.late_fee_due_period_5.alias(
        'late_fee_due_period_5'),
    ASPIRE_ADHOCREPORT_CONTRACT_AGING_IN.late_fee_due_period_6.alias(
        'late_fee_due_period_6'),
    ASPIRE_ADHOCREPORT_CONTRACT_AGING_IN.late_fee_due_period_7.alias(
        'late_fee_due_period_7'),
    Earnings_SUM_IN.SUM_EARNINGS.alias('SUM_EARNINGS'),
    aspire_adhocreport_contract_payment_summary_IN.current_contract_receivable.alias(
        'current_contract_receivable'),
    CONTRACT_GENERAL_LEDGER_IN.ytd_interest_income.alias(
        'ytd_interest_income'),
    CONTRACT_GENERAL_LEDGER_IN.ytd_earned_income_lease.alias(
        'ytd_earned_income_lease'),
    CONTRACT_GENERAL_LEDGER_IN.ytd_earned_income_residual.alias(
        'ytd_earned_income_residual'),
    CONTRACT_GENERAL_LEDGER_IN.ltd_earned_income_lease.alias(
        'ltd_earned_income_lease'),
    CONTRACT_GENERAL_LEDGER_IN.ltd_earned_income_residual.alias(
        'ltd_earned_income_residual'),
    CONTRACT_GENERAL_LEDGER_IN.ltd_interest_income.alias(
        'ltd_interest_income'),
    ASPIRE_GEN_CON_PRIN_AND_INT_TOTALS_IN.interest_mtd.alias('interest_mtd'),
    ASPIRE_GEN_CON_PRIN_AND_INT_TOTALS_IN.interest_ytd.alias('interest_ytd'),
    aspire_dbo_doc_gen_con_contract_IN.extended_maturity_date.alias(
        'extended_maturity_date')
)

# Processing node LKP_MASTER_OUT21, type TRANSFORMATION
# COLUMN COUNT: 46
# Original node name XFM_GL, link LKP_MASTER_OUT21

LKP_MASTER_OUT21 = LKP_MASTER_OUT1
LKP_MASTER_OUT21 = LKP_MASTER_OUT21.withColumn("Matdtnew1", when(
    (LKP_MASTER_OUT1.extended_maturity_date == None), (LKP_MASTER_OUT1.maturity_date)).otherwise(LKP_MASTER_OUT1.extended_maturity_date))

LKP_MASTER_OUT21 = LKP_MASTER_OUT21.withColumn("EquipmentCost", (when(((LKP_MASTER_OUT1.equipment_cost_equipment_summary) IS NOT None), ((LKP_MASTER_OUT1.equipment_cost_equipment_summary))).otherwise(lit(0))))

LKP_MASTER_OUT21 = LKP_MASTER_OUT21.withColumn("RenMatDt", (when(((LKP_MASTER_OUT1.renewal_maturity_date_lti_rpt) IS NOT None), ((LKP_MASTER_OUT1.renewal_maturity_date_lti_rpt))).otherwise((lit('00010101')))))

LKP_MASTER_OUT21 = LKP_MASTER_OUT21.withColumn(
    "RenwMatDt", RenMatDt .cast(date))

LKP_MASTER_OUT21 = LKP_MASTER_OUT21.withColumn("AsOfDt", .DATA_DATE .cast(date))

LKP_MASTER_OUT21 = LKP_MASTER_OUT21.withColumn("PiDueAmt", when((((when(((LKP_MASTER_OUT1.rent_due_period_2) IS NOT None, (LKP_MASTER_OUT1.rent_due_period_2)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_3) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_3)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_4) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_4)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_5) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_5)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_6) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_6)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_7) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_7)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_8) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_8)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_9) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_9)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_10) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_10)).otherwise(lit(0))) < lit(15))).otherwise(lit(0)).otherwise(((when(((LKP_MASTER_OUT1.rent_due_period_2) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_2)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_3) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_3)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_4) IS NOT None)).otherwise(((LKP_MASTER_OUT1.rent_due_period_4)))).otherwise((lit(0)) + (IFF((LKP_MASTER_OUT1.rent_due_period_5) IS NOT None))).otherwise(((LKP_MASTER_OUT1.rent_due_period_5)))).otherwise((lit(0)) + (IFF((LKP_MASTER_OUT1.rent_due_period_6) IS NOT None))).otherwise(((LKP_MASTER_OUT1.rent_due_period_6)))).otherwise((lit(0)) + (IFF((LKP_MASTER_OUT1.rent_due_period_7) IS NOT None))).otherwise(((LKP_MASTER_OUT1.rent_due_period_7)))).otherwise((lit(0)) + (IFF((LKP_MASTER_OUT1.rent_due_period_8) IS NOT None))).otherwise(((LKP_MASTER_OUT1.rent_due_period_8)))).otherwise((lit(0)) + (IFF((LKP_MASTER_OUT1.rent_due_period_9) IS NOT None))).otherwise(((LKP_MASTER_OUT1.rent_due_period_9)))).otherwise((lit(0)) + (IFF((LKP_MASTER_OUT1.rent_due_period_10) IS NOT None))).otherwise(((LKP_MASTER_OUT1.rent_due_period_10))).otherwise(lit(0))))))))))))))))))))))

LKP_MASTER_OUT21 = LKP_MASTER_OUT21.withColumn("OldRentDueDt", when((LKP_MASTER_OUT1.oldest_rent_due_date_contract_aging == None), (lit(
    '0001-01-01'))).otherwise(LKP_MASTER_OUT1.oldest_rent_due_date_contract_aging .cast(date)))

LKP_MASTER_OUT21 = LKP_MASTER_OUT21.withColumn("RegUnfunded", lit(0))

LKP_MASTER_OUT21 = LKP_MASTER_OUT21.withColumn("RegBnkShrUnfd", lit(0))

LKP_MASTER_OUT21 = LKP_MASTER_OUT21.withColumn("Svrentdueperiod", trim(LKP_MASTER_OUT1.rent_due_period_2) + trim(LKP_MASTER_OUT1.rent_due_period_3) + trim(
    LKP_MASTER_OUT1.rent_due_period_4) + trim(LKP_MASTER_OUT1.rent_due_period_5) + trim(LKP_MASTER_OUT1.rent_due_period_6) + trim(LKP_MASTER_OUT1.rent_due_period_7))

LKP_MASTER_OUT21 = LKP_MASTER_OUT21.withColumn("TotPdueFeeAmt", trim(LKP_MASTER_OUT1.late_fee_due_period_2) + trim(LKP_MASTER_OUT1.late_fee_due_period_3) + trim(
    LKP_MASTER_OUT1.late_fee_due_period_4) + trim(LKP_MASTER_OUT1.late_fee_due_period_5) + trim(LKP_MASTER_OUT1.late_fee_due_period_6) + trim(LKP_MASTER_OUT1.late_fee_due_period_7))

LKP_MASTER_OUT21 = LKP_MASTER_OUT21.withColumn("MatDt", when(
    (LKP_MASTER_OUT1.renewal_maturity_date_lti_rpt == None), (LKP_MASTER_OUT1.maturity_date)).otherwise(RenwMatDt))

LKP_MASTER_OUT21 = LKP_MASTER_OUT21.withColumn("StageVar", (when(((LKP_MASTER_OUT1.rent_due_period_2) IS NOT None, (LKP_MASTER_OUT1.rent_due_period_2)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_3) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_3)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_4) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_4)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_5) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_5)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_6) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_6)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_7) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_7))).otherwise((lit(0)) + (IFF((LKP_MASTER_OUT1.rent_due_period_8) IS NOT None))).otherwise(((LKP_MASTER_OUT1.rent_due_period_8)))).otherwise((lit(0)) + (IFF((LKP_MASTER_OUT1.rent_due_period_9) IS NOT None))).otherwise(((LKP_MASTER_OUT1.rent_due_period_9)))).otherwise((lit(0)) + (IFF((LKP_MASTER_OUT1.rent_due_period_10) IS NOT None))).otherwise(((LKP_MASTER_OUT1.rent_due_period_10))).otherwise(lit(0)))))))))))).select(
    (when((trim(LKP_MASTER_OUT1.product_code) == lit('LOAN'), (when(((LKP_MASTER_OUT1.interest_ltd) IS NOT None).otherwise((LKP_MASTER_OUT1.interest_ltd)).otherwise(lit(0)))).otherwise((when(((LKP_MASTER_OUT1.ltd_earned_income_residual) IS NOT None)).otherwise(((LKP_MASTER_OUT1.ltd_earned_income_residual)))).otherwise((lit(0)) + (IFF((LKP_MASTER_OUT1.ltd_earned_income_lease) IS NOT None))).otherwise(((LKP_MASTER_OUT1.ltd_earned_income_lease))).otherwise(lit(0)))))).alias('lftd_int_paid_amt'),
    (when((trim(LKP_MASTER_OUT1.product_code) == lit('LOAN'), (when(((LKP_MASTER_OUT1.interest_ytd) IS NOT None).otherwise((LKP_MASTER_OUT1.interest_ytd)).otherwise(lit(0)))).otherwise((when(((LKP_MASTER_OUT1.ytd_earned_income_residual) IS NOT None)).otherwise(((LKP_MASTER_OUT1.ytd_earned_income_residual)))).otherwise((lit(0)) + (IFF((LKP_MASTER_OUT1.ytd_earned_income_lease) IS NOT None))).otherwise(((LKP_MASTER_OUT1.ytd_earned_income_lease))).otherwise(lit(0)))))).alias('ytd_int_paid_amt'),
    (when((trim(LKP_MASTER_OUT1.product_code) == lit('LOAN'), (when(((LKP_MASTER_OUT1.interest_mtd) IS NOT None).otherwise((LKP_MASTER_OUT1.interest_mtd)).otherwise(lit(0)))).otherwise((when(((LKP_MASTER_OUT1.mtd_earned_income_residual) IS NOT None)).otherwise(((LKP_MASTER_OUT1.mtd_earned_income_residual)))).otherwise((lit(0)) + (IFF((LKP_MASTER_OUT1.mtd_earned_income_lease) IS NOT None))).otherwise(((LKP_MASTER_OUT1.mtd_earned_income_lease))).otherwise(lit(0)))))).alias('mtd_ln_int_paid_amt'),
    LKP_MASTER_OUT1.interest_ltd.alias('interest_ltd'),
    LKP_MASTER_OUT1.interest_ytd.alias('interest_ytd'),
    LKP_MASTER_OUT1.interest_mtd.alias('interest_mtd'),
    LKP_MASTER_OUT1.ltd_earned_income_lease.alias('ltd_earned_income_lease'),
    LKP_MASTER_OUT1.ltd_earned_income_residual.alias(
        'ltd_earned_income_residual'),
    LKP_MASTER_OUT1.ytd_earned_income_lease.alias('ytd_earned_income_lease'),
    LKP_MASTER_OUT1.ytd_earned_income_residual.alias(
        'ytd_earned_income_residual'),
    LKP_MASTER_OUT1.mtd_earned_income_lease.alias('mtd_earned_income_lease'),
    LKP_MASTER_OUT1.mtd_earned_income_residual.alias(
        'mtd_earned_income_residual'),
    (when(((Len(trim((when(((LKP_MASTER_OUT1.termination_date) IS NOT None, (LKP_MASTER_OUT1.termination_date)).otherwise(lit('')))) == lit(0)) & (trim(LKP_MASTER_OUT1.is_booked) == lit('True')))).otherwise(when((trim(LKP_MASTER_OUT1.product_code) == lit('CSA')).otherwise((when(((LKP_MASTER_OUT1.ltd_earned_income_lease) IS NOT None).otherwise((LKP_MASTER_OUT1.ltd_earned_income_lease)).otherwise(lit(0)) - (when(((LKP_MASTER_OUT1.deferred_income_lease) IS NOT None).otherwise((LKP_MASTER_OUT1.deferred_income_lease)).otherwise(lit(0)))).otherwise(when(((trim(LKP_MASTER_OUT1.product_code) == lit('TAX')) | (trim(LKP_MASTER_OUT1.product_code) == lit('FIN')) | (trim(LKP_MASTER_OUT1.product_code) == lit('LEV'))).otherwise(((when(((LKP_MASTER_OUT1.ltd_earned_income_lease) IS NOT None).otherwise((LKP_MASTER_OUT1.ltd_earned_income_lease)).otherwise(lit(0)) + (IFF((LKP_MASTER_OUT1.ltd_earned_income_residual) IS NOT None)).otherwise(((LKP_MASTER_OUT1.ltd_earned_income_residual)))).otherwise((lit(0))) - ((IFF((LKP_MASTER_OUT1.deferred_income_residual) IS NOT None))).otherwise(((LKP_MASTER_OUT1.deferred_income_residual)))).otherwise((lit(0)) + (IFF((LKP_MASTER_OUT1.deferred_income_lease) IS NOT None))).otherwise(((LKP_MASTER_OUT1.deferred_income_lease)))).otherwise((lit(0))))))).otherwise((lit(0)))).otherwise(lit(0)))))))).alias('disc_cur_bal'),
    LKP_MASTER_OUT1.is_booked.alias('is_booked'),
    LKP_MASTER_OUT1.product_code.alias('product_code_1'),
    LKP_MASTER_OUT1.termination_date.alias('termination_date'),
    LKP_MASTER_OUT1.deferred_income_lease.alias('deferred_income_lease'),
    LKP_MASTER_OUT1.deferred_income_residual.alias('deferred_income_residual'),
    LKP_MASTER_OUT1.ltd_earned_income_lease.alias('ltd_earned_income_lease_1'),
    LKP_MASTER_OUT1.ltd_earned_income_residual.alias(
        'ltd_earned_income_residual_1'),
    LKP_MASTER_OUT1.OPEN_CLOSE_CD.alias('OPEN_CLOSE_CD'),
    LKP_MASTER_OUT1.maturity_date.alias('maturity_date'),
    LKP_MASTER_OUT1.renewal_maturity_date.alias('renewal_maturity_date'),
    LKP_MASTER_OUT1.renewal_maturity_date_lti_rpt.alias(
        'renewal_maturity_date_lti_rpt'),
    LKP_MASTER_OUT1.extended_maturity_date.alias('extended_maturity_date'),
    LKP_MASTER_OUT1.BANK_SHR_BAL.alias('BANK_SHR_BAL'),
    LKP_MASTER_OUT1.rent_due_period_1.alias('rent_due_period_1'),
    LKP_MASTER_OUT1.rent_due_period_10.alias('rent_due_period_10'),
    LKP_MASTER_OUT1.rent_due_period_2.alias('rent_due_period_2'),
    LKP_MASTER_OUT1.rent_due_period_3.alias('rent_due_period_3'),
    LKP_MASTER_OUT1.rent_due_period_4.alias('rent_due_period_4'),
    LKP_MASTER_OUT1.rent_due_period_5.alias('rent_due_period_5'),
    LKP_MASTER_OUT1.rent_due_period_6.alias('rent_due_period_6'),
    LKP_MASTER_OUT1.rent_due_period_7.alias('rent_due_period_7'),
    LKP_MASTER_OUT1.rent_due_period_8.alias('rent_due_period_8'),
    LKP_MASTER_OUT1.rent_due_period_9.alias('rent_due_period_9'),
    (when((LKP_MASTER_OUT1.OPEN_CLOSE_CD == lit('C'), lit(0)).otherwise(when((LKP_MASTER_OUT1.BANK_SHR_BAL < lit(2)).otherwise(lit(0)).otherwise(when((Matdtnew1 <=
     AsOfDt)).otherwise((LKP_MASTER_OUT1.principal_balance))).otherwise((IFF(PiDueAmt > lit(15)))).otherwise((PiDueAmt)).otherwise(lit(0)))))).alias('pdue_pi_amt_new'),
    LKP_MASTER_OUT1.principal_balance.alias('principal_balance'),
    (when((LKP_MASTER_OUT1.OPEN_CLOSE_CD == lit('C'), lit(0)).otherwise(when((LKP_MASTER_OUT1.OPEN_CLOSE_CD == lit('C')).otherwise(lit(0)).otherwise(when((LKP_MASTER_OUT1.BANK_SHR_BAL < lit(2)).otherwise(lit(
        0)).otherwise(IFF(MatDt <= LKP_MASTER_OUT1.DATA_DATE)).otherwise((LKP_MASTER_OUT1.principal_balance))).otherwise((IFF(PiDueAmt > lit(15)))).otherwise((PiDueAmt)).otherwise(lit(0))))))).alias('pdue_pi_amt'),
    LKP_MASTER_OUT1.PiDueAmt.alias('PiDueAmt'),
    LKP_MASTER_OUT1.StageLKP_MASTER_OUT1.alias('Amtchk'),
    LKP_MASTER_OUT1.Matdtnew1.alias('Matdtnew'),
    LKP_MASTER_OUT1.AsOfDt.alias('Asofdt'),
    LKP_MASTER_OUT1.MatDt.alias('Matdt'),
    LKP_MASTER_OUT1.RenwMatDt.alias('Renwmatdt'),
    LKP_MASTER_OUT1.ACCT_ID.alias('ACCT_ID')
).filter("ACCT_ID <> 0")

# Processing node LKP_MASTER_OUT2, type TRANSFORMATION
# COLUMN COUNT: 192
# Original node name XFM_GL, link LKP_MASTER_OUT2

LKP_MASTER_OUT2 = LKP_MASTER_OUT1
LKP_MASTER_OUT2 = LKP_MASTER_OUT2.withColumn("Matdtnew1", when((LKP_MASTER_OUT1.extended_maturity_date == None), (
    LKP_MASTER_OUT1.maturity_date)).otherwise(LKP_MASTER_OUT1.extended_maturity_date))

LKP_MASTER_OUT2 = LKP_MASTER_OUT2.withColumn("EquipmentCost", (when(((LKP_MASTER_OUT1.equipment_cost_equipment_summary) IS NOT None), ((LKP_MASTER_OUT1.equipment_cost_equipment_summary))).otherwise(lit(0))))

LKP_MASTER_OUT2 = LKP_MASTER_OUT2.withColumn("RenMatDt", (when(((LKP_MASTER_OUT1.renewal_maturity_date_lti_rpt) IS NOT None), ((LKP_MASTER_OUT1.renewal_maturity_date_lti_rpt))).otherwise((lit('00010101')))))

LKP_MASTER_OUT2 = LKP_MASTER_OUT2.withColumn("RenwMatDt", RenMatDt .cast(date))

LKP_MASTER_OUT2 = LKP_MASTER_OUT2.withColumn("AsOfDt", .DATA_DATE .cast(date))

LKP_MASTER_OUT2 = LKP_MASTER_OUT2.withColumn("PiDueAmt", when((((when(((LKP_MASTER_OUT1.rent_due_period_2) IS NOT None, (LKP_MASTER_OUT1.rent_due_period_2)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_3) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_3)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_4) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_4)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_5) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_5)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_6) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_6)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_7) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_7)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_8) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_8)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_9) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_9)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_10) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_10)).otherwise(lit(0))) < lit(15))).otherwise(lit(0)).otherwise(((when(((LKP_MASTER_OUT1.rent_due_period_2) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_2)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_3) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_3)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_4) IS NOT None)).otherwise(((LKP_MASTER_OUT1.rent_due_period_4)))).otherwise((lit(0)) + (IFF((LKP_MASTER_OUT1.rent_due_period_5) IS NOT None))).otherwise(((LKP_MASTER_OUT1.rent_due_period_5)))).otherwise((lit(0)) + (IFF((LKP_MASTER_OUT1.rent_due_period_6) IS NOT None))).otherwise(((LKP_MASTER_OUT1.rent_due_period_6)))).otherwise((lit(0)) + (IFF((LKP_MASTER_OUT1.rent_due_period_7) IS NOT None))).otherwise(((LKP_MASTER_OUT1.rent_due_period_7)))).otherwise((lit(0)) + (IFF((LKP_MASTER_OUT1.rent_due_period_8) IS NOT None))).otherwise(((LKP_MASTER_OUT1.rent_due_period_8)))).otherwise((lit(0)) + (IFF((LKP_MASTER_OUT1.rent_due_period_9) IS NOT None))).otherwise(((LKP_MASTER_OUT1.rent_due_period_9)))).otherwise((lit(0)) + (IFF((LKP_MASTER_OUT1.rent_due_period_10) IS NOT None))).otherwise(((LKP_MASTER_OUT1.rent_due_period_10))).otherwise(lit(0))))))))))))))))))))))

LKP_MASTER_OUT2 = LKP_MASTER_OUT2.withColumn("OldRentDueDt", when((LKP_MASTER_OUT1.oldest_rent_due_date_contract_aging == None), (lit(
    '0001-01-01'))).otherwise(LKP_MASTER_OUT1.oldest_rent_due_date_contract_aging .cast(date)))

LKP_MASTER_OUT2 = LKP_MASTER_OUT2.withColumn("RegUnfunded", lit(0))

LKP_MASTER_OUT2 = LKP_MASTER_OUT2.withColumn("RegBnkShrUnfd", lit(0))

LKP_MASTER_OUT2 = LKP_MASTER_OUT2.withColumn("Svrentdueperiod", trim(LKP_MASTER_OUT1.rent_due_period_2) + trim(LKP_MASTER_OUT1.rent_due_period_3) + trim(
    LKP_MASTER_OUT1.rent_due_period_4) + trim(LKP_MASTER_OUT1.rent_due_period_5) + trim(LKP_MASTER_OUT1.rent_due_period_6) + trim(LKP_MASTER_OUT1.rent_due_period_7))

LKP_MASTER_OUT2 = LKP_MASTER_OUT2.withColumn("TotPdueFeeAmt", trim(LKP_MASTER_OUT1.late_fee_due_period_2) + trim(LKP_MASTER_OUT1.late_fee_due_period_3) + trim(
    LKP_MASTER_OUT1.late_fee_due_period_4) + trim(LKP_MASTER_OUT1.late_fee_due_period_5) + trim(LKP_MASTER_OUT1.late_fee_due_period_6) + trim(LKP_MASTER_OUT1.late_fee_due_period_7))

LKP_MASTER_OUT2 = LKP_MASTER_OUT2.withColumn("MatDt", when(
    (LKP_MASTER_OUT1.renewal_maturity_date_lti_rpt == None), (LKP_MASTER_OUT1.maturity_date)).otherwise(RenwMatDt))

LKP_MASTER_OUT2 = LKP_MASTER_OUT2.withColumn("StageVar", (when(((LKP_MASTER_OUT1.rent_due_period_2) IS NOT None, (LKP_MASTER_OUT1.rent_due_period_2)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_3) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_3)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_4) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_4)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_5) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_5)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_6) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_6)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.rent_due_period_7) IS NOT None).otherwise((LKP_MASTER_OUT1.rent_due_period_7))).otherwise((lit(0)) + (IFF((LKP_MASTER_OUT1.rent_due_period_8) IS NOT None))).otherwise(((LKP_MASTER_OUT1.rent_due_period_8)))).otherwise((lit(0)) + (IFF((LKP_MASTER_OUT1.rent_due_period_9) IS NOT None))).otherwise(((LKP_MASTER_OUT1.rent_due_period_9)))).otherwise((lit(0)) + (IFF((LKP_MASTER_OUT1.rent_due_period_10) IS NOT None))).otherwise(((LKP_MASTER_OUT1.rent_due_period_10))).otherwise(lit(0)))))))))))).select(
    LKP_MASTER_OUT1.AsOfDt.alias('AS_OF_DT'),
    LKP_MASTER_OUT1.MST_ACCT_NATL_KEY.alias('MST_ACCT_NATL_KEY'),
    LKP_MASTER_OUT1.PROD_DTL_NATL_KEY.alias('PROD_DTL_NATL_KEY'),
    (lit('LS')).alias('APPL_CD_LS'),
    LKP_MASTER_OUT1.APPL_CD_LX.alias('APPL_CD_LX'),
    (when(((LKP_MASTER_OUT1.last_month_end_date IS NOT None) & (LKP_MASTER_OUT1.termination_date == None)), (lit('C'))).otherwise(lit('None'))).alias('CLOSE_TO_POST_LKP'),
    (lit('None')).alias('CLOSE_TO_POST_LKP_1'),
    LKP_MASTER_OUT1.ACCT_ID.alias('ACCT_ID'),
    LKP_MASTER_OUT1.OPEN_CLOSE_CD.alias('OPEN_CLOSE_CD'),
    LKP_MASTER_OUT1.APPLICATIONS.alias('APPLICATIONS'),
    LKP_MASTER_OUT1.IN_2.alias('IN_2'),
    LKP_MASTER_OUT1.months_renewal_maturity_date.alias(
        'months_renewal_maturity_date'),
    LKP_MASTER_OUT1.acceptance_date.alias('acceptance_date'),
    LKP_MASTER_OUT1.account_dist_code.alias('account_dist_code'),
    LKP_MASTER_OUT1.account_dist_code_description.alias(
        'account_dist_code_description'),
    LKP_MASTER_OUT1.bill_to_address_1.alias('bill_to_address_1'),
    LKP_MASTER_OUT1.bill_to_address_2.alias('bill_to_address_2'),
    LKP_MASTER_OUT1.bill_to_address_3.alias('bill_to_address_3'),
    LKP_MASTER_OUT1.bill_to_attention.alias('bill_to_attention'),
    LKP_MASTER_OUT1.bill_to_city.alias('bill_to_city'),
    LKP_MASTER_OUT1.bill_to_country.alias('bill_to_country'),
    LKP_MASTER_OUT1.bill_to_country_code.alias('bill_to_country_code'),
    LKP_MASTER_OUT1.bill_to_county.alias('bill_to_county'),
    LKP_MASTER_OUT1.bill_to_email_address.alias('bill_to_email_address'),
    LKP_MASTER_OUT1.bill_to_fax_number.alias('bill_to_fax_number'),
    LKP_MASTER_OUT1.bill_to_invoice_code.alias('bill_to_invoice_code'),
    LKP_MASTER_OUT1.bill_to_invoice_group_description.alias(
        'bill_to_invoice_group_description'),
    LKP_MASTER_OUT1.bill_to_is_invoice_group.alias('bill_to_is_invoice_group'),
    LKP_MASTER_OUT1.bill_to_location_code.alias('bill_to_location_code'),
    LKP_MASTER_OUT1.bill_to_location_oid.alias('bill_to_location_oid'),
    LKP_MASTER_OUT1.bill_to_name.alias('bill_to_name'),
    LKP_MASTER_OUT1.bill_to_phone_ext.alias('bill_to_phone_ext'),
    LKP_MASTER_OUT1.bill_to_phone_number.alias('bill_to_phone_number'),
    LKP_MASTER_OUT1.bill_to_postal_code.alias('bill_to_postal_code'),
    LKP_MASTER_OUT1.bill_to_state.alias('bill_to_state'),
    LKP_MASTER_OUT1.bill_to_state_code.alias('bill_to_state_code'),
    LKP_MASTER_OUT1.book_treatment.alias('book_treatment'),
    LKP_MASTER_OUT1.booked_post_date.alias('booked_post_date'),
    LKP_MASTER_OUT1.chargeoff_effective_date.alias('chargeoff_effective_date'),
    LKP_MASTER_OUT1.commencement_date.alias('commencement_date'),
    LKP_MASTER_OUT1.compounding_period.alias('compounding_period'),
    LKP_MASTER_OUT1.compute_method.alias('compute_method'),
    LKP_MASTER_OUT1.contract_id.alias('contract_id'),
    LKP_MASTER_OUT1.contract_manager_id.alias('contract_manager_id'),
    LKP_MASTER_OUT1.contract_manager_name.alias('contract_manager_name'),
    LKP_MASTER_OUT1.contract_manager_oid.alias('contract_manager_oid'),
    LKP_MASTER_OUT1.contract_oid.alias('contract_oid'),
    LKP_MASTER_OUT1.contract_type_code.alias('contract_type_code'),
    LKP_MASTER_OUT1.contract_type_description.alias(
        'contract_type_description'),
    LKP_MASTER_OUT1.customer_alt_name.alias('customer_alt_name'),
    LKP_MASTER_OUT1.customer_dba.alias('customer_dba'),
    LKP_MASTER_OUT1.customer_first_name.alias('customer_first_name'),
    LKP_MASTER_OUT1.customer_id.alias('customer_id'),
    LKP_MASTER_OUT1.customer_last_name.alias('customer_last_name'),
    LKP_MASTER_OUT1.customer_middle_name.alias('customer_middle_name'),
    LKP_MASTER_OUT1.customer_name.alias('customer_name'),
    LKP_MASTER_OUT1.customer_oid.alias('customer_oid'),
    LKP_MASTER_OUT1.customer_salutaion.alias('customer_salutaion'),
    LKP_MASTER_OUT1.delinquency_code_code.alias('delinquency_code_code'),
    LKP_MASTER_OUT1.delinquency_code_description.alias(
        'delinquency_code_description'),
    LKP_MASTER_OUT1.delinquency_code_oid.alias('delinquency_code_oid'),
    LKP_MASTER_OUT1.finance_company_name.alias('finance_company_name'),
    LKP_MASTER_OUT1.finance_company_oid.alias('finance_company_oid'),
    LKP_MASTER_OUT1.funding_date.alias('funding_date'),
    LKP_MASTER_OUT1.funding_reference.alias('funding_reference'),
    LKP_MASTER_OUT1.funding_source_id.alias('funding_source_id'),
    LKP_MASTER_OUT1.funding_source_name.alias('funding_source_name'),
    LKP_MASTER_OUT1.funding_source_oid.alias('funding_source_oid'),
    LKP_MASTER_OUT1.implicit_rate.alias('implicit_rate'),
    LKP_MASTER_OUT1.interest_calculation.alias('interest_calculation'),
    LKP_MASTER_OUT1.invoice_code_code.alias('invoice_code_code'),
    LKP_MASTER_OUT1.invoice_code_description.alias('invoice_code_description'),
    LKP_MASTER_OUT1.invoice_code_oid.alias('invoice_code_oid'),
    LKP_MASTER_OUT1.is_booked.alias('is_booked'),
    LKP_MASTER_OUT1.is_chargeoff.alias('is_chargeoff'),
    LKP_MASTER_OUT1.is_invoicing_suspended.alias('is_invoicing_suspended'),
    LKP_MASTER_OUT1.is_nonaccrual.alias('is_nonaccrual'),
    LKP_MASTER_OUT1.is_same_as_cash.alias('is_same_as_cash'),
    LKP_MASTER_OUT1.is_terminated.alias('is_terminated'),
    LKP_MASTER_OUT1.is_variable_rate.alias('is_variable_rate'),
    LKP_MASTER_OUT1.maturity_date.alias('maturity_date'),
    LKP_MASTER_OUT1.nonaccrual_effective_date.alias(
        'nonaccrual_effective_date'),
    LKP_MASTER_OUT1.payment_adjustment_method.alias(
        'payment_adjustment_method'),
    LKP_MASTER_OUT1.payment_frequency.alias('payment_frequency'),
    LKP_MASTER_OUT1.payments_in_advance.alias('payments_in_advance'),
    LKP_MASTER_OUT1.primary_contact_name.alias('primary_contact_name'),
    LKP_MASTER_OUT1.primary_contact_title.alias('primary_contact_title'),
    LKP_MASTER_OUT1.product_code.alias('product_code'),
    LKP_MASTER_OUT1.product_description.alias('product_description'),
    LKP_MASTER_OUT1.product_oid.alias('product_oid'),
    LKP_MASTER_OUT1.purchase_order_number.alias('purchase_order_number'),
    LKP_MASTER_OUT1.rent_transaction_code_code.alias(
        'rent_transaction_code_code'),
    LKP_MASTER_OUT1.rent_transaction_code_description.alias(
        'rent_transaction_code_description'),
    LKP_MASTER_OUT1.rent_transaction_code_invoice_description.alias(
        'rent_transaction_code_invoice_description'),
    LKP_MASTER_OUT1.rent_transaction_code_oid.alias(
        'rent_transaction_code_oid'),
    LKP_MASTER_OUT1.sales_rep_address_1.alias('sales_rep_address_1'),
    LKP_MASTER_OUT1.sales_rep_address_2.alias('sales_rep_address_2'),
    LKP_MASTER_OUT1.sales_rep_address_3.alias('sales_rep_address_3'),
    LKP_MASTER_OUT1.sales_rep_city.alias('sales_rep_city'),
    LKP_MASTER_OUT1.sales_rep_county.alias('sales_rep_county'),
    LKP_MASTER_OUT1.sales_rep_email_address.alias('sales_rep_email_address'),
    LKP_MASTER_OUT1.sales_rep_fax_ext.alias('sales_rep_fax_ext'),
    LKP_MASTER_OUT1.sales_rep_fax_number.alias('sales_rep_fax_number'),
    LKP_MASTER_OUT1.sales_rep_id.alias('sales_rep_id'),
    LKP_MASTER_OUT1.sales_rep_name.alias('sales_rep_name'),
    LKP_MASTER_OUT1.sales_rep_oid.alias('sales_rep_oid'),
    LKP_MASTER_OUT1.sales_rep_phone_ext.alias('sales_rep_phone_ext'),
    LKP_MASTER_OUT1.sales_rep_phone_number.alias('sales_rep_phone_number'),
    LKP_MASTER_OUT1.sales_rep_postal_code.alias('sales_rep_postal_code'),
    LKP_MASTER_OUT1.sales_rep_state_code.alias('sales_rep_state_code'),
    LKP_MASTER_OUT1.same_as_cash_duration.alias('same_as_cash_duration'),
    LKP_MASTER_OUT1.same_as_cash_end_date.alias('same_as_cash_end_date'),
    LKP_MASTER_OUT1.same_as_cash_interval.alias('same_as_cash_interval'),
    LKP_MASTER_OUT1.sign_date.alias('sign_date'),
    LKP_MASTER_OUT1.start_date.alias('start_date'),
    LKP_MASTER_OUT1.suffix.alias('suffix'),
    LKP_MASTER_OUT1.tax_treatment.alias('tax_treatment'),
    LKP_MASTER_OUT1.term.alias('term'),
    LKP_MASTER_OUT1.termination_date.alias('termination_date'),
    LKP_MASTER_OUT1.transaction_number.alias('transaction_number'),
    LKP_MASTER_OUT1.vendor_manager_id.alias('vendor_manager_id'),
    LKP_MASTER_OUT1.vendor_manager_name.alias('vendor_manager_name'),
    LKP_MASTER_OUT1.vendor_manager_oid.alias('vendor_manager_oid'),
    LKP_MASTER_OUT1.year_length.alias('year_length'),
    LKP_MASTER_OUT1.yield_rate.alias('yield_rate'),
    LKP_MASTER_OUT1.customer_oid_from_customer.alias(
        'customer_oid_from_customer'),
    LKP_MASTER_OUT1.value_zr.alias('value_zr'),
    LKP_MASTER_OUT1.oid_from_chrg_dwn_amt.alias('oid_from_chrg_dwn_amt'),
    LKP_MASTER_OUT1.contract_oid_from_eff_rate_chg.alias(
        'contract_oid_from_eff_rate_chg'),
    LKP_MASTER_OUT1.benchmark_description.alias('benchmark_description'),
    LKP_MASTER_OUT1.benchmark_rate.alias('benchmark_rate'),
    LKP_MASTER_OUT1.plus_factor.alias('plus_factor'),
    LKP_MASTER_OUT1.contract_oid_from_contract_aging.alias(
        'contract_oid_from_contract_aging'),
    LKP_MASTER_OUT1.oldest_rent_due_date.alias('oldest_rent_due_date'),
    LKP_MASTER_OUT1.next_payment_due_date.alias('next_payment_due_date'),
    LKP_MASTER_OUT1.contract_oid_from_contract_general_ledger.alias(
        'contract_oid_from_contract_general_ledger'),
    LKP_MASTER_OUT1.original_lease_receivable_balance.alias(
        'original_lease_receivable_balance'),
    LKP_MASTER_OUT1.original_residual_receivable.alias(
        'original_residual_receivable'),
    LKP_MASTER_OUT1.contract_oid_from_contract_equipment_summary.alias(
        'contract_oid_from_contract_equipment_summary'),
    LKP_MASTER_OUT1.equipment_cost.alias('equipment_cost'),
    LKP_MASTER_OUT1.contract_oid_from_rpt_renewal_status.alias(
        'contract_oid_from_rpt_renewal_status'),
    LKP_MASTER_OUT1.is_current_renewal.alias('is_current_renewal'),
    LKP_MASTER_OUT1.renewal_maturity_date.alias('renewal_maturity_date'),
    LKP_MASTER_OUT1.renewal_start_date.alias('renewal_start_date'),
    LKP_MASTER_OUT1.finance_program_oid.alias('finance_program_oid'),
    LKP_MASTER_OUT1.customer_ssn_tax_id_number.alias(
        'customer_ssn_tax_id_number'),
    LKP_MASTER_OUT1.customer_business_individual_indicator.alias(
        'customer_business_individual_indicator'),
    LKP_MASTER_OUT1.industry_type.alias('industry_type'),
    LKP_MASTER_OUT1.entity_oid.alias('entity_oid'),
    LKP_MASTER_OUT1.account_industry_type.alias('account_industry_type'),
    LKP_MASTER_OUT1.benchmark_id.alias('benchmark_id'),
    LKP_MASTER_OUT1.udfcontract_year_of_libor_used.alias(
        'udfcontract_year_of_libor_used'),
    LKP_MASTER_OUT1.last_month_end_date.alias('last_month_end_date'),
    (LKP_MASTER_OUT1.deferred_income_lease .cast(
        numeric)).alias('deferred_income_lease'),
    (LKP_MASTER_OUT1.ltd_earned_income_lease .cast(
        numeric)).alias('ltd_earned_income_lease'),
    (LKP_MASTER_OUT1.ltd_earned_income_residual .cast(
        numeric)).alias('ltd_earned_income_residual'),
    LKP_MASTER_OUT1.earnings_type_oid.alias('earnings_type_oid'),
    (LKP_MASTER_OUT1.earnings .cast(numeric)).alias('earnings'),
    (lit(0)).alias('disc_cur_bal'),
    (when((((LKP_MASTER_OUT1.termination_date == None) | (Len(trim(LKP_MASTER_OUT1.termination_date)) == lit(0))) & (trim(LKP_MASTER_OUT1.is_booked) == lit('True')), - lit(1) * ((when(((LKP_MASTER_OUT1.idc) IS NOT None).otherwise((LKP_MASTER_OUT1.idc)).otherwise(lit(0)) - (IFF((LKP_MASTER_OUT1.accumulated_idc) IS NOT None)).otherwise(((LKP_MASTER_OUT1.accumulated_idc)))).otherwise((lit(0)) * - lit(1))))).otherwise(lit(0)))).alias('fasb_cur_bal'),
    (when(((trim(LKP_MASTER_OUT1.product_code) == lit('LOAN')) & (trim(LKP_MASTER_OUT1.description) == lit('ZCC-IDC')), - lit(1) * (when(((LKP_MASTER_OUT1.amount) IS NOT None).otherwise((LKP_MASTER_OUT1.amount)).otherwise(lit(0))))).otherwise((- lit(1) * (IFF((LKP_MASTER_OUT1.idc) IS NOT None))).otherwise(((LKP_MASTER_OUT1.idc))).otherwise(lit(0))))).alias('fasb_cost_orig_amt'),
    (when((((LKP_MASTER_OUT1.termination_date == None) | (Len(trim(LKP_MASTER_OUT1.termination_date)) == lit(0))) & (trim(
        LKP_MASTER_OUT1.is_booked) == lit('True'))), (LKP_MASTER_OUT1.accumulated_fee .cast(numeric))).otherwise(lit(0))).alias('fasb_fee_cur_bal'),
    (when((LKP_MASTER_OUT1.last_payment_amount IS NOT None), (LKP_MASTER_OUT1.last_payment_amount)).otherwise(None)).alias('last_pmt_pst_amt'),
    (when(((LKP_MASTER_OUT1.last_rental_paid_date == None) | (Len(trim(LKP_MASTER_OUT1.last_rental_paid_date))
     == lit(0))), (None)).otherwise(LKP_MASTER_OUT1.last_rental_paid_date .cast(date))).alias('last_pmt_pst_dt'),
    (when(((LKP_MASTER_OUT1.renewal_maturity_date_lti_rpt == None) | (Len(trim(LKP_MASTER_OUT1.renewal_maturity_date_lti_rpt)) == lit(0)), None).otherwise(IFF(
        (LKP_MASTER_OUT1.renewal_maturity_date_lti_rpt .cast(date) > LKP_MASTER_OUT1.maturity_date))).otherwise((LKP_MASTER_OUT1.maturity_date)).otherwise(None))).alias('last_mat_extn_dt'),
    LKP_MASTER_OUT1.delinquency_code_code.alias('late_chrg_meth_cd'),
    (when((substring(LKP_MASTER_OUT1.datavalue, lit(1)).otherwise(lit(3)) == lit('DIS')).otherwise(lit(0)).otherwise(when(((trim(LKP_MASTER_OUT1.product_code) == lit('LOAN')) & (LKP_MASTER_OUT1.ltd_interest_income > lit(0)) & ((LKP_MASTER_OUT1.earnings_type_oid) == lit('11100')) & (trim(LKP_MASTER_OUT1.earnings_date) <= lit(DATA_DATE))).otherwise((when(((LKP_MASTER_OUT1.ltd_interest_income) IS NOT None).otherwise((LKP_MASTER_OUT1.ltd_interest_income)).otherwise(lit(0)))).otherwise(when((((when(((LKP_MASTER_OUT1.SUM_EARNINGS) IS NOT None).otherwise((LKP_MASTER_OUT1.SUM_EARNINGS)).otherwise(lit(0)) > lit(0)) & ((when(((LKP_MASTER_OUT1.ltd_earned_income_residual) IS NOT None).otherwise((LKP_MASTER_OUT1.ltd_earned_income_residual))).otherwise((lit(0)) > lit(0))))).otherwise(((IFF((LKP_MASTER_OUT1.SUM_EARNINGS) IS NOT None))).otherwise(((LKP_MASTER_OUT1.SUM_EARNINGS)))).otherwise((lit(0)) + (IFF((LKP_MASTER_OUT1.ltd_earned_income_residual) IS NOT None))).otherwise(((LKP_MASTER_OUT1.ltd_earned_income_residual)))).otherwise((lit(0))))).otherwise(lit(0)))))))).alias('lftd_bank_shr_int_paid_amt'),
    (when((trim(LKP_MASTER_OUT1.product_code) != lit('LOAN'), (when(((LKP_MASTER_OUT1.ltd_earned_income_residual) IS NOT None).otherwise((LKP_MASTER_OUT1.ltd_earned_income_residual)).otherwise(lit(0)) + (when(((LKP_MASTER_OUT1.SUM_EARNINGS) IS NOT None).otherwise((LKP_MASTER_OUT1.SUM_EARNINGS))).otherwise((lit(0)))))).otherwise(((IFF((LKP_MASTER_OUT1.interest_ltd) IS NOT None))).otherwise(((LKP_MASTER_OUT1.interest_ltd))).otherwise(lit(0)))))).alias('lftd_int_accr_amt'),
    (when((trim(LKP_MASTER_OUT1.product_code) == lit('LOAN'), (when(((LKP_MASTER_OUT1.interest_ltd) IS NOT None).otherwise((LKP_MASTER_OUT1.interest_ltd)).otherwise(lit(0)))).otherwise((when(((LKP_MASTER_OUT1.ltd_earned_income_residual) IS NOT None)).otherwise(((LKP_MASTER_OUT1.ltd_earned_income_residual)))).otherwise((lit(0)) + (IFF((LKP_MASTER_OUT1.ltd_earned_income_lease) IS NOT None))).otherwise(((LKP_MASTER_OUT1.ltd_earned_income_lease))).otherwise(lit(0)))))).alias('lftd_int_paid_amt'),
    (EquipmentCost .cast(numeric)).alias('ltd_cust_prin_draw_amt'),
    ((when(((LKP_MASTER_OUT1.value_zr_udf_extn_euipment) IS NOT None), ((LKP_MASTER_OUT1.value_zr_udf_extn_euipment))).otherwise(lit(0)))).alias('mat_dt_extn_cnt'),
    (LKP_MASTER_OUT1.next_payment_date .cast(date)).alias('next_payment_date'),
    (when((trim(LKP_MASTER_OUT1.product_code) == lit('LOAN')),
     (LKP_MASTER_OUT1.ytd_interest_income)).otherwise(lit(0))).alias('ytd_bank_shr_int_paid_amt'),
    (when((trim(LKP_MASTER_OUT1.product_code) == lit('LOAN')),
     (LKP_MASTER_OUT1.ytd_accrued_interest)).otherwise(lit(0))).alias('ytd_int_accr_amt'),
    LKP_MASTER_OUT1.current_payment_amount.alias('pmt_due_amt'),
    (when((trim(LKP_MASTER_OUT1.product_code) == lit('LOAN'), (when(((LKP_MASTER_OUT1.interest_mtd) IS NOT None).otherwise((LKP_MASTER_OUT1.interest_mtd)).otherwise(lit(0)))).otherwise((when(((LKP_MASTER_OUT1.mtd_earned_income_residual) IS NOT None)).otherwise(((LKP_MASTER_OUT1.mtd_earned_income_residual)))).otherwise((lit(0)) + (IFF((LKP_MASTER_OUT1.mtd_earned_income_lease) IS NOT None))).otherwise(((LKP_MASTER_OUT1.mtd_earned_income_lease))).otherwise(lit(0)))))).alias('mtd_ln_int_paid_amt'),
    (when((LKP_MASTER_OUT1.OPEN_CLOSE_CD == lit('C'), lit(0)).otherwise(when((LKP_MASTER_OUT1.BANK_SHR_BAL < lit(2)).otherwise(lit(0)).otherwise(when((Matdtnew1 <=
     AsOfDt)).otherwise((LKP_MASTER_OUT1.principal_balance))).otherwise((IFF(PiDueAmt > lit(15)))).otherwise((PiDueAmt)).otherwise(lit(0)))))).alias('pdue_pi_amt'),
    (when((LKP_MASTER_OUT1.OPEN_CLOSE_CD == lit('C'), None).otherwise(when((LKP_MASTER_OUT1.BANK_SHR_BAL < lit(2)).otherwise(None).otherwise(when((PiDueAmt < lit(
        15))).otherwise((None))).otherwise((IFF(OldRentDueDt <= LKP_MASTER_OUT1.DATA_DATE))).otherwise((OldRentDueDt)).otherwise(None))))).alias('pdue_pi_dt'),
    (RegUnfunded + LKP_MASTER_OUT1.CUR_BAL).alias('reg_exp_amt'),
    (when((((LKP_MASTER_OUT1.termination_date == None) | (Len(trim(LKP_MASTER_OUT1.termination_date))
     == lit(0)))), (TotPdueFeeAmt .cast(numeric))).otherwise(lit(0))).alias('tot_pdue_fee_amt'),
    (lit(None).cast(NullType())).alias('PDUE_PRIN_DT'),
    (lit(None).cast(NullType())).alias('PDUE_INT_DT'),
    LKP_MASTER_OUT1.extended_maturity_date.alias('MAT_DT'),
    (when(((LKP_MASTER_OUT1.BANK_SHR_BAL < lit(0)) & (RegBnkShrUnfd > lit(0)), RegBnkShrUnfd).otherwise(IFF((LKP_MASTER_OUT1.BANK_SHR_BAL > lit(0)) & (
        RegBnkShrUnfd < lit(0)))).otherwise((LKP_MASTER_OUT1.BANK_SHR_BAL)).otherwise(LKP_MASTER_OUT1.BANK_SHR_BAL + RegBnkShrUnfd))).alias('reg_bank_shr_exp_amt'),
    LKP_MASTER_OUT1.current_contract_receivable.alias(
        'current_contract_receivable'),
    LKP_MASTER_OUT1.ytd_interest_income.alias('ytd_interest_income'),
    LKP_MASTER_OUT1.ytd_earned_income_lease.alias('ytd_earned_income_lease'),
    LKP_MASTER_OUT1.ytd_earned_income_residual.alias(
        'ytd_earned_income_residual'),
    LKP_MASTER_OUT1.ltd_interest_income.alias('ltd_interest_income'),
    LKP_MASTER_OUT1.BANK_SHR_BAL.alias('BANK_SHR_BAL'),
    (when((trim(LKP_MASTER_OUT1.product_code) == lit('LOAN'), (when(((LKP_MASTER_OUT1.interest_ytd) IS NOT None).otherwise((LKP_MASTER_OUT1.interest_ytd)).otherwise(lit(0)))).otherwise((when(((LKP_MASTER_OUT1.ytd_earned_income_residual) IS NOT None)).otherwise(((LKP_MASTER_OUT1.ytd_earned_income_residual)))).otherwise((lit(0)) + (IFF((LKP_MASTER_OUT1.ytd_earned_income_lease) IS NOT None))).otherwise(((LKP_MASTER_OUT1.ytd_earned_income_lease))).otherwise(lit(0)))))).alias('ytd_int_paid_amt'),
    LKP_MASTER_OUT1.oldest_rent_due_date_contract_aging.alias(
        'oldest_rent_due_date_contract_aging')
).filter("ACCT_ID <> 0")

# Processing node ASPIRE_CREDIT_FCLTY_BAL_INTERMEDIATE, type TARGET
# COLUMN COUNT: 192

ASPIRE_CREDIT_FCLTY_BAL_INTERMEDIATE = LKP_MASTER_OUT2.select('*')
ASPIRE_CREDIT_FCLTY_BAL_INTERMEDIATE.write.format('csv').option('header', 'false').mode('overwrite').option('sep', ',').csv('{getArgument('DS_DIR')}/aspire/ASPIRE_CREDIT_FCLTY_BAL_INTERMEDIATE.ds')

# Processing node ASPIRE_CREDIT_FCLTY_BAL_INTERMEDIATE_test, type TARGET
# COLUMN COUNT: 46

ASPIRE_CREDIT_FCLTY_BAL_INTERMEDIATE_test = LKP_MASTER_OUT21.select('*')
ASPIRE_CREDIT_FCLTY_BAL_INTERMEDIATE_test.write.format('csv').option('header', 'false').mode('overwrite').option('sep', ',').csv('{getArgument('DS_DIR')}/aspire/ASPIRE_CREDIT_FCLTY_BAL_INTERMEDIATE_TEST1.ds')
