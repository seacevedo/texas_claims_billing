

with inst_billing_detail as (
    select 
    cast(BILL_DETAIL_ID as varchar) as BILL_DETAIL_ID,
    cast(BILL_ID as varchar) as BILL_ID,
    cast(BILL_SELECTION_DATE as date) as BILL_SELECTION_DATE,
    cast(LINE_NUMBER as varchar) as LINE_NUMBER,
    cast(TOTAL_AMOUNT_PAID_PER_LINE as decimal) as TOTAL_AMOUNT_PAID_PER_LINE,
    cast(TOTAL_CHARGE_PER_LINE as decimal) as TOTAL_CHARGE_PER_LINE,
    cast(DAYS_UNITS_BILLED as integer) as DAYS_UNITS_BILLED,
    cast(DAYS_UNITS_PAID as integer) as DAYS_UNITS_PAID,
    cast(DAYS_UNITS_CODE as varchar) as DAYS_UNITS_CODE,
    cast(SERVICE_LINE_FROM_DATE as date) as SERVICE_LINE_FROM_DATE,
    cast(SERVICE_LINE_TO_DATE as date) as SERVICE_LINE_TO_DATE,
    cast(TREATMENT_LINE_AUTHORIZATION as char) as TREATMENT_LINE_AUTHORIZATION_NUMBER,
    cast(HCPCS_LINE_PROCEDURE_BILLED as varchar) as HCPCS_LINE_PROCEDURE_BILLED,
    cast(HCPCS_LINE_PROCEDURE_PAID as varchar) as HCPCS_LINE_PROCEDURE_PAID,
    cast(PROCEDURE_DESCRIPTION as varchar) as PROCEDURE_DESCRIPTION
    from {{ source('texas_claims_src', 'INST_BILLING_DETAIL') }}
)

select *, current_timestamp() as transformed_timestamp 
from inst_billing_detail