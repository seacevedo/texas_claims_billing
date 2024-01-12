with unique_detail as (
    select 
    distinct BILL_ID,
    BILL_DETAIL_ID,
    LINE_NUMBER,
    SERVICE_LINE_FROM_DATE,
    SERVICE_LINE_TO_DATE,
    TREATMENT_LINE_AUTHORIZATION_NUMBER,
    HCPCS_LINE_PROCEDURE_BILLED,
    HCPCS_LINE_PROCEDURE_PAID,
    PROCEDURE_DESCRIPTION,
    TOTAL_AMOUNT_PAID_PER_LINE,
    TOTAL_CHARGE_PER_LINE,
    DAYS_UNITS_BILLED,
    DAYS_UNITS_PAID,
    DAYS_UNITS_CODE
    from {{ ref('int_detail_union') }}
),

unique_header as (
    select
    distinct BILL_ID,
    BILL_SELECTION_DATE,
    FACILITY_FEIN,
    FACILITY_NAME,
    FACILITY_ADDRESS,
    FACILITY_CITY,
    FACILITY_STATE_CODE,
    FACILITY_POSTAL_CODE,
    FACILITY_COUNTRY_CODE,
    FACILITY_CODE,
    INSURER_FEIN,
    INSURER_POSTAL_CODE,
    DATE_INSURER_RECEIVED_BILL,
    DATE_INSURER_PAID_BILL,
    EMPLOYEE_MAILING_CITY,
    EMPLOYEE_MAILING_STATE_CODE,
    EMPLOYEE_MAILING_POSTAL_CODE,
    EMPLOYEE_MAILING_COUNTRY,
    EMPLOYEE_DATE_OF_BIRTH,
    EMPLOYEE_GENDER_CODE,
    EMPLOYEE_MARITAL_STATUS_CODE,
    EMPLOYEE_DATE_OF_INJURY,
    EMPLOYER_FEIN,
    EMPLOYER_PHYSICAL_CITY,
    EMPLOYER_PHYSICAL_STATE_CODE,
    EMPLOYER_PHYSICAL_POSTAL,
    ADMISSION_HOUR,
    ADMISSION_DATE,
    ADMISSION_TYPE_CODE,
    ADMITTING_DIAGNOSIS_CODE,
    PRINCIPAL_DIAGNOSIS_CODE,
    BILL_TYPE 
    from {{ ref('int_header_union') }}
),

unique_provider as (
    select 
    distinct BILL_ID,
    PROVIDER_FEIN,
    PROVIDER_FIRST_NAME,
    PROVIDER_LAST_NAME,
    PROVIDER_ADDRESS,
    PROVIDER_CITY,
    PROVIDER_STATE_CODE,
    PROVIDER_POSTAL_CODE,
    PROVIDER_COUNTRY,
    PROVIDER_TYPE
    from {{ ref('int_provider_consolidate') }}
),

joined_tbls as (
    select 
    {{ dbt_utils.generate_surrogate_key(['FACILITY_FEIN', 'FACILITY_NAME', 'FACILITY_ADDRESS', 'FACILITY_CITY', 'FACILITY_STATE_CODE', 'FACILITY_POSTAL_CODE', 'FACILITY_COUNTRY_CODE', 'FACILITY_CODE']) }} as FACILITY_ID,
    {{ dbt_utils.generate_surrogate_key(['INSURER_FEIN', 'INSURER_POSTAL_CODE', 'DATE_INSURER_RECEIVED_BILL', 'DATE_INSURER_PAID_BILL']) }} as INSURER_ID,
    {{ dbt_utils.generate_surrogate_key(['EMPLOYEE_MAILING_CITY', 'EMPLOYEE_MAILING_STATE_CODE', 'EMPLOYEE_MAILING_POSTAL_CODE', 'EMPLOYEE_MAILING_COUNTRY', 'EMPLOYEE_DATE_OF_BIRTH', 'EMPLOYEE_GENDER_CODE', 'EMPLOYEE_MARITAL_STATUS_CODE', 'EMPLOYEE_DATE_OF_INJURY']) }} as EMPLOYEE_ID,
    {{ dbt_utils.generate_surrogate_key(['EMPLOYER_FEIN', 'EMPLOYER_PHYSICAL_CITY', 'EMPLOYER_PHYSICAL_STATE_CODE', 'EMPLOYER_PHYSICAL_POSTAL']) }} as EMPLOYER_ID,
    {{ dbt_utils.generate_surrogate_key(['SERVICE_LINE_FROM_DATE', 'SERVICE_LINE_TO_DATE', 'TREATMENT_LINE_AUTHORIZATION_NUMBER', 'HCPCS_LINE_PROCEDURE_BILLED', 'HCPCS_LINE_PROCEDURE_PAID', 'PROCEDURE_DESCRIPTION', 'ADMISSION_HOUR', 'ADMISSION_DATE', 'ADMISSION_TYPE_CODE', 'ADMITTING_DIAGNOSIS_CODE', 'PRINCIPAL_DIAGNOSIS_CODE']) }} as SERVICE_ID,
    {{ dbt_utils.generate_surrogate_key(['BILL_SELECTION_DATE']) }} as DATE_ID,
    unique_header.BILL_ID,
    unique_detail.BILL_DETAIL_ID,
    unique_detail.LINE_NUMBER,
    unique_detail.TOTAL_AMOUNT_PAID_PER_LINE,
    unique_detail.TOTAL_CHARGE_PER_LINE,
    unique_detail.DAYS_UNITS_BILLED,
    unique_detail.DAYS_UNITS_PAID,
    unique_detail.DAYS_UNITS_CODE,
    unique_header.BILL_TYPE 
    from unique_detail
    left join unique_header 
    on unique_detail.BILL_ID = unique_header.BILL_ID
)

select 
joined_tbls.FACILITY_ID,
joined_tbls.INSURER_ID,
joined_tbls.EMPLOYEE_ID,
joined_tbls.EMPLOYER_ID,
{{ dbt_utils.generate_surrogate_key(['PROVIDER_FEIN', 'PROVIDER_FIRST_NAME', 'PROVIDER_LAST_NAME', 'PROVIDER_ADDRESS', 'PROVIDER_CITY', 'PROVIDER_STATE_CODE', 'PROVIDER_POSTAL_CODE', 'PROVIDER_COUNTRY', 'PROVIDER_TYPE']) }} as PROVIDER_ID,
joined_tbls.SERVICE_ID,
joined_tbls.DATE_ID,
joined_tbls.BILL_ID,
joined_tbls.BILL_DETAIL_ID,
joined_tbls.LINE_NUMBER,
joined_tbls.TOTAL_AMOUNT_PAID_PER_LINE,
joined_tbls.TOTAL_CHARGE_PER_LINE,
joined_tbls.DAYS_UNITS_BILLED,
joined_tbls.DAYS_UNITS_PAID,
joined_tbls.DAYS_UNITS_CODE,
joined_tbls.BILL_TYPE,
current_timestamp() as transformed_timestamp
from joined_tbls
left join unique_provider
on joined_tbls.BILL_ID = unique_provider.BILL_ID