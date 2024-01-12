with unique_detail as (
    select 
    distinct BILL_ID,
    SERVICE_LINE_FROM_DATE,
    SERVICE_LINE_TO_DATE,
    TREATMENT_LINE_AUTHORIZATION_NUMBER,
    HCPCS_LINE_PROCEDURE_BILLED,
    HCPCS_LINE_PROCEDURE_PAID,
    PROCEDURE_DESCRIPTION
    from {{ ref('int_detail_union') }}
),

unique_header as (
    select
    distinct BILL_ID,
    ADMISSION_HOUR, 
    ADMISSION_DATE,
    ADMISSION_TYPE_CODE, 
    ADMITTING_DIAGNOSIS_CODE,
    PRINCIPAL_DIAGNOSIS_CODE
    from {{ ref('int_header_union') }}
)

select 
{{ dbt_utils.generate_surrogate_key(['unique_detail.SERVICE_LINE_FROM_DATE', 'unique_detail.SERVICE_LINE_TO_DATE', 'unique_detail.TREATMENT_LINE_AUTHORIZATION_NUMBER', 'unique_detail.HCPCS_LINE_PROCEDURE_BILLED', 'unique_detail.HCPCS_LINE_PROCEDURE_PAID', 'unique_detail.PROCEDURE_DESCRIPTION', 'unique_header.ADMISSION_HOUR', 'unique_header.ADMISSION_DATE', 'unique_header.ADMISSION_TYPE_CODE', 'unique_header.ADMITTING_DIAGNOSIS_CODE', 'unique_header.PRINCIPAL_DIAGNOSIS_CODE']) }} as SERVICE_ID,
unique_detail.SERVICE_LINE_FROM_DATE,
unique_detail.SERVICE_LINE_TO_DATE,
unique_detail.TREATMENT_LINE_AUTHORIZATION_NUMBER,
unique_detail.HCPCS_LINE_PROCEDURE_BILLED,
unique_detail.HCPCS_LINE_PROCEDURE_PAID,
unique_detail.PROCEDURE_DESCRIPTION,
unique_header.ADMISSION_HOUR, 
unique_header.ADMISSION_DATE,
unique_header.ADMISSION_TYPE_CODE, 
unique_header.ADMITTING_DIAGNOSIS_CODE,
unique_header.PRINCIPAL_DIAGNOSIS_CODE
from unique_detail
left join unique_header 
on unique_detail.BILL_ID = unique_header.BILL_ID