with detail as (
    select 
    BILL_ID,
    SERVICE_LINE_FROM_DATE,
    SERVICE_LINE_TO_DATE,
    TREATMENT_LINE_AUTHORIZATION_NUMBER,
    HCPCS_LINE_PROCEDURE_BILLED,
    HCPCS_LINE_PROCEDURE_PAID,
    PROCEDURE_DESCRIPTION
    from {{ ref('int_detail_union') }}
),

header as (
    select
    BILL_ID,
    ADMISSION_HOUR, 
    ADMISSION_DATE,
    ADMISSION_TYPE_CODE, 
    ADMITTING_DIAGNOSIS_CODE,
    PRINCIPAL_DIAGNOSIS_CODE
    from {{ ref('int_header_union') }}
),

joined_tbl as (
    select 
    {{ dbt_utils.generate_surrogate_key(['detail.SERVICE_LINE_FROM_DATE', 'detail.SERVICE_LINE_TO_DATE', 'detail.TREATMENT_LINE_AUTHORIZATION_NUMBER', 'detail.HCPCS_LINE_PROCEDURE_BILLED', 'detail.HCPCS_LINE_PROCEDURE_PAID', 'detail.PROCEDURE_DESCRIPTION', 'header.ADMISSION_HOUR', 'header.ADMISSION_DATE', 'header.ADMISSION_TYPE_CODE', 'header.ADMITTING_DIAGNOSIS_CODE', 'header.PRINCIPAL_DIAGNOSIS_CODE']) }} as SERVICE_ID,
    detail.SERVICE_LINE_FROM_DATE,
    detail.SERVICE_LINE_TO_DATE,
    detail.TREATMENT_LINE_AUTHORIZATION_NUMBER,
    detail.HCPCS_LINE_PROCEDURE_BILLED,
    detail.HCPCS_LINE_PROCEDURE_PAID,
    detail.PROCEDURE_DESCRIPTION,
    header.ADMISSION_HOUR, 
    header.ADMISSION_DATE,
    header.ADMISSION_TYPE_CODE, 
    header.ADMITTING_DIAGNOSIS_CODE,
    header.PRINCIPAL_DIAGNOSIS_CODE
    from detail
    left join header 
    on detail.BILL_ID = header.BILL_ID
),

unique_join as (
    select *, row_number() over(partition by SERVICE_ID order by SERVICE_LINE_FROM_DATE) as row_number
    from joined_tbl
)

select * exclude row_number, current_timestamp() as transformed_timestamp
from unique_join
where row_number = 1