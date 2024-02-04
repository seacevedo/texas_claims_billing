/* Derived from dim_date, dim_employee, dim_facility, dim_provider, dim_service, and fct_claims_billing. The model is built by performing and inner join on all three tables. 
Unlike obt_claims_billing the grain is not redefined: rather it remains the same as fct_claims_billing, where each row corresponds to one line item within a bill. 
In this case you can think of the grain as a single procedure listed within the line item of a bill. */

with rendering_provider as (
    select * 
    from {{ ref('dim_provider') }}
    where PROVIDER_TYPE = 'Rendering'
),


joined_tbl as (
    select 
    {{ dbt_utils.generate_surrogate_key(['dim_facility.FACILITY_ID', 'dim_employee.EMPLOYEE_ID', 'rendering_provider.PROVIDER_ID', 'dim_service.SERVICE_ID', 'dim_date.DATE_ID']) }} as OUTCOME_ID,
    dim_facility.FACILITY_ID,
    FACILITY_FEIN,
    FACILITY_NAME,
    FACILITY_ADDRESS,
    FACILITY_CITY,
    FACILITY_STATE_CODE,
    FACILITY_POSTAL_CODE,
    FACILITY_COUNTRY_CODE,
    FACILITY_CODE,
    dim_employee.EMPLOYEE_ID,
    EMPLOYEE_MAILING_CITY,
    EMPLOYEE_MAILING_STATE_CODE,
    EMPLOYEE_MAILING_POSTAL_CODE,
    EMPLOYEE_MAILING_COUNTRY,
    EMPLOYEE_DATE_OF_BIRTH,
    EMPLOYEE_GENDER_CODE,
    EMPLOYEE_MARITAL_STATUS_CODE,
    EMPLOYEE_DATE_OF_INJURY,
    rendering_provider.PROVIDER_ID,
    PROVIDER_FEIN,
    PROVIDER_FIRST_NAME,
    PROVIDER_LAST_NAME,
    PROVIDER_ADDRESS,
    PROVIDER_CITY,
    PROVIDER_STATE_CODE,
    PROVIDER_POSTAL_CODE,
    PROVIDER_COUNTRY,
    PROVIDER_TYPE,
    dim_service.SERVICE_ID,
    SERVICE_LINE_FROM_DATE,
    SERVICE_LINE_TO_DATE,
    TREATMENT_LINE_AUTHORIZATION_NUMBER,
    HCPCS_LINE_PROCEDURE_BILLED,
    HCPCS_LINE_PROCEDURE_PAID,
    PROCEDURE_DESCRIPTION,
    ADMISSION_HOUR, 
    ADMISSION_DATE,
    ADMISSION_TYPE_CODE, 
    ADMITTING_DIAGNOSIS_CODE,
    PRINCIPAL_DIAGNOSIS_CODE,
    dim_date.DATE_ID,
    BILL_SELECTION_DATE,
    YEAR,
    YEAR_WEEK,
    YEAR_DAY,
    FISCAL_YEAR,
    FISCAL_QTR,
    MONTH,
    MONTH_NAME,
    WEEK_DAY,
    DAY_NAME
    from {{ ref('fct_claims_billing') }} 
    join {{ ref('dim_facility') }} 
    on dim_facility.FACILITY_ID = fct_claims_billing.FACILITY_ID
    join {{ ref('dim_employee') }}
    on dim_employee.EMPLOYEE_ID = fct_claims_billing.EMPLOYEE_ID
    join rendering_provider
    on rendering_provider.PROVIDER_ID = fct_claims_billing.PROVIDER_ID
    join {{ ref('dim_service') }} 
    on dim_service.SERVICE_ID = fct_claims_billing.SERVICE_ID
    join {{ ref('dim_date') }} 
    on dim_date.DATE_ID = fct_claims_billing.DATE_ID 
),

unique_join as (
    select *, row_number() over(partition by OUTCOME_ID order by BILL_SELECTION_DATE) as row_number
    from joined_tbl
)

select * exclude row_number, current_timestamp() as transformed_timestamp
from unique_join
where row_number = 1
