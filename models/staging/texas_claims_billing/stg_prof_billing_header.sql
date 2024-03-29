-- Derived from the PROF_BILLING_HEADER source table. Type casting is applied where columns are cast to appropriate data type and dates are properly formatted.

with prof_billing_header as (
    select 
    cast(BILL_ID as varchar) as BILL_ID,
    to_date(cast(BILL_SELECTION_DATE as varchar), 'yyyy-mm-dd hh:mi:ss.ff9') as BILL_SELECTION_DATE,
    cast(BILL_TYPE as varchar) as BILL_TYPE,
    cast(INSURER_FEIN as varchar) as INSURER_FEIN,
    cast(INSURER_POSTAL_CODE as varchar) as INSURER_POSTAL_CODE,
    to_date(cast(DATE_INSURER_RECEIVED_BILL as varchar), 'yyyy-mm-dd hh:mi:ss.ff9') as DATE_INSURER_RECEIVED_BILL,
    to_date(cast(DATE_INSURER_PAID_BILL as varchar), 'yyyy-mm-dd hh:mi:ss.ff9') as DATE_INSURER_PAID_BILL,
    cast(FACILITY_FEIN as varchar) as FACILITY_FEIN,
    cast(FACILITY_NAME as varchar) as FACILITY_NAME,
    cast(FACILITY_PRIMARY_ADDRESS as varchar) as FACILITY_ADDRESS,
    cast(FACILITY_CITY as varchar) as FACILITY_CITY,
    cast(FACILITY_STATE_CODE as varchar) as FACILITY_STATE_CODE,
    cast(FACILITY_POSTAL_CODE as varchar) as FACILITY_POSTAL_CODE,
    cast(FACILITY_COUNTRY_CODE as varchar) as FACILITY_COUNTRY_CODE,
    cast(EMPLOYER_FEIN as varchar) as EMPLOYER_FEIN,
    cast(EMPLOYER_PHYSICAL_CITY as varchar) as EMPLOYER_PHYSICAL_CITY,
    cast(EMPLOYER_PHYSICAL_STATE_CODE as varchar) as EMPLOYER_PHYSICAL_STATE_CODE,
    cast(EMPLOYER_PHYSICAL_POSTAL as varchar) as EMPLOYER_PHYSICAL_POSTAL,
    cast(BILLING_PROVIDER_FEIN as varchar) as BILLING_PROVIDER_FEIN,
    cast(RENDERING_BILL_PROVIDER_FEIN as varchar) as RENDERING_BILL_PROVIDER_FEIN,
    cast(REFERRING_PROVIDER_FEIN as varchar) as REFERRING_PROVIDER_FEIN,
    cast(BILLING_PROVIDER_FIRST_NAME as varchar) as BILLING_PROVIDER_FIRST_NAME,
    cast(RENDERING_BILL_PROVIDER_FIRST as varchar) as RENDERING_BILL_PROVIDER_FIRST_NAME,
    cast(REFERRING_PROVIDER_FIRST as varchar) as REFERRING_PROVIDER_FIRST_NAME,
    cast(BILLING_PROVIDER_LAST_NAME as varchar) as BILLING_PROVIDER_LAST_NAME,
    cast(RENDERING_BILL_PROVIDER_LAST as varchar) as RENDERING_BILL_PROVIDER_LAST_NAME,
    cast(REFERRING_PROVIDER_LAST_NAME as varchar) as REFERRING_PROVIDER_LAST_NAME,
    cast(BILLING_PROVIDER_PRIMARY_1 as varchar) as BILLING_PROVIDER_ADDRESS,
    cast(RENDERING_BILL_PROVIDER_1 as varchar) as RENDERING_BILL_PROVIDER_ADDRESS,
    cast(BILLING_PROVIDER_CITY as varchar) as BILLING_PROVIDER_CITY,
    cast(RENDERING_BILL_PROVIDER_CITY as varchar) as RENDERING_BILL_PROVIDER_CITY,
    cast(BILLING_PROVIDER_STATE_CODE as varchar) as BILLING_PROVIDER_STATE_CODE,
    cast(RENDERING_BILL_PROVIDER_STATE as varchar) as RENDERING_BILL_PROVIDER_STATE,
    cast(BILLING_PROVIDER_POSTAL_CODE as varchar) as BILLING_PROVIDER_POSTAL_CODE,
    cast(RENDERING_BILL_PROVIDER_POSTAL as varchar) as RENDERING_BILL_PROVIDER_POSTAL,
    cast(BILLING_PROVIDER_COUNTRY as varchar) as BILLING_PROVIDER_COUNTRY,
    cast(RENDERING_BILL_PROVIDER_3 as varchar) as RENDERING_BILL_PROVIDER_COUNTRY_CODE,
    cast(EMPLOYEE_MAILING_CITY as varchar) as EMPLOYEE_MAILING_CITY,
    cast(EMPLOYEE_MAILING_STATE_CODE as varchar) as EMPLOYEE_MAILING_STATE_CODE,
    cast(EMPLOYEE_MAILING_POSTAL_CODE as varchar) as EMPLOYEE_MAILING_POSTAL_CODE,
    cast(EMPLOYEE_MAILING_COUNTRY as varchar) as EMPLOYEE_MAILING_COUNTRY,
    to_date(cast(EMPLOYEE_DATE_OF_BIRTH as varchar), 'yyyy-mm-dd hh:mi:ss.ff9') as EMPLOYEE_DATE_OF_BIRTH,
    cast(EMPLOYEE_GENDER_CODE as char) as EMPLOYEE_GENDER_CODE, 
    cast(EMPLOYEE_MARITAL_STATUS_CODE as char) as EMPLOYEE_MARITAL_STATUS_CODE, 
    to_date(cast(EMPLOYEE_DATE_OF_INJURY as varchar), 'yyyy-mm-dd hh:mi:ss.ff9') as EMPLOYEE_DATE_OF_INJURY 
    from {{ source('texas_claims_src', 'PROF_BILLING_HEADER') }}
),

unique_source as (
    select *, row_number() over(partition by BILL_ID order by BILL_SELECTION_DATE DESC) as row_number
    from prof_billing_header
)

select * exclude row_number, current_timestamp() as transformed_timestamp 
from unique_source
where row_number = 1
