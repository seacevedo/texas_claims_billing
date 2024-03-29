/* Solely derived from int_header_union. In this dataset, three types of provider are present: billing (submits insurance claims for reimbursement), 
rendering (individual that renders the m edical service, usually some sort of doctor), and referring (person who directed the patient to the rendering provider). 
Here we consolidate the data from all three providers by seperating it out into Common Table Expressions (CTEs) and performing a union join on all three CTEs, selecting only unique rows. */

with billing_provider as (
    select
    BILL_ID,
    BILLING_PROVIDER_FEIN as PROVIDER_FEIN,
    BILLING_PROVIDER_FIRST_NAME as PROVIDER_FIRST_NAME,
    BILLING_PROVIDER_LAST_NAME as PROVIDER_LAST_NAME,
    BILLING_PROVIDER_ADDRESS as PROVIDER_ADDRESS,
    BILLING_PROVIDER_CITY as PROVIDER_CITY,
    BILLING_PROVIDER_STATE_CODE as PROVIDER_STATE_CODE,
    BILLING_PROVIDER_POSTAL_CODE as PROVIDER_POSTAL_CODE,
    BILLING_PROVIDER_COUNTRY as PROVIDER_COUNTRY,
    'Billing' as PROVIDER_TYPE
    from {{ ref('int_header_union') }}
),

rendering_provider as (
    select
    BILL_ID,
    RENDERING_BILL_PROVIDER_FEIN as PROVIDER_FEIN,
    RENDERING_BILL_PROVIDER_FIRST_NAME as PROVIDER_FIRST_NAME,
    RENDERING_BILL_PROVIDER_LAST_NAME as PROVIDER_LAST_NAME,
    RENDERING_BILL_PROVIDER_ADDRESS as PROVIDER_ADDRESS,
    RENDERING_BILL_PROVIDER_CITY as PROVIDER_CITY,
    RENDERING_BILL_PROVIDER_STATE as PROVIDER_STATE_CODE,
    RENDERING_BILL_PROVIDER_POSTAL as PROVIDER_POSTAL_CODE,
    RENDERING_BILL_PROVIDER_COUNTRY_CODE as PROVIDER_COUNTRY,
    'Rendering' as PROVIDER_TYPE
    from {{ ref('int_header_union') }}
),

referring_provider as (
    select 
    BILL_ID,
    REFERRING_PROVIDER_FEIN as PROVIDER_FEIN,
    REFERRING_PROVIDER_FIRST_NAME as PROVIDER_FIRST_NAME,
    REFERRING_PROVIDER_LAST_NAME as PROVIDER_LAST_NAME,
    null as PROVIDER_ADDRESS,
    null as PROVIDER_CITY,
    null as PROVIDER_STATE_CODE,
    null as PROVIDER_POSTAL_CODE,
    null as PROVIDER_COUNTRY,
    'Referring' as PROVIDER_TYPE
    from {{ ref('int_header_union') }}
),

union_provider as (
    select * from billing_provider
    union 
    select * from rendering_provider
    union 
    select * from referring_provider
),

unique_provider as (
    select *, row_number() over(partition by PROVIDER_FEIN order by PROVIDER_FIRST_NAME) as row_number
    from union_provider
)

select * exclude row_number, current_timestamp() as transformed_timestamp
from unique_provider
where row_number = 1


