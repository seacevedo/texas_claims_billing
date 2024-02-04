-- Derived from int_provider_consolidate. The model is built by selecting only columns relating to provider data and a unique id is produced by hashing all columns.

with unique_provider as (
    select
    {{ dbt_utils.generate_surrogate_key(['PROVIDER_FEIN', 'PROVIDER_FIRST_NAME', 'PROVIDER_LAST_NAME', 'PROVIDER_ADDRESS', 'PROVIDER_CITY', 'PROVIDER_STATE_CODE', 'PROVIDER_POSTAL_CODE', 'PROVIDER_COUNTRY', 'PROVIDER_TYPE']) }} as PROVIDER_ID,
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
)

select *, current_timestamp() as transformed_timestamp 
from unique_provider
