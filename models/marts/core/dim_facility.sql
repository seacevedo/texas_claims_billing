with unique_facility as (
    select 
    distinct {{ dbt_utils.generate_surrogate_key(['FACILITY_FEIN', 'FACILITY_NAME', 'FACILITY_ADDRESS', 'FACILITY_CITY', 'FACILITY_STATE_CODE', 'FACILITY_POSTAL_CODE', 'FACILITY_COUNTRY_CODE', 'FACILITY_CODE']) }} as FACILITY_ID,
    FACILITY_FEIN,
    FACILITY_NAME,
    FACILITY_ADDRESS,
    FACILITY_CITY,
    FACILITY_STATE_CODE,
    FACILITY_POSTAL_CODE,
    FACILITY_COUNTRY_CODE,
    FACILITY_CODE
    from {{ ref('int_header_union') }}
)

select *, current_timestamp() as transformed_timestamp
from unique_facility