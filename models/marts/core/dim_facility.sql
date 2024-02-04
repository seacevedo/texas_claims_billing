-- Derived from int_header_union. The model is built by selecting only columns relating to medical facility data and a unique id is produced by hashing all columns.

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
