-- Derived from int_header_union. The model is built by selecting only columns relating to employer data and a unique id is produced by hashing all columns.

with unique_employer as (
    select 
    distinct {{ dbt_utils.generate_surrogate_key(['EMPLOYER_FEIN', 'EMPLOYER_PHYSICAL_CITY', 'EMPLOYER_PHYSICAL_STATE_CODE', 'EMPLOYER_PHYSICAL_POSTAL']) }} as EMPLOYER_ID,
    EMPLOYER_FEIN,
    EMPLOYER_PHYSICAL_CITY,
    EMPLOYER_PHYSICAL_STATE_CODE,
    EMPLOYER_PHYSICAL_POSTAL
    from {{ ref('int_header_union') }}
)

select *, current_timestamp() as transformed_timestamp
from unique_employer
