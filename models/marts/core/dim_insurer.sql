-- Derived from int_header_union. The model is built by selecting only columns relating to insurer data and a unique id is produced by hashing all columns.

with unique_insurer as (
    select 
    distinct {{ dbt_utils.generate_surrogate_key(['INSURER_FEIN', 'INSURER_POSTAL_CODE', 'DATE_INSURER_RECEIVED_BILL', 'DATE_INSURER_PAID_BILL']) }} as INSURER_ID,
    INSURER_FEIN,
    INSURER_POSTAL_CODE,
    DATE_INSURER_RECEIVED_BILL,
    DATE_INSURER_PAID_BILL
    from {{ ref('int_header_union') }}
)

select *, current_timestamp() as transformed_timestamp
from unique_insurer
