with inst_billing_detail as (
    select * from {{ref('stg_inst_billing_detail')}}
),

prof_billing_detail as (
    select * from {{ref('stg_prof_billing_detail')}}
),

union_detail as (
    select * from inst_billing_detail
    union 
    select * from prof_billing_detail
),

unique_detail as (
    select *, row_number() over(partition by BILL_DETAIL_ID order by BILL_SELECTION_DATE) as row_number
    from union_detail
)

select * exclude row_number, current_timestamp() as transformed_timestamp
from unique_detail
where row_number = 1

