with inst_billing_detail as (
    select * from {{ref('stg_inst_billing_detail')}}
),

prof_billing_detail as (
    select * from {{ref('stg_prof_billing_detail')}}
)

select * from inst_billing_detail
union 
select * from prof_billing_detail