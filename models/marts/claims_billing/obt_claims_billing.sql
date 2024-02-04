/* Derived from dim_insurer, dim_date, and fct_claims_billing. We first perform an inner join on all three tables, and select only unique rows. 
We then redefine the grain of our resulting table by summing the total bill charge amount, payment amount, days charged, and days paid for each day. */

with joined_tbl as (
    select 
    BILL_DETAIL_ID,
    TOTAL_AMOUNT_PAID_PER_LINE,
    TOTAL_CHARGE_PER_LINE,
    DAYS_UNITS_BILLED,
    DAYS_UNITS_PAID,
    DAYS_UNITS_CODE,
    dim_insurer.INSURER_ID,
    INSURER_FEIN,
    INSURER_POSTAL_CODE,
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
    join {{ ref('dim_insurer') }} 
    on dim_insurer.INSURER_ID = fct_claims_billing.INSURER_ID
    join {{ ref('dim_date') }} 
    on dim_date.DATE_ID = fct_claims_billing.DATE_ID 
),

unique_join as (
    select *, row_number() over(partition by BILL_DETAIL_ID order by BILL_SELECTION_DATE) as row_number
    from joined_tbl
)

select BILL_SELECTION_DATE, 
    YEAR,
    YEAR_WEEK,
    YEAR_DAY,
    FISCAL_YEAR,
    FISCAL_QTR,
    MONTH,
    MONTH_NAME,
    WEEK_DAY,
    DAY_NAME,
    INSURER_ID, 
    INSURER_FEIN,
    sum(TOTAL_AMOUNT_PAID_PER_LINE) as TOTAL_AMOUNT_PAID_PER_DAY, 
    sum(TOTAL_CHARGE_PER_LINE) as TOTAL_CHARGES_PER_DAY, 
    sum(DAYS_UNITS_BILLED) as TOTAL_DAYS_UNITS_BILLED_PER_DAY,
    sum(coalesce(DAYS_UNITS_PAID, 0)) as TOTAL_DAYS_UNITS_PAID_PER_DAY,
    current_timestamp() as transformed_timestamp
from unique_join
where row_number = 1
group by BILL_SELECTION_DATE, YEAR, YEAR_WEEK, YEAR_DAY, FISCAL_YEAR, FISCAL_QTR, MONTH, MONTH_NAME, WEEK_DAY, DAY_NAME, INSURER_ID, INSURER_FEIN
