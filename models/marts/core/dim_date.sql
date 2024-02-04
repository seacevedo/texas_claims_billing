-- Derived from int_detail_union. Unique dates are selected and data like month, day, quarter, etc are extracted from each date and a unique id is produced by hashing BILL_SELECTION_DATE.


with unique_dates as (
    select distinct BILL_SELECTION_DATE from {{ ref('int_detail_union') }}
)


select
  {{ dbt_utils.generate_surrogate_key(['BILL_SELECTION_DATE']) }} as DATE_ID,
  BILL_SELECTION_DATE as BILL_SELECTION_DATE,
  extract(YEAR from BILL_SELECTION_DATE) as YEAR,
  extract(WEEK from BILL_SELECTION_DATE) as YEAR_WEEK,
  extract(DAY from BILL_SELECTION_DATE)  as YEAR_DAY,
  extract(YEAR from BILL_SELECTION_DATE) as FISCAL_YEAR,
  quarter(BILL_SELECTION_DATE) as FISCAL_QTR,
  extract(MONTH from BILL_SELECTION_DATE) as MONTH,
  monthname(BILL_SELECTION_DATE) as MONTH_NAME,
  dayofweek(BILL_SELECTION_DATE) as WEEK_DAY,
  dayname(BILL_SELECTION_DATE) as DAY_NAME,
  current_timestamp() as transformed_timestamp
from unique_dates 
order by BILL_SELECTION_DATE
