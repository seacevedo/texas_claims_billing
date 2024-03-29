-- Derived from int_header_union. The model is built by selecting only columns relating to employee data and a unique id is produced by hashing all columns.

with unique_employee as (
    select 
    distinct {{ dbt_utils.generate_surrogate_key(['EMPLOYEE_MAILING_CITY', 'EMPLOYEE_MAILING_STATE_CODE', 'EMPLOYEE_MAILING_POSTAL_CODE', 'EMPLOYEE_MAILING_COUNTRY', 'EMPLOYEE_DATE_OF_BIRTH', 'EMPLOYEE_GENDER_CODE', 'EMPLOYEE_MARITAL_STATUS_CODE', 'EMPLOYEE_DATE_OF_INJURY']) }} as EMPLOYEE_ID,
    EMPLOYEE_MAILING_CITY,
    EMPLOYEE_MAILING_STATE_CODE,
    EMPLOYEE_MAILING_POSTAL_CODE,
    EMPLOYEE_MAILING_COUNTRY,
    EMPLOYEE_DATE_OF_BIRTH,
    EMPLOYEE_GENDER_CODE,
    EMPLOYEE_MARITAL_STATUS_CODE,
    EMPLOYEE_DATE_OF_INJURY
    from {{ ref('int_header_union') }}
)

select *, current_timestamp() as transformed_timestamp
from unique_employee
