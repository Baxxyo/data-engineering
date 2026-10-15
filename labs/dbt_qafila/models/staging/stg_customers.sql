-- One row per customer account. Light cleaning only: types, normalized contact fields,
-- and a person-level key that links duplicate accounts of the same national ID.
with source as (
    select * from {{ source('raw', 'customers') }}
)

select
    customer_id,
    min(customer_id) over (partition by national_id)        as person_id,     -- oldest account of this person
    full_name,
    lower(trim(email))                                        as email,
    {{ normalize_ksa_mobile('mobile') }}                      as mobile_e164,
    national_id,
    iban,
    city,
    district,
    created_at::timestamp                                     as created_at,
    updated_at::timestamp                                     as updated_at,
    is_deleted = 1                                            as is_deleted
from source
