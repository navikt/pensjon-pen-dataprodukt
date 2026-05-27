{{
  config(
    materialized = 'table',
    tags = ['analyse']
    )
}}

select
    sysdate                                                   as sysdate_utc,
    current_date                                              as current_date_utc,
    systimestamp                                              as systimestamp_utc,
    systimestamp at time zone 'Europe/Oslo'                   as systimestamp_oslo,
    cast(systimestamp at time zone 'Europe/Oslo' as date)     as oslo_as_date,
    from_tz(cast(sysdate as timestamp), 'UTC')
        at time zone 'Europe/Oslo'                            as from_tz_oslo
from dual