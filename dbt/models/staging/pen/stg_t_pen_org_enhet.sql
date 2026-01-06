-- stg_t_pen_org_enhet

select
    pen_org_enhet_id,
    org_enhet_id_fk
from {{ source('pen', 't_pen_org_enhet') }}
