-- stg_t_kode67_kravhode

select kravhode_id from {{ source('pen', 't_kode67_kravhode') }}
