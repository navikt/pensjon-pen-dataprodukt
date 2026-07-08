{% macro beregn_behandling_metode(
    k_krav_s,
    ferdigbehandlet_tid,
    opprettet_av_kode,
    attestert_av_kode,
    endret_av_kode,
    k_krav_arsak_t,
    k_krav_gjelder,
    k_behandling_t

) %}
    case
        
        
        -- when rnk=1 and {{ opprettet_av_kode }} is not null 
        -- and not ( rnk_desc = 1 and ({{ ferdigbehandlet_tid }} is not null or {{ k_krav_s }} in ('FERDIG', 'AVBRUTT'))) 
        --     then 'OPPRETTET_AV_' || {{ opprettet_av_kode }} || '_' || nvl({{ k_behandling_t }}, 'UKJ')
        -- 
        -- when ( {{ attestert_av_kode }} = 'NULL' or {{ attestert_av_kode }} is null ) 
        -- and rnk_desc = 1 and ( {{ ferdigbehandlet_tid }} is not null or {{ k_krav_s }} in ('FERDIG', 'AVBRUTT') )
        --     then coalesce({{ opprettet_av_kode }}, 'UKJENT') || '_UKJENT_' || coalesce({{ k_behandling_t }}, 'UKJ')
        -- 
        -- when rnk!=1 and not (rnk_desc=1 and ({{ ferdigbehandlet_tid }} is not null or {{ k_krav_s }} in ('FERDIG', 'AVBRUTT')))
        --     then 'OPPRETTET_AV_' || coalesce({{ opprettet_av_kode }}, 'UKJENT') || '_ENDRET_AV_' || coalesce({{ endret_av_kode }}, 'UKJENT')

        when {{ opprettet_av_kode }} = 'SYSTEM' and {{ attestert_av_kode }} = 'SYSTEM' and {{ k_krav_arsak_t }} = 'ALDERSOVERGANG'
            and {{ k_krav_gjelder }} = 'FORSTEG_BH'
            then 'ALDERSOVERGANG_AUTO'
        when {{ opprettet_av_kode }} = 'SYSTEM' and {{ endret_av_kode }} = 'SYSTEM' and {{ attestert_av_kode }} = 'SYSTEM'
            and {{ k_krav_gjelder }} in ('REGULERING', 'REVURD')
            then 'HELAUTO'
        when {{ opprettet_av_kode }} = 'SYSTEM' and {{ attestert_av_kode }} = 'SYSTEM'
            then 'SYSTEM_SYSTEM_' || coalesce({{ k_behandling_t }}, 'UKJ')
        when {{ opprettet_av_kode }} in ('SELVB_ANNET', 'BRUKER') and {{ attestert_av_kode }} = 'SYSTEM'
            then {{ opprettet_av_kode }} || '_SYSTEM_AUTO'
        when {{ opprettet_av_kode }} in ('SELVB_ANNET', 'BRUKER') and {{ attestert_av_kode }} = 'SAKSBEH'
            then {{ opprettet_av_kode }} || '_SAKSBEH' || '_' || decode(coalesce({{ k_behandling_t }}, 'UKJ'), 'AUTO', 'MAN', coalesce({{ k_behandling_t }}, 'UKJ'))
        when {{ opprettet_av_kode }} = 'SAKSBEH' and {{ attestert_av_kode }} = 'SAKSBEH'
            then 'SAKSBEH_SAKSBEH' || '_' || decode(coalesce({{ k_behandling_t }}, 'UKJ'), 'AUTO', 'MAN', coalesce({{ k_behandling_t }}, 'UKJ'))
        when {{ opprettet_av_kode }} = 'SAKSBEH' and {{ attestert_av_kode }} = 'SYSTEM' and coalesce({{ k_behandling_t }}, 'x') = 'MAN'
            and {{ k_krav_gjelder }} in ('KLAGE', 'ANKE')
            then 'SAKSBEH_SYSTEM_MAN'
        when {{ opprettet_av_kode }} = 'SAKSBEH' and {{ attestert_av_kode }} = 'SYSTEM'
            then 'SAKSBEH_SYSTEM_AUTO'
        when {{ opprettet_av_kode }} = 'SYSTEM' and {{ attestert_av_kode }} = 'SAKSBEH'
            then 'SYSTEM_SAKSBEH' || '_' || decode(coalesce({{ k_behandling_t }}, 'UKJ'), 'AUTO', 'MAN', coalesce({{ k_behandling_t }}, 'UKJ'))
        when {{ attestert_av_kode }} in ('SELVB_ANNET', 'BRUKER')
            then {{ opprettet_av_kode }} || '_' || {{ attestert_av_kode }} || '_AUTO'
        when {{ opprettet_av_kode }} is null
            then 'UKJENT_OPPRETTET' || '_' || coalesce({{ k_behandling_t }}, 'UKJ')
        else 'UKJENT'
    end
{% endmacro %}
