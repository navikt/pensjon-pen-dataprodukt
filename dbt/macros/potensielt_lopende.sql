{% macro potensielt_lopende(k_krav_gjelder) %}
    case
        when {{ k_krav_gjelder }} in (
            'EKSPORT',
            'ENDR_UTTAKSGRAD',
            'F_BH_BO_UTL',
            'F_BH_KUN_UTL',
            'F_BH_MED_UTL',
            'FAS_UTG_IO',
            'FORSTEG_BH',
            'GJ_RETT',
            'GOMR',
            'INNT_E',
            'KONV_AVVIK_G_BATCH',
            'KONVERTERING_MIN',
            'KONVERTERING',
            'MELLOMBH',
            'MTK',
            'REGULERING',
            'REVURD',
            'SLUTT_BH_UTL',
            'SLUTTBEH_KUN_UTL',
            'SOK_OKN_UG',
            'SOK_RED_UG',
            'SOK_UU',
            'SOK_YS',
            'UT_EO'
        )
    then '1'
    when {{ k_krav_gjelder }} in (
            'AFP_EO',
            'ANKE',
            'ERSTATNING',
            'ETTERGIV_GJELD',
            'GOD_OMSGSP',
            'HJLPBER_OVERG_UT',
            'INNT_KTRL',
            'KLAGE',
            'KONTROLL_3_17_A',
            'OMGJ_TILBAKE',
            'OVERF_OMSGSP',
            'SAK_OMKOST',
            'TILBAKEKR',
            'UT_VURDERING_EO',
            'UTSEND_AVTALELAND'
        )
        then '0'
    else '-1' -- todo: kjøre en test på at k_krav_gjelder gir 0 eller 1
    end
{% endmacro %}
