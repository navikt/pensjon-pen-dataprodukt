{% macro dekode(column_name) %}
    {% set column_parts = column_name.split('.') %}
    {% set column_lower = column_parts[-1] | lower %}
    
    {% if column_lower == 'k_afp_t' %}
        {{ dekode_k_afp_t(column_name) }}
    {% elif column_lower == 'k_bereg_metode_t' %}
        {{ dekode_k_bereg_metode_t(column_name) }}
    {% elif column_lower == 'k_grnl_rolle_t' %}
        {{ dekode_k_grnl_rolle_t(column_name) }}
    {% elif column_lower == 'k_minstepen_niva' %}
        {{ dekode_k_minstepen_niva(column_name) }}
    {% elif column_lower == 'k_regelverk_t' %}
        {{ dekode_k_regelverk_t(column_name) }}
    {% elif column_lower == 'k_sak_t' %}
        {{ dekode_k_sak_t(column_name) }}
    {% elif column_lower == 'k_sivilstand_t' %}
        {{ dekode_k_sivilstand_t(column_name) }}
    {% elif column_lower == 'k_vedtak_s' %}
        {{ dekode_k_vedtak_s(column_name) }}
    {% elif column_lower == 'k_vedtak_t' %}
        {{ dekode_k_vedtak_t(column_name) }}
    {% else %}
        {{ column_lower }}
    {% endif %}
{% endmacro %}

{% macro dekode_k_afp_t(column_name) %}
    case {{ column_name }}
        when 'AFPKOM' then 'AFP - Kommunalsektor'
        when 'AFPSTAT' then 'AFP - Stat'
        when 'FINANS' then 'Finansnæringen'
        when 'KONV_K' then 'Konvertert privat'
        when 'KONV_O' then 'Konvertert offentlig'
        when 'LONHO' then 'LO/NHO - ordningen'
        when 'NAVO' then 'Spekter'
        else {{ column_name }}
    end
{% endmacro %}

{% macro dekode_k_bereg_metode_t(column_name) %}
    case {{ column_name }}
        when 'AUSTRALIA' then 'Australia pro rata'
        when 'CANADA' then 'Canada pro rata'
        when 'CHILE' then 'Chile pro rata'
        when 'EOS' then 'EØS pro rata'
        when 'FOLKETRYGD' then 'Folketrygd'
        when 'INDIA' then 'India pro rata'
        when 'ISRAEL' then 'Israel pro rata'
        when 'NORDISK' then 'Nordisk konvensjon'
        when 'PRORATA' then 'Bilateral avtale'
        when 'SOR_KOREA' then 'Sør-Korea pro rata'
        when 'SVEITS' then 'Sveits pro rata'
        when 'USA' then 'USA pro rata'
        else {{ column_name }}
    end
{% endmacro %}

{% macro dekode_k_grnl_rolle_t(column_name) %}
    case {{ column_name }}
        when 'MEDMOR' then 'Medmor'
        when 'ABARN' then 'Avdødes barn'
        when 'AEKTEF' then 'Avdødes ektefelle'
        when 'AFBARN' then 'Avdødes fosterbarn'
        when 'APARTNER' then 'Avdødes partner'
        when 'ASAMBO' then 'Avdødes samboer'
        when 'AVDOD' then 'Avdød'
        when 'BARN' then 'Barn'
        when 'EKTEF' then 'Ektefelle'
        when 'FAR' then 'Far'
        when 'FBARN' then 'Fosterbarn'
        when 'MOR' then 'Mor'
        when 'PARTNER' then 'Partner'
        when 'SAMBO' then 'Samboer'
        when 'SOKER' then 'Bruker'
        when 'SOSKEN' then 'Søsken'
        else {{ column_name }}
    end
{% endmacro %}

{% macro dekode_k_minstepen_niva(column_name) %}
    case {{ column_name }}
        when 'HOY' then 'Høy'
        when 'LAV' then 'Lav'
        when 'ORDINAER' then 'Ordinær'
        when 'SAERSKILT' then 'Særskilt'
        when 'HOY_ENSLIG' then 'Høy, enslig'
        else {{ column_name }}
    end
{% endmacro %}

{% macro dekode_k_regelverk_t(column_name) %}
    case {{ column_name }}
        when 'G_REG' then 'AP kap 19 tom 2010'
        when 'N_REG_G_N_OPPTJ' then 'AP kap. 19 og kap. 20'
        when 'N_REG_G_OPPTJ' then 'AP kap 19 fom 2011'
        when 'N_REG_N_OPPTJ' then 'AP kap. 20'
        else {{ column_name }}
    end
{% endmacro %}

{% macro dekode_k_sak_t(column_name) %}
    case {{ column_name }}
        when 'AFP' then 'AFP'
        when 'AFP_PRIVAT' then 'AFP Privat'
        when 'ALDER' then 'Alderspensjon'
        when 'BARNEP' then 'Barnepensjon'
        when 'FAM_PL' then 'Familiepleierytelse'
        when 'GAM_YRK' then 'Gammel yrkesskade'
        when 'GENRL' then 'Generell'
        when 'GJENLEV' then 'Gjenlevendeytelse'
        when 'GRBL' then 'Grunnblanketter'
        when 'KRIGSP' then 'Krigspensjon'
        when 'OMSORG' then 'Omsorgsopptjening'
        when 'UFOREP' then 'Uføretrygd'
        else {{ column_name }}
    end
{% endmacro %}

{% macro dekode_k_sivilstand_t(column_name) %}
    case {{ column_name }}
        when 'ENKE' then 'Enke/-mann'
        when 'GIFT' then 'Gift'
        when 'GJPA' then 'Gjenlevende partner'
        when 'NULL' then 'Uoppgitt'
        when 'REPA' then 'Registrert partner'
        when 'SEPA' then 'Separert partner'
        when 'SEPR' then 'Separert'
        when 'SKIL' then 'Skilt'
        when 'SKPA' then 'Skilt partner'
        when 'UGIF' then 'Ugift'
        else {{ column_name }}
    end
{% endmacro %}

{% macro dekode_k_vedtak_s(column_name) %}
    case {{ column_name }}
        when 'ATT' then 'Attestert'
        when 'AVBR' then 'Avbrutt'
        when 'AVV_OS_KVITT' then 'Avventer oppdragskvittering'
        when 'FASTS' then 'Fastsettes'
        when 'IVERKS' then 'Iverksatt'
        when 'REAK' then 'Reaktiviseres'
        when 'SAMORDN' then 'Samordnet'
        when 'STOPPES' then 'Stoppes'
        when 'STOPPET' then 'Stoppet'
        when 'TIL_ATT' then 'Til attestering'
        when 'TIL_IVERKS' then 'Til iverksettelse'
        when 'TIL_SAMORDN' then 'Til samordning'
        else {{ column_name }}
    end
{% endmacro %}

{% macro dekode_k_vedtak_t(column_name) %}
    case {{ column_name }}
        when 'AFPEO' then 'AFP-etteroppgjør'
        when 'ANKE' then 'Anke'
        when 'AVSL' then 'Avslag'
        when 'ENDRING' then 'Endring'
        when 'ERSTATNING' then 'Erstatning'
        when 'ETTERGIV_GJELD' then 'Ettergivelse av gjeld'
        when 'FORGANG' then 'Førstegang'
        when 'FRYS' then 'Frys'
        when 'GOMR' then 'G-omregning'
        when 'INTERNKON' then 'Internkontroll'
        when 'KLAGE' then 'Klage'
        when 'MTK' then 'Merskatt tilbakekrevning'
        when 'OMGJ_TILBAKE' then 'Omgjøring av tilbakekreving'
        when 'OPPHOR' then 'Opphør'
        when 'OPPTJ' then 'Opptjening'
        when 'REGULERING' then 'Regulering av pensjon'
        when 'SAK_OMKOST' then 'Saksomkostninger'
        when 'SAMMENSTOT' then 'Sammenstøt'
        when 'TILBAKEKR' then 'Tilbakekreving'
        when 'UT_VURDERING_EO' then 'Vurdering av etteroppgjør'
        when 'UTSEND_AVTALELAND' then 'Utsendelse til avtaleland'
        else {{ column_name }}
    end
{% endmacro %}
