-- klassifiserer et revisjonsfelt (f.eks. opprettet_av, endret_av, attestert_av)
-- til hvilken type ident det er snakk om: saksbehandler-ident, fødselsnummer, selvbetjening, tomt felt eller system
{% macro klassifiser_revisjonsfelt(column_name) %}
    case
        when {{ column_name }} = 'BRUKER-FNR' then 'BRUKER' -- pga egen mapping i int-modell
        when regexp_like({{ column_name }}, '^([A-Z]{3})([0-9]{1,4})$|^([A-Z])([0-9]{1,6})$') then 'SAKSBEH'
        when regexp_like({{ column_name }}, '^[0-9]{11}$') then 'BRUKER'
        when {{ column_name }} in ('srvpselv', 'pensjon-selvbetjenin') then 'SELVB_ANNET'
        when {{ column_name }} is null then 'NULL'
        else 'SYSTEM'
    end
{% endmacro %}
