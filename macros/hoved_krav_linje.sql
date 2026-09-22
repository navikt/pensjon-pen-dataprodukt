-- Erstatter join mot kilde-tabellen pen.t_k_kravlinje_t (kolonnen hoved_krav_linje),
-- som er en kodeverkstabell som slettes fra Oracle-databasen.
-- Kodeverdiene vedlikeholdes i javakoden i pensjon-pen, se
-- pen/src/main/java/no/nav/domain/pensjon/kjerne/kodetabeller/KravlinjeTypeCode.kt
-- (feltet hovedKravLinje). Hold denne macroen i sync med den enum-en.
{% macro hoved_krav_linje(column_name) %}
    case {{ column_name }}
        when 'AFP' then '1'
        when 'AFP_PRIVAT' then '1'
        when 'ANKE' then '1'
        when 'AP' then '1'
        when 'BP' then '1'
        when 'BT' then '0'
        when 'EO' then '1'
        when 'ERSTATNING' then '1'
        when 'ET' then '0'
        when 'ETTERGIV_GJELD' then '1'
        when 'FAST_UTG_INST' then '0'
        when 'FP' then '1'
        when 'GJP' then '1'
        when 'GJR' then '0'
        when 'GY' then '1'
        when 'HJBIDRAG' then '0'
        when 'HJELP_HUS' then '0'
        when 'IK' then '1'
        when 'KLAGE' then '1'
        when 'KP' then '1'
        when 'MTK' then '1'
        when 'OG' then '1'
        when 'OO' then '1'
        when 'OTK' then '1'
        when 'SAK_OMKOST' then '1'
        when 'TK' then '1'
        when 'UP' then '1'
        when 'UT' then '1'
        when 'UT_GJT' then '0'
    end
{% endmacro %}
