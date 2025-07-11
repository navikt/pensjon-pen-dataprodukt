-- brukes på periode (YYYYMM) for å spytte ut siste sekund av måneden
-- blir brukt på filtre for å finne gyldige vedtak i perioden
{% macro periode_sluttdato(aarmnd) %}
  last_day(to_date({{ aarmnd }}, 'YYYYMM')) + interval '23:59:59' hour to second
{% endmacro %}