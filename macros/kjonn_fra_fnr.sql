{% macro kjonn_fra_fnr(fnr) %}
case
    when substr({{ fnr }}, 9, 1) in ('0', '2', '4', '6', '8') then 'K'
    when substr({{ fnr }}, 9, 1) in ('1', '3', '5', '7', '9') then 'M'
end
{% endmacro %}
