{% macro k_bor_med_t__k_grnl_rolle_t(column_name) %}
    case {{ column_name }}
        when 'GLAD_EKT' then 'EKTEF'
        when 'GLAD_PART' then 'PARTNER'
        when 'J_AVDOD' then null
        when 'J_BARN' then 'BARN'
        when 'J_EKTEF' then 'EKTEF'
        when 'J_FBARN' then 'FBARN'
        when 'J_PARTNER' then 'PARTNER'
        when 'J_SOSKEN' then 'SOSKEN'
        when 'N_AVDOD' then null
        when 'N_BARN' then 'BARN'
        when 'N_FBARN' then 'FBARN'
        when 'N_GIFT' then 'EKTEF'
        when 'N_GIFT_P' then 'PARTNER'
        when 'N_SAMBOER' then 'SAMBO'
        when 'N_SOSKEN' then 'SOSKEN'
        when 'SAMBOER1_5' then 'SAMBO'
        when 'SAMBOER3_2' then 'SAMBO'
        else {{ column_name }}
    end
{% endmacro %}
