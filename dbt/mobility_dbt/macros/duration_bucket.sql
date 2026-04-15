{% macro duration_bucket(minutes_col) %}
    case
        when {{ minutes_col }} < 10 then 'short'
        when {{ minutes_col }} < 30 then 'medium'
        else 'long'
    end
{% endmacro %}