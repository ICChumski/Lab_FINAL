{% macro classify_demand(column_name) %}
    case
        when {{ column_name }} < 100 then 'low'
        when {{ column_name }} between 100 and 500 then 'medium'
        else 'high'
    end
{% endmacro %}