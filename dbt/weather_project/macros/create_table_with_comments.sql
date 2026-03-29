{% macro apply_column_comments() %}
    {% set model_columns = model.columns %}
    
    {% for col_name, col in model_columns.items() %}
        {% if col.description %}
            {% call statement('comment_' ~ col_name) %}
                ALTER TABLE {{ this }} 
                COMMENT COLUMN `{{ col_name }}` '{{ col.description }}'
            {% endcall %}
        {% endif %}
    {% endfor %}
{% endmacro %}