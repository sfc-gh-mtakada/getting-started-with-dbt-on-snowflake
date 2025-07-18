{# macros/sanitize_id.sql #}
{% macro sanitize_id(name) %}
    {# ASCII 英数字 + '_' だけ／かつ先頭が英字 → 非引用識別子 OK #}
    {% if name.isascii()
          and name.replace('_','').isalnum()
          and name[0].isalpha() %}
        {{ return(name.upper()) }}
    {% else %}
        {# それ以外は論理名も引用識別子にする #}
        {{ return('"' ~ name.replace('"','""') ~ '"') }}
    {% endif %}
{% endmacro %}