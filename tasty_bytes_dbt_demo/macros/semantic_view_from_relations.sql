{% macro semantic_view_from_relations(
        tables,
        relationships,
        view_name,
        view_schema=target.schema,
        view_db=target.database) -%}

  {%- if tables | length == 0 %}
      {{ exceptions.raise_compiler_error('Empty tables variable') }}
  {%- endif %}

  {# ① 依存を静的登録 ── DAG 用 #}
  {% for t in tables %}{{ ref(t.ref) }}{% endfor %}

  {% set numeric_kw = ['NUMBER','DECIMAL','INT','INTEGER','BIGINT','SMALLINT','FLOAT','DOUBLE','NUMERIC'] %}
  {% set tbl_lines, fact_lines, dim_lines, metric_lines, rel_lines = [], [], [], [], [] %}

  {# ---------- TABLES & COLUMN META ---------- #}
  {% for t in tables %}
      {% set alias = t.alias %}
      {% set rel   = ref(t.ref) %}
      {% set pk    = t.get('pk','') %}

      {# テーブルコメント取得 (# res[0][0] が正) #}
      {% set tbl_cmt = '' %}
      {% if execute %}
        {% set q = "select coalesce(comment,'') from " ~ rel.database ~
                   ".information_schema.tables where table_schema = upper('" ~ rel.schema ~
                   "') and table_name = upper('" ~ rel.identifier ~ "')" %}
        {% set res = run_query(q) %}
        {% if res and res[0][0] %}{% set tbl_cmt = res[0][0] | replace("'", "''") %}{% endif %}
      {% endif %}

      {% do tbl_lines.append(
         alias ~ " AS " ~ rel
         ~ (pk and (" PRIMARY KEY (" ~ pk ~ ")") or '')
         ~ (tbl_cmt and (" COMMENT = '" ~ tbl_cmt ~ "'") or '')
      ) %}

      {# 列コメント辞書 #}
      {% set col_cmt = dict() %}
      {% if execute %}
        {% set c_q = "
          select column_name,
                 case when comment is null or comment='' then column_name else comment end
          from " ~ rel.database ~ ".information_schema.columns
          where table_schema = upper('" ~ rel.schema ~ "') and table_name = upper('" ~ rel.identifier ~ "')" %}
          
        {% for r in run_query(c_q) %}
          {% do col_cmt.update({ r[0]|upper: r[1]|replace("'", "''") }) %}
        {% endfor %}
      {% endif %}


      {# 列処理 #}
      {% for c in adapter.get_columns_in_relation(rel) %}
          {% set cname = c.name %}
          {% set dtype = c.dtype.upper() %}
          {% set cmt   = col_cmt.get(cname.upper(), cname) %}
          {% set logical = sanitize_id(cname) %}
          {% set phys    = sanitize_id(cname) %}
          {% set is_numeric = dtype in numeric_kw
                             or dtype.startswith('NUMBER(')
                             or dtype.startswith('DECIMAL(') %}

          {% if is_numeric %}
              {% do fact_lines.append(alias ~ '.' ~ logical
                  ~ ' AS ' ~ alias ~ '.' ~ phys
                  ~ " COMMENT = '" ~ cmt ~ "'") %}
              {% set metric_logical = logical.startswith('"') and logical[:-1] ~ '_SUM"' or logical ~ '_SUM' %}
              {% do metric_lines.append(alias ~ '.' ~ metric_logical
                  ~ ' AS SUM(' ~ alias ~ '.' ~ phys ~ ") COMMENT = 'sum of " ~ cmt ~ "'") %}
          {% else %}
              {% do dim_lines.append(alias ~ '.' ~ logical
                  ~ ' AS ' ~ alias ~ '.' ~ phys
                  ~ " COMMENT = '" ~ cmt ~ "'") %}
          {% endif %}
      {% endfor %}
  {% endfor %}

  {# ---------- RELATIONSHIPS ---------- #}
  {% for r in relationships %}
      {% set lhs = r.left ~ ' (' ~ r.left_cols | join(', ') ~ ')' %}
      {% set rhs = r.right ~ ' (' ~ r.right_cols | join(', ') ~ ')' %}
      {% do rel_lines.append(lhs ~ ' REFERENCES ' ~ rhs) %}
  {% endfor %}

  {# ---------- 組み立て ---------- #}
  {% set sql %}
    TABLES (
      {{ tbl_lines | join(',\n      ') }}
    )
    {% if rel_lines %}RELATIONSHIPS (
      {{ rel_lines | join(',\n      ') }}
    ){% endif %}
    {% if fact_lines %}FACTS (
      {{ fact_lines | join(',\n      ') }}
    ){% endif %}
    DIMENSIONS (
      {{ dim_lines | join(',\n      ') }}
    )
    {% if metric_lines %}METRICS (
      {{ metric_lines | join(',\n      ') }}
    ){% endif %}
  {% endset %}

  {{ return(sql) }}
{%- endmacro %}