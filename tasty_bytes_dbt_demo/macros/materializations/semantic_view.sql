{% materialization semantic_view, default %}

  {%- set identifier = model['name'] -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(
      database=database,
      schema=schema,
      identifier=identifier,
      type='view') -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

　　
  {% set tbl_cfg = config.get('tables') %}
  {% set rel_cfg = config.get('relationships', []) %}
  {% set sql = semantic_view_from_relations(
      tables        = tbl_cfg,
      relationships = rel_cfg,
      view_name     = identifier,
      view_schema   = schema,
      view_db       = database) %}

  -- build model
  {% call statement('main') -%}
    create or replace semantic view {{ target_relation }}

    {{ sql }}

  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %} 