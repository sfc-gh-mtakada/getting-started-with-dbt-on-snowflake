{% materialization semantic_file, default %}
    {# ------------------------------------------------------------------
       config
         view_fqn   : Name of Semantic View with Full Path (DB.SCH.SV)
         stage_fqn  : A Stage a YAML File uploads (@DEV.SHARED_STAGE)
         file_name  : Yaml File Name (".yaml")
       Example:
         {{ config(materialized='semantic_file',
                   view_fqn  ='DB.SCH.MY_SV',
                   stage_fqn ='@DEV.SHARED_STAGE',
                   file_name ='my_sv.yaml') }}
       ------------------------------------------------------------------ #}

    {% set view  = config.get('view_fqn') %}
    {% set stage = config.get('stage_fqn') %}
    {% set fname = config.get('file_name', this.identifier ~ '.yaml') %}

    {% if view is none or stage is none %}
        {{ exceptions.raise_compiler_error("Must set to view_fqn and stage_fqn") }}
    {% endif %}

    {# ---- Stored Procedure ---- #}
    {% call statement('main') %}
        CALL TASTY_BYTES_DBT_DB.PUBLIC.EXPORT_SV_YAML_TO_STAGE(
              '{{ view }}',
              '{{ stage }}',
              '{{ fname }}' );
    {% endcall %}

    {{ log('YAML exported: ' ~ stage ~ '/' ~ fname, info=True) }}

    {{ return({'relations': []}) }}

{% endmaterialization %}