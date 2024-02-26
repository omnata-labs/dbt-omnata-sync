{% materialization omnata_inbound_stream, default %}
    {{ run_hooks(pre_hooks) }}
    
    {%- set omnata_application_name = var('omnata_application_name',default='OMNATA_SYNC_ENGINE') -%}
    {%- set stream_name = config.require('stream_name') -%}
    {{ log("model: " ~ model) }}
    
    {% if model['depends_on']['nodes'] | length == 0 %}
        {{ exceptions.raise_compiler_error("Omnata inbound streams must 'ref' the omnata_sync model for the Sync they belong to.") }}
    {% endif %}

    {%- set sync_model_node_name = model['depends_on']['nodes'][0] -%}
    {%- set sync_model = graph.nodes[sync_model_node_name] -%}
    {{ log("sync_model: " ~ sync_model.config) }}
    {%- set default_branch_name = 'main' if target.name==sync_model['config']['main_target'] else target.name -%}
    {%- set branch_name = var('omnata_branch',default=default_branch_name) -%}
    {% if execute %}
        {%- set identifier = model['alias'] -%}
        {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
        {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}
        {%- set target_relation = api.Relation.create(
            identifier=identifier, schema=schema, database=database,
            type='view') -%}
        {%- set sync_slug = sync_model.config['sync'] -%}
        {% set view_definition_query %}
        call "{{omnata_application_name}}".API.GET_INBOUND_STREAM_VIEW_DEFINITION(
            $${{sync_slug}}$$,
            $${{branch_name}}$$,
            $${{stream_name}}$$
            )
        {% endset %}
        {%- call statement('main', fetch_result=True) -%}
            {{view_definition_query}}
        {%- endcall -%}
        {% set results = load_result('main') %}
        {% set result_object = fromjson(results['data'][0][0]) %}
        {{ log("Get inbound stream view definition response: " ~ result_object) }}
        {% if result_object.success==False %}
            {{ exceptions.raise_compiler_error("Error applying sync settings: " ~ result_object.error) }}
        {% endif %}
        {% set view_definition = result_object['data'] %}
        {% set sql %}
        select {{view_definition['snowflake_columns']|join(',')}} 
        from {{view_definition['table_name']}}
        {% endset %}
        
        {{ run_hooks(pre_hooks, inside_transaction=True) }}
        
        -- build model
        {% call statement('main') -%}
            {{ create_view_as(target_relation, sql) }}
        {%- endcall %}
        
    {% endif %}

    {{ adapter.commit() }}

    {{ run_hooks(post_hooks) }}
    
    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}