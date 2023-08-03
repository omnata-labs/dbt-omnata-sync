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
    {%- set branch_name = 'main' if target.name==sync_model['config']['main_target'] else target.name -%}
    {%- set source_schema = "INBOUND_RAW" -%}
    {%- set source_name = sync_model['config']['sync_task'] ~ "_" ~ stream_name ~ "_" ~ branch_name -%}
    {{ log("source_name: " ~ source_name) }}
    {% if execute %}
        {%- set identifier = model['alias'] -%}
        {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
        {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}
        {%- set target_relation = api.Relation.create(
            identifier=identifier, schema=schema, database=database,
            type='view') -%}
        {%- set streams_configuration = fromjson(sync_model.config['streams_configuration']) -%}
        {%- set this_stream_configuration = streams_configuration['included_streams'][stream_name] -%}
        {%- set this_stream_schema = this_stream_configuration['stream']['json_schema'] -%}
        {% set columns_query %}
        select "{{omnata_application_name}}".API.COLUMNS_FROM_JSON_SCHEMA(
            PARSE_JSON($${{tojson(this_stream_schema)}}$$)) as COLUMNS
        {% endset %}
        {% set columns_query_result = run_query(columns_query) %}
        {% set column_definitions = columns_query_result[0].values()[0] %}
        {% set sql %}
        select {{column_definitions}} from "{{omnata_application_name}}"."{{source_schema}}"."{{source_name}}"
        {% endset %}
        
        {% if run_outside_transaction_hooks %}
            -- no transactions on BigQuery
            {{ run_hooks(pre_hooks, inside_transaction=False) }}
        {% endif %}
        -- `BEGIN` happens here on Snowflake
        {{ run_hooks(pre_hooks, inside_transaction=True) }}
        -- If there's a table with the same name and we weren't told to full refresh,
        -- that's an error. If we were told to full refresh, drop it. This behavior differs
        -- for Snowflake and BigQuery, so multiple dispatch is used.
        {%- if old_relation is not none and old_relation.is_table -%}
            {{ handle_existing_table(flags.FULL_REFRESH, old_relation) }}
        {%- endif -%}

        -- build model
        {% call statement('main') -%}
            {{ create_view_as(target_relation, sql) }}
        {%- endcall %}
        {{ run_hooks(post_hooks, inside_transaction=True) }}
        
    {% endif %}

    {{ adapter.commit() }}

    {% if run_outside_transaction_hooks %}
        -- No transactions on BigQuery
        {{ run_hooks(post_hooks, inside_transaction=False) }}
    {% endif %}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}