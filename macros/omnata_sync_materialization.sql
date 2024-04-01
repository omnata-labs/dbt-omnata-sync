{% materialization omnata_sync, default %}
    {{ run_hooks(pre_hooks) }}
    {% if execute %}
        {%- set this_model_name = this.name -%}
        {%- set sync_slug = config.require('sync') -%}
        {%- set main_target = config.require('main_target') -%}
        {%- set match_required_targets = config.require('match_required_targets') -%}
        {%- set default_branch_name = 'main' if target.name==main_target else target.name -%}
        {%- set branch_name = var('omnata_branch',default=default_branch_name) -%}
        {%- set use_dbt_warehouse = var('use_dbt_warehouse',default=False) -%}
        {%- set match_required = target.name in match_required_targets -%}
        {%- set direction = config.require('direction') -%}
        {%- set sync_parameters = config.require('sync_parameters') -%}
        {%- set sync_parameter_overrides_all = config.get('sync_parameter_overrides',default='{}') -%}
        {%- set wait_for_completion = config.get('wait_for_completion',default=True) -%}
        {%- set omnata_application_name = var('omnata_application_name',default='OMNATA_SYNC_ENGINE') -%}
        {%- set expect_omnata_match = var('expect_omnata_match',default=True) -%}
        {%- set uppercase_source_model = var('uppercase_source_model',default=True) -%}
        
        /* 
        We skip over post hooks by default, since this materialization doesn't yield normal tables/views
        and we don't want to trip up people's universal actions.
         */
        {%- set omnata_run_post_hooks = var('omnata_run_post_hooks',default=False) -%}
        
        /* Get the SQL from the model body and trim the whitespace */
        {%- set source_model = modules.re.compile("^\s+|\s+$", modules.re.MULTILINE).sub('',sql) -%}
        {% if uppercase_source_model==True %}
            {%- set source_model = source_model.upper() -%}
        {% endif %}
        /* TODO: Improve the regex to better extract Snowflake identifiers */
        {%- set source_model_parts = modules.re.split('\.',source_model) -%}
        {% if branch_name=='main' %}
            {%- set connection_slug = config.get('main_connection',default='') -%}
            {% if connection_slug is none or connection_slug=='' %}
                {% if expect_omnata_match==True %}
                    {{ exceptions.raise_compiler_error("You initially chose a non-production connection for this sync, and only the branch has been configured. 
                    Before you can apply settings to the main sync, you must first specify a production connection in the Omnata UI.
                    To prevent this error by skipping the sync until the connection exists, set the expect_omnata_match var to False (--vars 'expect_omnata_match: False')") }}
                {% else %}
                    {{ log("Skipping sync because connection is not configured and expect_omnata_match was set to False") }}
                    {{ return({'relations': []}) }}
                {% endif %}
            {% endif %}

        {% else %}
            {%- set branch_connections = config.get('branch_connections',default='{}') -%}
            {% if branch_name not in branch_connections %}
                {%- set connection_slug = '' -%}
            {% else %}
                {%- set connection_slug = branch_connections[branch_name] -%}
            {% endif %}
            {% if connection_slug is none or connection_slug=='' %}
                {% if expect_omnata_match==True %}
                    {{ exceptions.raise_compiler_error("You are applying settings to a branch that has not been initially configured. 
                    Please create the " ~ branch_name ~ " branch in the Omnata UI so that its connection can be specified.
                    To prevent this error by skipping the sync until the connection exists, set the expect_omnata_match var to False (--vars 'expect_omnata_match: False')") }}
                {% else %}
                    {{ log("Skipping sync because connection is not configured and expect_omnata_match was set to False") }}
                    {{ return({'relations': []}) }}
                {% endif %}
            {% endif %}
        {% endif %}

        /* After this point, we no longer allow skipping via expect_omnata_match=False for missing model configuration. */
        /* If the connection has been configured, it means that the rest of the branch configuration should also be present */

        {% if branch_name=='main' %}
            {%- set record_state_behaviour = 'null' -%}
            {%- set record_filter = 'null' -%}
            {%- set records_behaviour = 'null' -%}
            {%- set stream_state_behaviour = 'null' -%}
            {%- set reopen_behaviour = 'null' -%}
        {% else %}
            {%- set branching_behaviour_all = config.require('branching_behaviour') -%}
            {% if branch_name not in branching_behaviour_all %}
                {{ exceptions.raise_compiler_error("The branch named " ~ branch_name ~ " had a connection configured, but no branching_behaviour was defined. This indicates that the dbt model was not correctly generated.") }}
            {% endif %}
            {%- set branching_behaviour = branching_behaviour_all[branch_name] -%}
            {%- set record_state_behaviour = "'"~branching_behaviour['record_state_behaviour']~"'" -%}
            {%- set record_filter = branching_behaviour['record_filter'] -%}
            {%- set records_behaviour = "'"~branching_behaviour['records_behaviour']~"'" -%}
            {%- set stream_state_behaviour = "'"~branching_behaviour['stream_state_behaviour']~"'" -%}
            {%- set reopen_behaviour = "'"~branching_behaviour['reopen_behaviour']~"'" -%}
        {% endif %}
        
        {% if branch_name in sync_parameter_overrides_all %}
            {%- set sync_branch_overrides = sync_parameter_overrides_all[branch_name] -%}
        {% else %}
            {%- set sync_branch_overrides = '{}' -%}
        {% endif %}
        
        {% if env_var('DBT_CLOUD_PROJECT_ID', 'not_cloud')=='not_cloud' %}
            {%- set run_source_name = 'dbt_core' -%}
            {% set run_source_metadata %}
                {
                    "project_name":"{{ project_name }}"
                    {% if use_dbt_warehouse %},"warehouse":"{{target.warehouse}}"{% endif%}
                }
            {% endset %}
        {% else %}
            {%- set run_source_name = 'dbt_cloud' -%}
            {% set run_source_metadata %}
                {
                    "project_id":"{{ env_var('DBT_CLOUD_PROJECT_ID') }}",
                    "project_name":"{{ project_name }}",
                    "job_id":"{{ env_var('DBT_CLOUD_JOB_ID') }}",
                    "run_id":"{{ env_var('DBT_CLOUD_RUN_ID') }}",
                    "run_reason_category":"{{ env_var('DBT_CLOUD_RUN_REASON_CATEGORY') }}",
                    "run_reason":"{{ env_var('DBT_CLOUD_RUN_REASON') }}"
                    {% if use_dbt_warehouse %},"warehouse":"{{target.warehouse}}"{% endif%}
                }
            {% endset %}
        {% endif %}

        {% if direction=='outbound' %}
            {%- set source_id_column = config.get('source_id_column',default='') -%}
            {% if source_id_column == '' %}
                {{ exceptions.raise_compiler_error("Outbound syncs require the source_id_column config attribute") }}
            {% endif %}
            {%- set field_mappings = config.get('field_mappings',default="{'mapper_type':'field_mapping_selector','field_mappings':[]}") -%}
        {% else %}
            {%- set streams_configuration = config.get('streams_configuration',default='[]') -%}
        {% endif %}
        
        {% if direction=='outbound' %}
            {% set query %}
                call "{{omnata_application_name}}".API.APPLY_OUTBOUND_SYNC_SETTINGS(
                                        null,
                                        '{{sync_slug}}',
                                        '{{connection_slug}}',
                                        '{{branch_name}}',
                                        '{{source_model_parts[0]}}',
                                        '{{source_model_parts[1]}}',
                                        '{{source_model_parts[2]}}',
                                        '{{source_id_column}}',
                                        {{match_required}},
                                        PARSE_JSON($${{sync_parameters}}$$),
                                        PARSE_JSON($${{sync_branch_overrides}}$$),
                                        PARSE_JSON($${{field_mappings}}$$),
                                        true, -- activate draft
                                        {{record_state_behaviour}},
                                        {{reopen_behaviour}},
                                        {{record_filter or []}}
                                        )
            {% endset %}
        {% else %}
            {% set query %}
                call "{{omnata_application_name}}".API.APPLY_INBOUND_SYNC_SETTINGS(
                                        null,
                                        '{{sync_slug}}',
                                        '{{connection_slug}}',
                                        '{{branch_name}}',
                                        {{match_required}},
                                        PARSE_JSON($${{sync_parameters}}$$),
                                        PARSE_JSON($${{sync_branch_overrides}}$$),
                                        PARSE_JSON($${{streams_configuration}}$$),
                                        true, -- activate draft
                                        {{records_behaviour}},
                                        {{stream_state_behaviour}},
                                        {{reopen_behaviour}},
                                        null
                                        )
            {% endset %}
        {% endif %}
        {%- call statement('main', fetch_result=True) -%}
            {{query}}
        {%- endcall -%}
        {% set results = load_result('main') %}
        {% set result_object = fromjson(results['data'][0][0]) %}
        {{ log("Apply Sync settings response: " ~ result_object) }}
        {% if result_object.success==False %}
            {{ exceptions.raise_compiler_error("Error applying sync settings: " ~ result_object.error) }}
        {% endif %}
        {{ adapter.commit() }}
        {% if expect_omnata_match==True and result_object.data.settingsApplied==False %}
            {{ exceptions.raise_compiler_error("expect_omnata_match was True, but sync settings did not match") }}
        {% endif %}
        /* if the settings were applied, run the sync */
        {% if result_object.data.settingsApplied==True %}
            {% if direction=='outbound' %}
                /* for outbound syncs, ensure the Omnata application has permission to access the source table */
                {%- call statement() -%}
                grant usage on database {{source_model_parts[0]}} to application "{{omnata_application_name}}"
                {%- endcall -%}
                {%- call statement() -%}
                grant usage on schema {{source_model_parts[0]}}.{{source_model_parts[1]}} to application "{{omnata_application_name}}"
                {%- endcall -%}
                {%- call statement() -%}
                grant select on table {{source_model_parts[0]}}.{{source_model_parts[1]}}.{{source_model_parts[2]}} to application "{{omnata_application_name}}"
                {%- endcall -%}
            {% endif %}
            /* run the sync */
            {% set query %}
            call "{{omnata_application_name}}".API.RUN_SYNC(null,'{{sync_slug}}',
                                                    '{{branch_name}}',
                                                    '{{run_source_name}}',
                                                    PARSE_JSON('{{run_source_metadata}}'),
                                                    {{wait_for_completion}})
            {% endset %}
            {%- call statement('main', fetch_result=True) -%}
            {{query}}
            {%- endcall -%}
            {% set results = load_result('main') %}
            {% set result_object = fromjson(results['data'][0][0]) %}
            {{ log("Run Sync response: " ~ result_object) }}
            {% if result_object.success==False %}
                {{ exceptions.raise_compiler_error("Error running Sync: " ~ result_object.error) }}
            {% endif %}
            {% if result_object.data.syncRunHealth=='FAILED' %}
                {{ exceptions.raise_compiler_error("Sync status: " ~ result_object.data.syncRunHealth ~ ". Please check the sync logs for run " ~result_object.data.syncRunId~", either in the Omnata UI or in the Snowflake events table.") }}
            {% endif %}
        {% endif %}
    {% endif %}
    {% if omnata_run_post_hooks %}
        {{ run_hooks(post_hooks) }}
    {% endif %}
    {{ return({'relations': []}) }}
{% endmaterialization %}