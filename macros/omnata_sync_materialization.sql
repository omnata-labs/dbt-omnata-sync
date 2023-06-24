

{% materialization omnata_sync, default %}
    {{ run_hooks(pre_hooks) }}
    {% if execute %}
        {%- set this_model_name = this.name -%}
        {%- set main_target = config.require('main_target') -%}
        {%- set match_required_targets = config.require('match_required_targets') -%}
        {%- set branch_name = 'main' if target.name==main_target else target.name -%}
        {%- set match_required = target.name in match_required_targets -%}
        {%- set direction = config.require('direction') -%}
        {%- set sync_parameters = config.require('sync_parameters') -%}
        {%- set branching_behaviour = config.require('branching_behaviour') -%}
        {%- set sync_branch_overrides = config.get('sync_branch_overrides',default='{}') -%}
        {%- set wait_for_completion = config.get('wait_for_completion',default=True) -%}
        /* Get the SQL from the model body and trim the whitespace */
        {%- set source_model = modules.re.compile("^\s+|\s+$", modules.re.MULTILINE).sub('',sql) -%}
        /* TODO: Improve the regex to better extract Snowflake identifiers */
        {%- set source_model_parts = modules.re.split('\.',source_model) -%}
        {%- set sync_task = config.require('sync_task') -%}
        {%- set sync_connection = config.require('sync_connection') -%}
        {%- set omnata_application_name = var('omnata_application_name') -%}
        {%- set expect_omnata_match = var('expect_omnata_match',default=True) -%}
        {% if env_var('DBT_CLOUD_PROJECT_ID', 'not_cloud')=='not_cloud' %}
            {%- set run_source_name = 'dbt_core' -%}
            {% set run_source_metadata %}
                {
                    "project_name":"{{ project_name }}"
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
                }
            {% endset %}
        {% endif %}

        {% if direction=='outbound' %}
            {%- set source_id_column = config.get('source_id_column',default='') -%}
            {% if source_id_column == '' %}
                {{ exceptions.raise_compiler_error("Outbound syncs require the source_id_column config attribute") }}
            {% endif %}
            {%- set field_mappings = config.get('field_mappings',default='[]') -%}
        {% else %}
            {%- set streams_configuration = config.get('streams_configuration',default='[]') -%}
        {% endif %}
        /*sync_branch_name="{{target.name}}", /* hardcoded? */
        /*bring_record_history_to_new_branch=True, /* make this optional with default */
        /*skip_all_records_on_new_branch=False, /* make this optional with default */
        {% if sync_connection != 'TBD' %}
            {% if direction=='outbound' %}
                {% set query %}
                    call "{{omnata_application_name}}".API.APPLY_OUTBOUND_SYNC_SETTINGS(
                                            null,
                                            '{{sync_task}}',
                                            '{{sync_connection}}',
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
                                            '{{branching_behaviour['record_state_behaviour']}}',        -- OUTBOUND_RECORD_STATE_BEHAVIOUR
                                            '{{branching_behaviour['reopen_behaviour']}}',              -- REOPEN_BEHAVIOUR
                                            {{branching_behaviour['record_filter'] or []}}              -- OUTBOUND_BRANCH_RECORD_FILTER
                                            )
                {% endset %}
            {% else %}
                {% set query %}
                    call "{{omnata_application_name}}".API.APPLY_INBOUND_SYNC_SETTINGS(
                                            null,
                                            '{{sync_task}}',
                                            '{{sync_connection}}',
                                            '{{branch_name}}',
                                            {{match_required}},
                                            PARSE_JSON($${{sync_parameters}}$$),
                                            PARSE_JSON($${{sync_branch_overrides}}$$),
                                            PARSE_JSON($${{streams_configuration}}$$),
                                            true, -- activate draft
                                            '{{branching_behaviour['records_behaviour']}}',      -- INBOUND_RECORDS_BEHAVIOUR
                                            '{{branching_behaviour['stream_state_behaviour']}}', -- INBOUND_STREAM_STATE_BEHAVIOUR
                                            '{{branching_behaviour['reopen_behaviour']}}'        -- REOPEN_BEHAVIOUR
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
                call "{{omnata_application_name}}".API.RUN_SYNC(null,'{{sync_task}}',
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
                    {{ exceptions.raise_compiler_error("Sync status: " ~ result_object.data.syncRunHealth) }}
                {% endif %}
            {% endif %}
        {% else %}
            {% if expect_omnata_match==True %}
                {{ exceptions.raise_compiler_error("This target (" ~ target.name ~ ") does not have a connection set. To fix this, ensure that it matches the development or production target configured in the Omnata UI. To ignore this and disable syncing for this target, set the expect_omnata_match var to False (--vars 'expect_omnata_match: False')") }}
            {% else %}
                {%- call statement('main', fetch_result=True) -%}
                    select 'Skipping until connection exists'
                {%- endcall -%}
            {% endif %}
        {% endif %}
    {% endif %}
    {{ run_hooks(post_hooks) }}

    {{ return({'relations': []}) }}
{% endmaterialization %}