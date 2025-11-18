{% snapshot snapshot_session_summary %}

{{
    config(
        target_database='USER_DB_MAGPIE',
        target_schema='ANALYTICS',
        unique_key='sessionId',
        strategy='timestamp',
        updated_at='session_end'
    )
}}

select * from {{ ref('session_summary') }}

{% endsnapshot %}
