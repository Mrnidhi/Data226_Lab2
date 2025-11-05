{{ config(materialized='table') }}

with user_session_channel as (
    select
        sessionId,
        userId,
        channel
    from {{ ref('user_session_channel') }}
),

session_timestamp as (
    select
        sessionId,
        event_ts
    from {{ ref('session_timestamp') }}
),

session_summary as (
    select
        usc.sessionId,
        usc.userId,
        usc.channel,
        min(st.event_ts) as session_start,
        max(st.event_ts) as session_end,
        datediff('second', min(st.event_ts), max(st.event_ts)) as session_duration_seconds
    from user_session_channel usc
    join session_timestamp st
      on usc.sessionId = st.sessionId
    group by usc.sessionId, usc.userId, usc.channel
)

select * from session_summary

