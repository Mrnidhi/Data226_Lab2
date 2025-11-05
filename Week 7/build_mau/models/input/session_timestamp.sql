{{ config(materialized='view') }}

-- Input model built as a single CTE
with source_data as (
  select
      cast(SESSIONID as string) as sessionId,
      TS::timestamp_ntz         as event_ts
  from USER_DB_MAGPIE.RAW.SESSION_TIMESTAMP

)
select * from source_data
