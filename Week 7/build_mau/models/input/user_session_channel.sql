{{ config(materialized='view') }}

-- Input model built as a single CTE
with source_data as (
  select
      cast(SESSIONID as string) as sessionId,
      cast(USERID   as string)  as userId,
      lower(CHANNEL)            as channel
  from USER_DB_MAGPIE.RAW.USER_SESSION_CHANNEL

)

select * from source_data
