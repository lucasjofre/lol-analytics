-- Top-level facts for one match, pulled out of the raw Riot JSON in bronze.
-- The `payload:info.x` operator reads a path out of the JSON string column.
-- Nested arrays (participants, teams) are left for downstream models.

select
    match_id,
    platform,
    fetched_at,

    timestamp_millis(cast(payload:info.gameCreation as bigint)) as created_at,
    cast(payload:info.gameDuration as bigint)                   as duration_seconds,
    cast(payload:info.queueId     as int)                       as queue_id,
    cast(payload:info.mapId       as int)                       as map_id,
    cast(payload:info.gameVersion as string)                    as game_version

from {{ source('bronze', 'matches') }}
