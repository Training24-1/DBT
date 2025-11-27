{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'order_id',
    on_schema_change = 'fail'    -- we’ll change this later
) }}

WITH src AS (
    SELECT
        order_id,
        customer_id,
        status,
        created_at
    FROM {{ source('raw', 'orders') }}
)

SELECT * FROM src

{% if is_incremental() %}
  WHERE created_at > (SELECT COALESCE(MAX(created_at), '1900-01-01') FROM {{ this }})
{% endif %}


ignore
append_new_columns
sync_all_columns
fail
