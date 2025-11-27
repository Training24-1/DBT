{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'order_id',

    pre_hook = [
      "ALTER SESSION SET QUERY_TAG = 'dbt:fct_orders:{{ invocation_id }}';"
    ],

    post_hook = [
      "INSERT INTO analytics.model_run_audit (model_name, run_at, row_count, run_id)
       SELECT '{{ this }}', CURRENT_TIMESTAMP(), COUNT(*), '{{ invocation_id }}' FROM {{ this }}",
      "GRANT SELECT ON {{ this }} TO ROLE reporting"
    ]
) }}

SELECT
    order_id,
    customer_id,
    status,
    created_at
FROM {{ ref('stg_orders') }}
