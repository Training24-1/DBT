{{config(materialized = "incremental", 
incremental_strategy ="merge", unique_key = "id"

)}}

merge into target as target t
using source data src


{{config(materialized = "incremental", 
incremental_strategy ="insert_overwrite", paritition_by = {
"field": "order_date", "data_type":"date"

}

)}}


{{config(materialized = "incremental", 
incremental_strategy ="append"

)}}

json flattern

lateral flattern

SELECT
    f.value,
    f.index,
    f.key,
    f.path
FROM table_name t,
LATERAL FLATTEN(input => t.json_column) f;

CREATE OR REPLACE TABLE demo (
  id INT,
  data VARIANT
);

INSERT INTO demo VALUES
(1, PARSE_JSON('{"items": ["A", "B", "C"]}'));

