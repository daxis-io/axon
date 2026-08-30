SELECT COUNT(*) AS group_count,
       MIN(event_id) AS event_id_min,
       MAX(event_id) AS event_id_max,
       SUM(event_id) AS event_id_sum,
       SUM((event_id % 65521) * (event_id % 65521)) AS event_id_square_residue_sum,
       BIT_XOR(event_id) AS event_id_xor,
       SUM(quantity_sum) AS quantity_sum,
       SUM((event_id % 1009) * quantity_sum) AS quantity_weighted_sum,
       BIT_XOR(quantity_sum) AS quantity_xor,
       SUM(CAST(ROUND(score_sum * 1000000) AS BIGINT)) AS score_micros_sum,
       SUM((event_id % 1009) * CAST(ROUND(score_sum * 1000000) AS BIGINT))
         AS score_micros_weighted_sum,
       BIT_XOR(CAST(ROUND(score_sum * 1000000) AS BIGINT)) AS score_micros_xor
FROM (
  SELECT event_id, SUM(quantity) AS quantity_sum, SUM(score) AS score_sum
  FROM query_engine_stress_delta
  GROUP BY event_id
) AS grouped
