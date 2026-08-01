SELECT event_id, SUM(quantity) AS quantity_sum, SUM(score) AS score_sum
FROM query_engine_stress_delta
GROUP BY event_id
ORDER BY event_id
