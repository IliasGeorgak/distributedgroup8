WITH d AS (
    INSERT INTO dataset (owner_id, path, status)
    VALUES (1, '/data/input.txt', 'ready')
    RETURNING dataset_id
),
c AS (
    INSERT INTO code (owner_id, map_path, reduce_path, status)
    VALUES (1, '/code/map.py', '/code/reduce.py', 'ready')
    RETURNING code_id
),
ds AS (
    INSERT INTO dataset_split (dataset_id)
    SELECT dataset_id FROM d
    RETURNING dataset_split_id, dataset_id
),
j AS (
    INSERT INTO job (manager_id, owner_id, code_id, dataset_id, reduce_count, map_count, status)
    SELECT 10, 1, c.code_id, ds.dataset_id, 2, 4, 'pending'
    FROM c, ds
    RETURNING job_id, code_id
)
INSERT INTO task (job_id, code_id, dataset_split_id, task_type, input_path, output_path, status)
SELECT j.job_id, j.code_id, ds.dataset_split_id,
       'map', '/data/input_part_1.txt', '/data/output_part_1.txt', 'pending'
FROM j, ds;