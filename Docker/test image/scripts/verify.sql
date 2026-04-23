SELECT
    j.job_id,
    j.status AS job_status,
    d.path AS dataset_path,
    c.map_path,
    t.task_id,
    t.task_type,
    t.status AS task_status
FROM job j
JOIN dataset d ON j.dataset_id = d.dataset_id
JOIN code c ON j.code_id = c.code_id
JOIN task t ON t.job_id = j.job_id;