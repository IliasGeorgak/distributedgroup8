-- =========================
-- DATASET
-- =========================
CREATE TABLE dataset (
    dataset_id     SERIAL PRIMARY KEY,
    owner_id       INT NOT NULL,
    path           TEXT NOT NULL,
    status         VARCHAR(50),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =========================
-- CODE
-- =========================
CREATE TABLE code (
    code_id     SERIAL PRIMARY KEY,
    owner_id    INT NOT NULL,
    map_path    TEXT,
    reduce_path TEXT,
    status      VARCHAR(50),
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =========================
-- DATASET_SPLIT
-- =========================
CREATE TABLE dataset_split (
    dataset_split_id SERIAL PRIMARY KEY,
    dataset_id       INT NOT NULL,
    -- add other attributes as needed
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_dataset_split_dataset
        FOREIGN KEY (dataset_id)
        REFERENCES dataset(dataset_id)
        ON DELETE CASCADE
);

-- =========================
-- JOB
-- =========================
CREATE TABLE job (
    job_id       SERIAL PRIMARY KEY,
    manager_id   INT,
    owner_id     INT,
    code_id      INT NOT NULL,
    dataset_id   INT NOT NULL,
    reduce_count INT,
    map_count    INT,
    status       VARCHAR(50),
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_job_code
        FOREIGN KEY (code_id)
        REFERENCES code(code_id)
        ON DELETE CASCADE,

    CONSTRAINT fk_job_dataset
        FOREIGN KEY (dataset_id)
        REFERENCES dataset(dataset_id)
        ON DELETE CASCADE
);

-- =========================
-- TASK
-- =========================
CREATE TABLE task (
    task_id           SERIAL PRIMARY KEY,
    job_id            INT NOT NULL,
    code_id           INT NOT NULL,
    dataset_split_id  INT NOT NULL,
    task_type         VARCHAR(50),
    input_path        TEXT,
    output_path       TEXT,
    status            VARCHAR(50),
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_task_job
        FOREIGN KEY (job_id)
        REFERENCES job(job_id)
        ON DELETE CASCADE,

    CONSTRAINT fk_task_code
        FOREIGN KEY (code_id)
        REFERENCES code(code_id)
        ON DELETE CASCADE,

    CONSTRAINT fk_task_dataset_split
        FOREIGN KEY (dataset_split_id)
        REFERENCES dataset_split(dataset_split_id)
        ON DELETE CASCADE
);