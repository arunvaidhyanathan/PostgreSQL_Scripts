CREATE SCHEMA IF NOT EXISTS cads;

select * from CADS.alert_mst am;


CREATE TABLE cads.alert_mst (
    alert_id SERIAL PRIMARY KEY,
    alert_name VARCHAR(255) NOT NULL,
    description TEXT,
    created_date TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc'),
    modified_date TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc')
    -- Add other relevant columns
);

CREATE TABLE cads.alert_details_mst (
    alert_detail_id SERIAL PRIMARY KEY,
    alert_id INT REFERENCES cads.alert_mst(alert_id),
    detail_description TEXT,
    created_date TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc'),
    modified_date TIMESTAMP WITHOUT TIME
    ZONE DEFAULT (NOW() AT TIME ZONE 'utc')
    -- Add other relevant columns
);


------------------Automated Partition--------------------------------


-- === SCRIPT TO ADD partition_date COLUMN ===

-- Make sure to replace 'cads.' with your actual schema if it's different.
-- Choose EITHER TIMESTAMP or TIMESTAMPTZ based on your needs.

DO $$
DECLARE
    _schema TEXT := 'cads'; -- <<< CHANGE SCHEMA HERE IF NEEDED
    _col_type TEXT := 'TIMESTAMP'; -- <<< CHANGE TO 'TIMESTAMPTZ' IF NEEDED
    _tbl RECORD;
BEGIN
    RAISE NOTICE 'Starting script to add partition_date column to tables in schema %', _schema;

    FOR _tbl IN
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = _schema
          AND table_name IN (
            'alert_attachments',
            'alert_audit',
            'alert_details_mst',
            'alert_mst',
            'alert_notes',
            'business_on_boarding',
            'business_role',
            'business_units',
            'business_user',
            'match_notes',
            'notes_audit'
          )
    LOOP
        RAISE NOTICE 'Processing table: %.%', _schema, _tbl.table_name;

        -- Add the column if it doesn't exist
        EXECUTE format('ALTER TABLE %I.%I ADD COLUMN IF NOT EXISTS partition_date %s DEFAULT NULL;',
                       _schema,
                       _tbl.table_name,
                       _col_type
                      );

        RAISE NOTICE ' -> Column partition_date added (or already existed) to %.%', _schema, _tbl.table_name;

    END LOOP;

    RAISE NOTICE 'Script finished.';
    RAISE NOTICE ' ';
    RAISE NOTICE 'IMPORTANT NEXT STEPS:';
    RAISE NOTICE '1. BACKFILL the partition_date column with appropriate data for existing rows.';
    RAISE NOTICE '   Example: UPDATE cads.alert_mst SET partition_date = created_at WHERE partition_date IS NULL; (Adapt column names!)';
    RAISE NOTICE '2. CREATE an INDEX on the partition_date column after backfilling.';
    RAISE NOTICE '   Example: CREATE INDEX IF NOT EXISTS idx_%s_partition_date ON cads.%I(partition_date);', _tbl.table_name, _tbl.table_name;
    RAISE NOTICE '3. Consider adding a NOT NULL constraint once backfilling is complete.';
    RAISE NOTICE '   Example: ALTER TABLE cads.alert_mst ALTER COLUMN partition_date SET NOT NULL;';

END $$;
CREATE TABLE cads.business_role (
    role_id SERIAL PRIMARY KEY,
    role_name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    created_date TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc'),
    modified_date TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc')
    -- Add other relevant columns
);

CREATE TABLE cads.business_user (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL, -- HASH THIS IN REAL LIFE!
    email VARCHAR(255),
    role_id INT REFERENCES cads.business_role(role_id),  -- Foreign Key
    created_date TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc'),
    modified_date TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc')
    -- Add other relevant columns (first_name, last_name, etc.)
);

CREATE TABLE cads.alert_notes (
    note_id SERIAL PRIMARY KEY,
    alert_id INT REFERENCES cads.alert_mst(alert_id),
    note_text TEXT,
    created_date TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc'),
    modified_date TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc')
    -- Add other relevant columns (author, etc.)
);

CREATE TABLE cads.match_notes (
    match_note_id SERIAL PRIMARY KEY,
    alert_detail_id INT REFERENCES cads.alert_details_mst(alert_detail_id),
    note_text TEXT,
    created_date TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc'),
    modified_date TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc')
    -- Add other relevant columns
);

CREATE TABLE cads.alert_audit (
    audit_id SERIAL PRIMARY KEY,
    alert_id INT REFERENCES cads.alert_mst(alert_id),
    user_id INT, -- consider referencing the business_user table
    action VARCHAR(255),
    details TEXT,
    created_date TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc')
);

CREATE TABLE cads.notes_audit (
    audit_id SERIAL PRIMARY KEY,
    note_id INT REFERENCES cads.alert_notes(note_id),
    user_id INT, -- consider referencing the business_user table
    action VARCHAR(255),
    details TEXT,
    created_date TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc')
);

CREATE TABLE cads.business_units (
    unit_id SERIAL PRIMARY KEY,
    unit_name VARCHAR(255) NOT NULL,
    description TEXT,
    created_date TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc'),
    modified_date TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc')
);

CREATE TABLE cads.business_on_boarding (
    onboarding_id SERIAL PRIMARY KEY,
    user_id INT REFERENCES cads.business_user(user_id),
    unit_id INT REFERENCES cads.business_units(unit_id),
    start_date DATE,
    end_date DATE,
    created_date TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc'),
    modified_date TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc')
);

CREATE TABLE cads.alert_attachments (
    attachment_id SERIAL PRIMARY KEY,
    alert_id INT REFERENCES cads.alert_mst(alert_id),
    file_name VARCHAR(255) NOT NULL,
    file_path VARCHAR(255) NOT NULL, -- Or store the actual file in a bytea column
    created_date TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc'),
    modified_date TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc')
);

-- Function to get alert details from alert_mst and alert_details_mst

CREATE OR REPLACE FUNCTION cads.get_alert_details(p_alert_id INT)
RETURNS TABLE (
    alert_name VARCHAR(255),
    detail_description TEXT
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        am.alert_name,
        adm.detail_description
    FROM
        cads.alert_mst am
    JOIN
        cads.alert_details_mst adm ON am.alert_id = adm.alert_id
    WHERE
        am.alert_id = p_alert_id;
END;
$$ LANGUAGE plpgsql;

-- Example usage:
-- SELECT * FROM cads.get_alert_details(1);

-- Function to retrieve audit information for an alert
CREATE OR REPLACE FUNCTION cads.get_alert_audit_info(p_alert_id INT)
RETURNS TABLE (
    action VARCHAR(255),
    details TEXT,
    created_date TIMESTAMP WITHOUT TIME ZONE
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        aa.action,
        aa.details,
        aa.created_date
    FROM
        cads.alert_audit aa
    WHERE
        aa.alert_id = p_alert_id;
END;
$$ LANGUAGE plpgsql;

-- Example usage:
-- SELECT * FROM cads.get_alert_audit_info(1);

-- Function to retrieve attachments and notes information for an alert
CREATE OR REPLACE FUNCTION cads.get_alert_attachments_and_notes(p_alert_id INT)
RETURNS TABLE (
    note_text TEXT,
    file_name VARCHAR(255),
    file_path VARCHAR(255)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        an.note_text,
        aa.file_name,
        aa.file_path
    FROM
        cads.alert_notes an
    LEFT JOIN
        cads.alert_attachments aa ON an.alert_id = aa.alert_id
    WHERE
        an.alert_id = p_alert_id;
END;
$$ LANGUAGE plpgsql;


INSERT INTO cads.alert_mst (alert_name, description) VALUES ('Alert 1', 'Description 1');
INSERT INTO cads.alert_mst (alert_name, description) VALUES ('Alert 2', 'Description 2');

-- src/main/resources/db/migration/V2__add_email_to_business_user.sql

ALTER TABLE cads.business_user ADD COLUMN email VARCHAR(255);

CREATE INDEX idx_business_user_username ON cads.business_user (username);

SELECT * FROM cads.alert_mst;


CREATE EXTENSION IF NOT EXISTS pg_partman;

SELECT * FROM pg_extension WHERE extname = 'pg_partman';

ALTER TABLE CADS.ALERT_MST ADD COLUMN partition_dates DATE; -- Or TIMESTAMPTZ

 UPDATE CADS.ALERT_MST SET partition_dates = NOW()::date WHERE partition_dates IS NULL;
 
 ALTER TABLE CADS.ALERT_MST ALTER COLUMN partition_dates SET NOT NULL;


 DO $$
DECLARE
    _schema TEXT := 'cads'; -- <<< CHANGE SCHEMA HERE IF NEEDED
    _tbl RECORD;
    _col_type TEXT;

BEGIN
    RAISE NOTICE 'Starting script to change partition_date column type from DATE to TIMESTAMP in schema %', _schema;

    FOR _tbl IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = _schema
          AND table_name IN (
            'alert_attachments',
            'alert_audit',
            'alert_details_mst',
            'alert_mst',
            'alert_notes',
            'business_on_boarding',
            'business_role',
            'business_units',
            'business_user',
            'match_notes',
            'notes_audit'
          )
    LOOP
        RAISE NOTICE 'Processing table: %.%', _schema, _tbl.table_name;

        -- Check current data type
        SELECT data_type
        INTO _col_type
        FROM information_schema.columns
        WHERE table_schema = _schema
          AND table_name = _tbl.table_name
          AND column_name = 'partition_date';

        IF _col_type = 'date' THEN
            RAISE NOTICE ' -> Column partition_date is DATE.  Altering type to TIMESTAMP.';
            -- Change the column type
            EXECUTE format('ALTER TABLE %I.%I ALTER COLUMN partition_date TYPE TIMESTAMP USING partition_date::TIMESTAMP;',
                           _schema,
                           _tbl.table_name);
            RAISE NOTICE ' -> Column partition_date altered to TIMESTAMP for %.%', _schema, _tbl.table_name;
        ELSIF _col_type = 'timestamp without time zone' THEN
            RAISE NOTICE ' -> Column partition_date is already TIMESTAMP. Skipping %.%', _schema, _tbl.table_name;
        ELSE
            RAISE WARNING ' -> Column partition_date has unexpected type: %. Skipping %.%', _col_type, _schema, _tbl.table_name;
        END IF;


    END LOOP;

    RAISE NOTICE 'Script finished.';
    RAISE NOTICE ' ';
    RAISE NOTICE 'REMEMBER: If you needed to preserve time-of-day information, the USING clause needs to be adjusted. This script truncates the time.';

END $$;

----------------------------------------------------------------


-- 1. Metadata Table for Partitioning Configuration
CREATE TABLE IF NOT EXISTS cads.partition_table_list (
    id SERIAL PRIMARY KEY,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    partition_column TEXT NOT NULL DEFAULT 'partition_date',
    partition_column_type TEXT NOT NULL DEFAULT 'DATE', -- e.g., DATE, TIMESTAMPTZ
    partition_interval TEXT NOT NULL DEFAULT '1 day', -- pg_partman interval (e.g., '1 day', '1 week', '1 month')
    premake INTEGER NOT NULL DEFAULT 4, -- How many future partitions to create
    retention_interval TEXT, -- pg_partman retention (e.g., '3 months', '1 year'). NULL means keep forever.
    retention_keep_table BOOLEAN NOT NULL DEFAULT false, -- Keep dropped partition tables?
    retention_keep_index BOOLEAN NOT NULL DEFAULT true, -- Keep indexes on dropped tables?
    add_partition_column_if_missing BOOLEAN NOT NULL DEFAULT false, -- USE WITH EXTREME CAUTION
    create_partition_column_index BOOLEAN NOT NULL DEFAULT true, -- Create index on partition column if missing/added
    optimize_constraint_exclusion BOOLEAN NOT NULL DEFAULT true, -- partman setting
    inherit_privileges BOOLEAN NOT NULL DEFAULT true, -- partman setting for child tables
    job_name TEXT, -- Optional: Custom name for pg_cron job if finer control needed (usually not)
    cron_schedule TEXT DEFAULT '0 1 * * *', -- Schedule for the *single* maintenance job
    enabled BOOLEAN NOT NULL DEFAULT true, -- Controls if the function processes this entry
    last_run_status TEXT, -- 'SUCCESS', 'ERROR: message'
    last_run_timestamp TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (schema_name, table_name) -- Ensure only one entry per table
);

COMMENT ON TABLE cads.partition_table_list IS 'Configuration for tables managed by the partitioning automation function.';
COMMENT ON COLUMN cads.partition_table_list.add_partition_column_if_missing IS 'DANGER: Automatically adds partition_column if missing. Can cause data issues if not carefully managed.';
COMMENT ON COLUMN cads.partition_table_list.cron_schedule IS 'Defines the schedule for the SINGLE partman.run_maintenance_proc() job. Only the first enabled entry''s schedule might be used, or managed manually.';


-- 2. Logging Table for Errors and Actions
CREATE TABLE IF NOT EXISTS cads.partition_log (
    log_id SERIAL PRIMARY KEY,
    log_timestamp TIMESTAMPTZ DEFAULT now(),
    schema_name TEXT,
    table_name TEXT,
    log_level TEXT, -- INFO, WARNING, ERROR
    message TEXT,
    details TEXT -- Optional: e.g., SQLSTATE, SQLERRM
);

COMMENT ON TABLE cads.partition_log IS 'Log table for partitioning automation events and errors.';

-- Optional: Index for faster log querying
CREATE INDEX IF NOT EXISTS idx_partition_log_timestamp ON cads.partition_log(log_timestamp);
CREATE INDEX IF NOT EXISTS idx_partition_log_table ON cads.partition_log(schema_name, table_name);
CREATE INDEX IF NOT EXISTS idx_partition_log_level ON cads.partition_log(log_level);

set schema cads;

select * from cads.partition_table_list;


-- After inserting, run the setup_schema_partitions function for the schema:
-- SELECT cads.setup_schema_partitions('cads');  -- Adjust the schema name if needed

select * from cads.partition_table_list ptl ;

UPDATE cads.alert_mst
SET partition_date = created_date::DATE  -- Or use date_trunc('day', created_at)
WHERE partition_date IS NULL AND created_date IS NOT NULL;  -- Only update NULL values!

CREATE INDEX IF NOT EXISTS idx_alert_mst_partition_date ON cads.alert_mst(partition_date);

select * from pg_class;
