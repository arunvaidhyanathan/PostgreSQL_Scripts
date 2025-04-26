
-- Check if the pg_partman extension is already installed
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_partman') THEN
        RAISE NOTICE 'pg_partman extension is not installed. Please install it first.';
        -- Consider raising an exception if you want to enforce the extension
        -- RAISE EXCEPTION 'pg_partman extension is not installed.';
    ELSE
        RAISE NOTICE 'pg_partman extension is already installed.';
    END IF;
END $$;

create schema partman;

CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;

create schema cron;
drop schema cron;

CREATE EXTENSION pg_cron;

Run CREATE EXTENSION pg_cron;

CREATE OR REPLACE FUNCTION cads.setup_schema_partitions(p_schema_name TEXT)
RETURNS VOID AS $$
DECLARE
    _config RECORD;
    _parent_table TEXT;
    _column_exists BOOLEAN;
    _pg_partman_installed BOOLEAN;
    _pg_cron_installed BOOLEAN;
    _is_partitioned BOOLEAN;
    _partition_key TEXT;
    _sql TEXT;
    _job_id BIGINT;
    _job_exists BOOLEAN := FALSE;
    _cron_schedule TEXT;
    _maintenance_job_name TEXT := 'pg_partman_maintenance'; -- Standard name for the single maintenance job
BEGIN
    RAISE NOTICE '--- Starting Partitioning Setup for Schema: % ---', p_schema_name;

    -- === Initial Checks ===
    SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_partman') INTO _pg_partman_installed;
    IF NOT _pg_partman_installed THEN
        INSERT INTO cads.partition_log (log_level, message, details) VALUES ('ERROR', 'pg_partman extension not installed.', 'Install with CREATE EXTENSION pg_partman;');
        RAISE EXCEPTION 'pg_partman extension is not installed. Aborting. Install with CREATE EXTENSION pg_partman;';
    END IF;
    RAISE NOTICE 'CHECK: pg_partman extension found.';

    SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') INTO _pg_cron_installed;
    IF NOT _pg_cron_installed THEN
        INSERT INTO cads.partition_log (log_level, message, details) VALUES ('WARNING', 'pg_cron extension not installed.', 'Automatic scheduling via pg_cron will not be configured.');
        RAISE WARNING 'pg_cron extension not installed. Automatic scheduling will not be configured.';
        -- Continue without scheduling if pg_cron isn't there
    ELSE
        RAISE NOTICE 'CHECK: pg_cron extension found.';
    END IF;

    -- === Iterate Through Configured Tables ===
    FOR _config IN
        SELECT * FROM cads.partition_table_list
        WHERE schema_name = p_schema_name AND enabled = true
        ORDER BY table_name -- Process consistently
    LOOP
        _parent_table := format('%I.%I', _config.schema_name, _config.table_name);
        RAISE NOTICE 'Processing table: %', _parent_table;
        INSERT INTO cads.partition_log (schema_name, table_name, log_level, message)
            VALUES (_config.schema_name, _config.table_name, 'INFO', 'Starting processing for table ' || _parent_table);

        BEGIN -- Start block for table-specific error handling

            -- 1. Check Table Existence
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = _config.schema_name AND table_name = _config.table_name
            ) THEN
                RAISE WARNING 'Table % not found. Skipping.', _parent_table;
                INSERT INTO cads.partition_log (schema_name, table_name, log_level, message)
                    VALUES (_config.schema_name, _config.table_name, 'WARNING', 'Table not found. Skipping.');
                UPDATE cads.partition_table_list SET last_run_status = 'ERROR: Table not found', last_run_timestamp = now()
                    WHERE id = _config.id;
                CONTINUE; -- Next table in loop
            END IF;

            -- 2. Check Partition Column
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = _config.schema_name
                  AND table_name = _config.table_name
                  AND column_name = _config.partition_column
            ) INTO _column_exists;

            IF NOT _column_exists THEN
                IF _config.add_partition_column_if_missing THEN
                    RAISE WARNING '[%] Partition column "%" missing. Adding automatically as type % (DEFAULT NULL). VALIDATE DATA COMPATIBILITY!',
                        _parent_table, _config.partition_column, _config.partition_column_type;
                    INSERT INTO cads.partition_log (schema_name, table_name, log_level, message, details)
                        VALUES (_config.schema_name, _config.table_name, 'WARNING', 'Partition column missing. Adding automatically.', format('Column: %s, Type: %s', _config.partition_column, _config.partition_column_type));

                    _sql := format('ALTER TABLE %I.%I ADD COLUMN %I %s DEFAULT NULL;',
                                   _config.schema_name, _config.table_name, _config.partition_column, _config.partition_column_type);
                    RAISE NOTICE 'Executing: %', _sql;
                    EXECUTE _sql;

                    IF _config.create_partition_column_index THEN
                         _sql := format('CREATE INDEX IF NOT EXISTS idx_%s_%s ON %I.%I(%I);',
                                       _config.table_name, _config.partition_column, _config.schema_name, _config.table_name, _config.partition_column);
                         RAISE NOTICE 'Executing: %', _sql;
                         EXECUTE _sql;
                         INSERT INTO cads.partition_log (schema_name, table_name, log_level, message, details)
                            VALUES (_config.schema_name, _config.table_name, 'INFO', 'Index created on partition column.', format('Column: %s', _config.partition_column));
                    END IF;
                     -- Re-check column existence after adding it
                     _column_exists := TRUE;
                ELSE
                    RAISE WARNING '[%] Partition column "%" is missing. Manual intervention required (add column and backfill data) or set add_partition_column_if_missing=true.',
                        _parent_table, _config.partition_column;
                    INSERT INTO cads.partition_log (schema_name, table_name, log_level, message, details)
                        VALUES (_config.schema_name, _config.table_name, 'ERROR', 'Partition column missing. Manual intervention required.', format('Column: %s', _config.partition_column));
                    UPDATE cads.partition_table_list SET last_run_status = format('ERROR: Partition column %s missing', _config.partition_column), last_run_timestamp = now()
                        WHERE id = _config.id;
                    CONTINUE; -- Next table in loop
                END IF;
            ELSE
                 RAISE NOTICE '[%] Partition column "%" exists.', _parent_table, _config.partition_column;
                 -- Optionally check/create index even if column exists
                 IF _config.create_partition_column_index AND NOT EXISTS (
                     SELECT 1
                     FROM pg_index idx
                     JOIN pg_class t ON t.oid = idx.indrelid
                     JOIN pg_attribute a ON a.attrelid = t.oid AND a.attname = _config.partition_column AND a.attnum = ANY(idx.indkey) -- Ensure it's part of the index
                     JOIN pg_class i ON i.oid = idx.indexrelid
                     JOIN pg_namespace n ON n.oid = t.relnamespace
                     WHERE n.nspname = _config.schema_name
                       AND t.relname = _config.table_name
                       AND i.relname LIKE 'idx_%'  -- index naming convention
                 ) THEN
                     _sql := format('CREATE INDEX IF NOT EXISTS idx_%s_%s ON %I.%I(%I);',
                                    _config.table_name, _config.partition_column, _config.schema_name, _config.table_name, _config.partition_column);
                     RAISE NOTICE '[%] Creating missing index on partition column: %', _parent_table, _sql;
                     EXECUTE _sql;
                     INSERT INTO cads.partition_log (schema_name, table_name, log_level, message, details)
                        VALUES (_config.schema_name, _config.table_name, 'INFO', 'Creating missing index on existing partition column.', format('Column: %s', _config.partition_column));
                 END IF;
            END IF; -- End column check/add

            -- 3. Check if already partitioned (and by the correct column)
            SELECT p.partstrat IS NOT NULL, a.attname
            INTO _is_partitioned, _partition_key
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_partitioned_table p ON p.partrelid = c.oid
            LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(p.partattrs)
            WHERE n.nspname = _config.schema_name AND c.relname = _config.table_name AND c.relkind IN ('p', 'r'); -- 'p' = partitioned table, 'r' = regular table

            IF _is_partitioned AND _partition_key IS DISTINCT FROM _config.partition_column THEN
                 RAISE WARNING '[%] Table is already partitioned by "%", but config expects "%". Skipping pg_partman setup.',
                     _parent_table, _partition_key, _config.partition_column;
                 INSERT INTO cads.partition_log (schema_name, table_name, log_level, message, details)
                     VALUES (_config.schema_name, _config.table_name, 'ERROR', 'Table partitioned by wrong column.', format('Expected: %s, Found: %s', _config.partition_column, _partition_key));
                 UPDATE cads.partition_table_list SET last_run_status = format('ERROR: Partitioned by wrong column (%s)', _partition_key), last_run_timestamp = now()
                     WHERE id = _config.id;
                 CONTINUE; -- Next table in loop
            END IF;

            -- 4. Setup pg_partman Parent Table configuration
            -- Check if already configured in partman to avoid errors/duplicates
            IF NOT EXISTS (SELECT 1 FROM partman.part_config WHERE parent_table = _parent_table) THEN
                RAISE NOTICE '[%] Configuring pg_partman for table...', _parent_table;
                 _sql := format(
                    'SELECT partman.create_parent(
                        p_parent_table := %L,
                        p_control := %L,
                        p_type := ''range'',
                        p_interval := %L,
                        p_premake := %s
                    )',
                    _parent_table,
                    _config.partition_column,
                    _config.partition_interval,
                    _config.premake,
                    _config.partition_column, _config.schema_name, _config.table_name, -- For min() lookup
                    _config.inherit_privileges,
                    _config.optimize_constraint_exclusion
                );
                RAISE NOTICE 'Executing: %', _sql;
                EXECUTE _sql;
                INSERT INTO cads.partition_log (schema_name, table_name, log_level, message)
                    VALUES (_config.schema_name, _config.table_name, 'INFO', 'Executed partman.create_parent.');
            ELSE
                RAISE NOTICE '[%] Table already configured in pg_partman. Skipping create_parent.', _parent_table;
                INSERT INTO cads.partition_log (schema_name, table_name, log_level, message)
                    VALUES (_config.schema_name, _config.table_name, 'INFO', 'Table already configured in pg_partman.');
            END IF;

            -- 5. Configure Retention (if specified)
            IF _config.retention_interval IS NOT NULL THEN
                RAISE NOTICE '[%] Setting retention policy: interval=%, keep_table=%, keep_index=%',
                             _parent_table, _config.retention_interval, _config.retention_keep_table, _config.retention_keep_index;
                UPDATE partman.part_config
                   SET retention = _config.retention_interval,
                       retention_keep_table = _config.retention_keep_table,
                       retention_keep_index = _config.retention_keep_index,
                       infinite_time_partitions = true -- Recommended for time-based partitioning
                 WHERE parent_table = _parent_table;
                 INSERT INTO cads.partition_log (schema_name, table_name, log_level, message, details)
                    VALUES (_config.schema_name, _config.table_name, 'INFO', 'Retention policy updated.',
                            format('Interval: %s, KeepTable: %s, KeepIndex: %s', _config.retention_interval, _config.retention_keep_table, _config.retention_keep_index));
            ELSE
                 RAISE NOTICE '[%] No retention interval specified. Disabling retention.', _parent_table;
                 UPDATE partman.part_config
                   SET retention = NULL,
                       infinite_time_partitions = true
                 WHERE parent_table = _parent_table;
                 INSERT INTO cads.partition_log (schema_name, table_name, log_level, message)
                    VALUES (_config.schema_name, _config.table_name, 'INFO', 'Retention disabled (retention_interval is NULL).');
            END IF;

             -- 6. Update Status in Metadata Table on Success for this table
            UPDATE cads.partition_table_list SET last_run_status = 'SUCCESS', last_run_timestamp = now()
                WHERE id = _config.id;
            RAISE NOTICE '[%] Successfully processed.', _parent_table;

        EXCEPTION WHEN OTHERS THEN
            -- Log error and update status, then continue with the next table
            RAISE WARNING '[%] ERROR during processing: % (%)', _parent_table, SQLERRM, SQLSTATE;
            INSERT INTO cads.partition_log (schema_name, table_name, log_level, message, details)
                VALUES (_config.schema_name, _config.table_name, 'ERROR', SQLERRM, format('SQLSTATE: %s', SQLSTATE));
            UPDATE cads.partition_table_list SET last_run_status = format('ERROR: %s', SQLERRM), last_run_timestamp = now()
                WHERE id = _config.id;
            -- Do not re-raise; continue loop
        END; -- End block for table-specific error handling

    END LOOP; -- End table loop

    -- === Setup pg_cron Maintenance Job (if pg_cron is installed) ===
    IF _pg_cron_installed THEN
        -- Find the desired schedule from the *first* enabled config entry (or use a default)
        SELECT cron_schedule INTO _cron_schedule
        FROM cads.partition_table_list
        WHERE enabled = true AND schema_name = p_schema_name AND cron_schedule IS NOT NULL
        ORDER BY id
        LIMIT 1;

        _cron_schedule := COALESCE(_cron_schedule, '0 2 * * *'); -- Default: 2 AM daily if none found

        -- Check if the standard maintenance job already exists
        SELECT EXISTS (SELECT 1 FROM cron.job WHERE jobname = _maintenance_job_name) INTO _job_exists;

        IF NOT _job_exists THEN
            RAISE NOTICE 'Setting up pg_cron job "%" with schedule "%" to run partman.run_maintenance_proc().', _maintenance_job_name, _cron_schedule;
            _sql := format('SELECT cron.schedule(%L, %L, %L)',
                            _maintenance_job_name,
                            _cron_schedule,
                            'SELECT partman.run_maintenance_proc();');
            RAISE NOTICE 'Executing: %', _sql;
            BEGIN
                EXECUTE _sql INTO _job_id;
                 INSERT INTO cads.partition_log (log_level, message, details)
                    VALUES ('INFO', 'pg_cron job scheduled.', format('Job Name: %s, Schedule: %s, Job ID: %s', _maintenance_job_name, _cron_schedule, _job_id));
                 RAISE NOTICE 'Scheduled pg_cron job ID: %', _job_id;
            EXCEPTION WHEN OTHERS THEN
                 RAISE WARNING 'Failed to schedule pg_cron job "%": % (%)', _maintenance_job_name, SQLERRM, SQLSTATE;
                 INSERT INTO cads.partition_log (log_level, message, details)
                    VALUES ('ERROR', 'Failed to schedule pg_cron job.', format('Job Name: %s, Error: %s, SQLSTATE: %s', _maintenance_job_name, SQLERRM, SQLSTATE));
            END;
        ELSE
            RAISE NOTICE 'pg_cron job "%" already exists. Ensure schedule is appropriate. Current schedule can be checked via SELECT schedule FROM cron.job WHERE jobname = %L;', _maintenance_job_name, _maintenance_job_name;
            -- Optional: Update schedule if needed, but be cautious about overwriting manual changes
            -- EXECUTE format('SELECT cron.unschedule(%L)', _maintenance_job_name);
            -- EXECUTE format('SELECT cron.schedule(%L, %L, %L)', _maintenance_job_name, _cron_schedule, 'SELECT partman.run_maintenance_proc();');
             INSERT INTO cads.partition_log (log_level, message, details)
                    VALUES ('INFO', 'pg_cron maintenance job already exists.', format('Job Name: %s', _maintenance_job_name));
        END IF;

        RAISE NOTICE E'IMPORTANT: Ensure the user running the cron job (usually the db superuser or postgres user) has necessary permissions:';
        RAISE NOTICE E'  - USAGE on schema partman';
        RAISE NOTICE E'  - EXECUTE on ALL FUNCTIONS in schema partman';
        RAISE NOTICE E'  - Necessary privileges (SELECT, INSERT, DELETE, UPDATE, CREATE, TRIGGER etc.) on the partitioned tables and their schemas (e.g., %)', p_schema_name;

    ELSE
         RAISE NOTICE 'pg_cron not installed. Manual execution of partman.run_maintenance_proc() is required.';
         INSERT INTO cads.partition_log (log_level, message)
            VALUES ('WARNING', 'pg_cron not installed, maintenance job not scheduled.');
    END IF; -- End pg_cron setup

    RAISE NOTICE '--- Partitioning Setup Finished for Schema: % ---', p_schema_name;

END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cads.setup_schema_partitions(TEXT) IS 'Automates pg_partman setup for tables listed in cads.partition_table_list for the given schema. Configures partitioning, retention, and optionally schedules pg_cron maintenance.';


-- Maintenance job for pg_partman
-- This job runs the partman.run_maintenance_proc() function to manage partitioning tasks.
-- It should be scheduled to run periodically (e.g., daily) to ensure that partitions are created and maintained as needed.
-- The job can be managed via pg_cron or manually as needed.
-- The function handles the creation of new partitions, cleanup of old partitions, and other maintenance tasks.
-- The job can be customized to run for specific schemas or tables as needed.
-- The function can be run manually or scheduled via pg_cron for regular maintenance.
-- This is a placeholder for the pg_cron job setup. The actual scheduling should be done in the pg_cron interface or via the setup_schema_partitions function.
-- Example pg_cron job setup (to be run in the database):
-- SELECT cron.schedule('0 2 * * *', 'SELECT partman.run_maintenance_proc();'); 

-- Function to view recent logs
CREATE OR REPLACE FUNCTION cads.get_partition_logs(p_limit INT DEFAULT 100)
RETURNS TABLE (
    log_id INT,
    log_timestamp TIMESTAMPTZ,
    schema_name TEXT,
    table_name TEXT,
    log_level TEXT,
    message TEXT,
    details TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT pl.log_id, pl.log_timestamp, pl.schema_name, pl.table_name, pl.log_level, pl.message, pl.details
    FROM cads.partition_log pl
    ORDER BY pl.log_timestamp DESC
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cads.get_partition_logs(INT) IS 'Retrieves recent partition automation logs, newest first.';

-- Function to view configuration status and errors
CREATE OR REPLACE FUNCTION cads.get_partition_status(p_schema_name TEXT DEFAULT NULL)
RETURNS TABLE (
    id INT,
    schema_name TEXT,
    table_name TEXT,
    enabled BOOLEAN,
    last_run_status TEXT,
    last_run_timestamp TIMESTAMPTZ
) AS $$
BEGIN
    RETURN QUERY
    SELECT ptl.id, ptl.schema_name, ptl.table_name, ptl.enabled, ptl.last_run_status, ptl.last_run_timestamp
    FROM cads.partition_table_list ptl
    WHERE (p_schema_name IS NULL OR ptl.schema_name = p_schema_name)
    ORDER BY ptl.schema_name, ptl.table_name;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cads.get_partition_status(TEXT) IS 'Retrieves the configuration status and last run outcome for partitioned tables, optionally filtered by schema.';

-- Function to check pg_cron job status (if installed)
CREATE OR REPLACE FUNCTION cads.get_partition_cron_job_status()
RETURNS TABLE (
    jobid BIGINT,
    schedule TEXT,
    command TEXT,
    nodename TEXT,
    nodeport INT,
    database TEXT,
    username TEXT,
    active BOOLEAN,
    jobname TEXT
) AS $$
DECLARE
    _pg_cron_installed BOOLEAN;
BEGIN
    SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') INTO _pg_cron_installed;
    IF _pg_cron_installed THEN
        RETURN QUERY SELECT * FROM cron.job WHERE jobname = 'pg_partman_maintenance'; -- Use the standard name
    ELSE
        RAISE NOTICE 'pg_cron extension is not installed.';
        -- Return empty result set
        RETURN QUERY SELECT * FROM cron.job WHERE false;
    END IF;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cads.get_partition_cron_job_status() IS 'Retrieves the status of the standard pg_partman maintenance job from pg_cron, if installed.';

SELECT cads.setup_schema_partitions('cads');

select * from cads.partition_log pl;

