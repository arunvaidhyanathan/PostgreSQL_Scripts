
CREATE OR REPLACE FUNCTION cads.automate_search_index_creation()
RETURNS VOID AS $$
DECLARE
    r RECORD;
    index_name TEXT;
    sql_command TEXT;
    schema_name TEXT := 'cads'; -- Or the schema where your target tables reside. Adjust if needed.
BEGIN
    RAISE NOTICE 'Starting index automation process...';

    -- Ensure the target schema exists (optional, good practice)
    -- EXECUTE 'CREATE SCHEMA IF NOT EXISTS ' || quote_ident(schema_name);

    -- Enable pg_trgm if not already enabled (runs once)
    BEGIN
        CREATE EXTENSION IF NOT EXISTS pg_trgm;
        RAISE NOTICE 'pg_trgm extension ensured.';
    EXCEPTION WHEN duplicate_object THEN
        RAISE NOTICE 'pg_trgm extension already exists.';
    END;

    FOR r IN
        SELECT lower(esf.collection_name) AS table_name,
               lower(esf.field_name) AS column_name,
               lower(esf.field_type) AS column_type,
               esf.regex_enabled
        FROM cads.enhanced_search_fields esf
        JOIN cads.gui_label gl ON esf.gui_identifier = gl.gui_identifier
        WHERE esf.is_active = 'Y'
        -- Add any other filtering if needed, e.g., specific tables
        -- AND esf.collection_name IN ('table1', 'table2', ...)
    LOOP
        RAISE NOTICE 'Processing table: %, column: %, type: %, regex_enabled: %',
                     quote_ident(r.table_name), quote_ident(r.column_name), r.column_type, r.regex_enabled;

        -- Sanity check: Ensure table and column exist before attempting index creation
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = schema_name -- Use the defined schema
              AND table_name = r.table_name
              AND column_name = r.column_name
        ) THEN
            RAISE WARNING 'Skipping index creation: Table %.% or column % does not exist in schema %.',
                          quote_ident(r.table_name), quote_ident(r.column_name), quote_ident(r.column_name), quote_ident(schema_name);
            CONTINUE;
        END IF;

        -- Determine index type based on column_type and regex_enabled flag
        IF r.column_type IN ('int', 'date') THEN
            -- Create B-Tree Index
            index_name := format('idx_btree_%s_%s', r.table_name, r.column_name);
            sql_command := format(
                'CREATE INDEX IF NOT EXISTS %I ON %I.%I (%I);',
                index_name,            -- Index name
                schema_name,           -- Schema name
                r.table_name,          -- Table name
                r.column_name          -- Column name
            );
            RAISE NOTICE 'Executing: %', sql_command;
            EXECUTE sql_command;
            RAISE NOTICE 'B-Tree index % created or already exists.', index_name;

        ELSIF r.column_type IN ('string', 'regex') THEN
             -- Create GIN Index using pg_trgm (covers =, !=, LIKE, regex)
            index_name := format('idx_gin_trgm_%s_%s', r.table_name, r.column_name);
             -- Important: Ensure the column type is text-compatible (TEXT, VARCHAR, etc.)
            sql_command := format(
                'CREATE INDEX IF NOT EXISTS %I ON %I.%I USING GIN (%I gin_trgm_ops);',
                index_name,            -- Index name
                schema_name,           -- Schema name
                r.table_name,          -- Table name
                r.column_name          -- Column name
            );
            RAISE NOTICE 'Executing: %', sql_command;
            EXECUTE sql_command;
            RAISE NOTICE 'GIN (pg_trgm) index % created or already exists.', index_name;
        ELSE
            RAISE WARNING 'Unsupported column_type % for column %.% - No index created.',
                          r.column_type, quote_ident(r.table_name), quote_ident(r.column_name);
        END IF;

    END LOOP;

    RAISE NOTICE 'Index automation process finished.';
END;
$$ LANGUAGE plpgsql;
