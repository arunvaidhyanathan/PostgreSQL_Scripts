CREATE TABLE cads.alert_attachments (
    id SERIAL PRIMARY KEY,
    partition_date TIMESTAMPTZ NOT NULL,
    data TEXT  -- Add other columns as needed
) PARTITION BY RANGE (partition_date);

select * from PG_CLASS where relkind = 'p';

delete from cads.partition_log pl;


SELECT * FROM pg_indexes pi where schemaname = 'cads';

select * from pg_class;

SELECT extname, extnamespace::regnamespace FROM pg_extension WHERE extname = 'pg_partman';

SET search_path TO partman, public, "$user"; -- Add "partman" at the beginning

select * from cads.partition_log;

alter table cads.alert_attachments alter column partition_date set not null;

alter table cads.alert_audit alter column partition_date set not null;

alter table cads.alert_mst alter column partition_date set not null;

alter table cads.alert_notes alter column partition_date set not null;

alter table cads.business_on_boarding alter column partition_date set not null;

alter table cads.business_role alter column partition_date set not null;

alter table cads.business_units alter column partition_date set not null;

alter table cads.business_user alter column partition_date set not null;

alter table cads.match_notes alter column partition_date set not null;

alter table cads.notes_audit alter column partition_date set not null;



SELECT * FROM cads.partition_table_list
        WHERE schema_name = 'cads' AND enabled = true
        ORDER BY table_name;

-- Grant EXECUTE on the specific function signature (replace your_executing_user)
GRANT EXECUTE ON FUNCTION partman.create_parent(text,text,text,text,integer,boolean,text,boolean,boolean) TO wf_owner;

-- Or grant on ALL functions in the schema (simpler but broader)
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA partman TO wf_owner;
GRANT USAGE ON SCHEMA partman TO wf_owner; -- Also need USAGE on the schema

SELECT partman.create_parent(
    p_parent_table := 'cads.alert_attachments',
    p_control := 'partition_date',
    p_type := 'native',
    p_interval := '1 day'::interval,
    p_premake := 1,
    p_start_partition := date_trunc('day', now())::timestamptz
);

ALTER EXTENSION pg_partman UPDATE;


with recursive inh as (
   select i.inhrelid, null::text as parent
   from pg_catalog.pg_inherits i
     join pg_catalog.pg_class cl on i.inhparent = cl.oid
     join pg_catalog.pg_namespace nsp on cl.relnamespace = nsp.oid
   where nsp.nspname = 'cads'
     and cl.relname = 'alert_attachments'
   union all
   select i.inhrelid, (i.inhparent::regclass)::text
   from inh
   join pg_catalog.pg_inherits i on (inh.inhrelid = i.inhparent)
)
select c.relname as partition_name,
        pg_get_expr(c.relpartbound, c.oid, true) as partition_expression
from inh
   join pg_catalog.pg_class c on inh.inhrelid = c.oid
   join pg_catalog.pg_namespace n on c.relnamespace = n.oid
   left join pg_partitioned_table p on p.partrelid = c.oid
order by n.nspname, c.relname;

SET search_path TO partman, public;

select * from cads.databasechangelog;

select * from cads.alert_mst;

delete from cads.alert_mst;

drop table cads.alert_mst cascade;