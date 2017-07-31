/* citus--7.0-8--7.0-9 */

SET search_path = 'pg_catalog';

CREATE FUNCTION check_distributed_deadlocks()
RETURNS BOOL
LANGUAGE 'c' STRICT
AS $$MODULE_PATHNAME$$, $$check_distributed_deadlocks$$;
COMMENT ON FUNCTION check_distributed_deadlocks()
IS 'does a distributed deadlock check, if a deadlock found kills one of the participating backends and returns true ';

RESET search_path; 

