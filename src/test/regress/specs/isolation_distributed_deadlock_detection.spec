setup
{
  SELECT citus.replace_isolation_tester_func();
  SELECT citus.refresh_isolation_tester_prepared_statement();

  CREATE TABLE deadlock_detection_test (user_id int, some_val int);
  SELECT create_distributed_table('deadlock_detection_test', 'user_id');

  INSERT INTO deadlock_detection_test VALUES (1,1);
  INSERT INTO deadlock_detection_test VALUES (2,2);
}

teardown
{
  DROP TABLE deadlock_detection_test;
  SELECT citus.restore_isolation_tester_func();
}

session "s1"

step "s1-set-deadlock-prevention"
{
	SET citus.enable_deadlock_prevention TO off;
	
    -- we don't want Postgres deadlock detection to kick in
    SET deadlock_timeout TO '20min';
}

step "s1-update-1"
{
  BEGIN;
  UPDATE deadlock_detection_test SET some_val = 15 WHERE user_id = 1;
}

step "s1-update-2"
{
  UPDATE deadlock_detection_test SET some_val = 15 WHERE user_id = 2;
}

step "s1-finish"
{
  COMMIT;
}

session "s2"

step "s2-set-deadlock-prevention"
{
	SET citus.enable_deadlock_prevention TO off;

    -- we don't want Postgres deadlock detection to kick in
    SET deadlock_timeout TO '20min';
}

step "s2-update-1"
{
  UPDATE deadlock_detection_test SET some_val = 15 WHERE user_id = 1;
}

step "s2-update-2"
{
  BEGIN;
  UPDATE deadlock_detection_test SET some_val = 15 WHERE user_id = 2;
}

step "s2-finish"
{
  COMMIT;
}


permutation "s1-set-deadlock-prevention" "s2-set-deadlock-prevention" "s1-update-1" "s2-update-2" "s2-update-1" "s1-update-2" "s1-finish" "s2-finish"
