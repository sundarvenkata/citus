setup
{
  SELECT citus.replace_isolation_tester_func();
  SELECT citus.refresh_isolation_tester_prepared_statement();

  CREATE TABLE deadlock_detection_test (user_id int, some_val int);
  SELECT create_distributed_table('deadlock_detection_test', 'user_id');

  INSERT INTO deadlock_detection_test VALUES (1,1);
  INSERT INTO deadlock_detection_test VALUES (2,2);
  INSERT INTO deadlock_detection_test VALUES (3,3);
  INSERT INTO deadlock_detection_test VALUES (4,4);
  INSERT INTO deadlock_detection_test VALUES (5,5);
  INSERT INTO deadlock_detection_test VALUES (6,6);
  INSERT INTO deadlock_detection_test VALUES (7,7);
}

teardown
{
  DROP TABLE deadlock_detection_test;
  SELECT citus.restore_isolation_tester_func();
}

session "s1"

step "s1-begin"
{
  BEGIN;
}

step "s1-update-1"
{
  UPDATE deadlock_detection_test SET some_val = 1 WHERE user_id = 1;
}

step "s1-update-2"
{
  UPDATE deadlock_detection_test SET some_val = 1 WHERE user_id = 2;
}

step "s1-update-3"
{
  UPDATE deadlock_detection_test SET some_val = 1 WHERE user_id = 3;
}

step "s1-update-4"
{
  UPDATE deadlock_detection_test SET some_val = 1 WHERE user_id = 4;
}

step "s1-update-5"
{
  UPDATE deadlock_detection_test SET some_val = 1 WHERE user_id = 5;
}

step "s1-finish"
{
  COMMIT;
}

session "s2"

step "s2-begin"
{
  BEGIN;
}

step "s2-update-1"
{
  UPDATE deadlock_detection_test SET some_val = 2 WHERE user_id = 1;
}

step "s2-update-2"
{
  UPDATE deadlock_detection_test SET some_val = 2 WHERE user_id = 2;
}

step "s2-update-3"
{
  UPDATE deadlock_detection_test SET some_val = 2 WHERE user_id = 3;
}

step "s2-finish"
{
  COMMIT;
}

session "s3"

step "s3-begin"
{
  BEGIN;
}

step "s3-update-1"
{
  UPDATE deadlock_detection_test SET some_val = 3 WHERE user_id = 1;
}

step "s3-update-2"
{
  UPDATE deadlock_detection_test SET some_val = 3 WHERE user_id = 2;
}

step "s3-update-3"
{
  UPDATE deadlock_detection_test SET some_val = 3 WHERE user_id = 3;
}

step "s3-update-4"
{
  UPDATE deadlock_detection_test SET some_val = 3 WHERE user_id = 4;
}

step "s3-finish"
{
  COMMIT;
}

session "s4"

step "s4-begin"
{
  BEGIN;
}

step "s4-update-1"
{
  UPDATE deadlock_detection_test SET some_val = 4 WHERE user_id = 1;
}

step "s4-update-2"
{
  UPDATE deadlock_detection_test SET some_val = 4 WHERE user_id = 2;
}

step "s4-update-3"
{
  UPDATE deadlock_detection_test SET some_val = 4 WHERE user_id = 3;
}

step "s4-update-4"
{
  UPDATE deadlock_detection_test SET some_val = 4 WHERE user_id = 4;
}

step "s4-update-5"
{
  UPDATE deadlock_detection_test SET some_val = 4 WHERE user_id = 5;
}

step "s4-update-6"
{
  UPDATE deadlock_detection_test SET some_val = 4 WHERE user_id = 6;
}

step "s4-update-7"
{
  UPDATE deadlock_detection_test SET some_val = 4 WHERE user_id = 7;
}

step "s4-finish"
{
  COMMIT;
}

session "s5"

step "s5-begin"
{
  BEGIN;
}

step "s5-update-1"
{
  UPDATE deadlock_detection_test SET some_val = 5 WHERE user_id = 1;
}

step "s5-update-2"
{
  UPDATE deadlock_detection_test SET some_val = 5 WHERE user_id = 2;
}

step "s5-update-3"
{
  UPDATE deadlock_detection_test SET some_val = 5 WHERE user_id = 3;
}

step "s5-update-4"
{
  UPDATE deadlock_detection_test SET some_val = 5 WHERE user_id = 4;
}

step "s5-update-5"
{
  UPDATE deadlock_detection_test SET some_val = 5 WHERE user_id = 5;
}

step "s5-update-6"
{
  UPDATE deadlock_detection_test SET some_val = 5 WHERE user_id = 6;
}

step "s5-update-7"
{
  UPDATE deadlock_detection_test SET some_val = 5 WHERE user_id = 7;
}

step "s5-finish"
{
  COMMIT;
}

session "s6"

step "s6-begin"
{
  BEGIN;
}

step "s6-update-1"
{
  UPDATE deadlock_detection_test SET some_val = 6 WHERE user_id = 1;
}

step "s6-update-2"
{
  UPDATE deadlock_detection_test SET some_val = 6 WHERE user_id = 2;
}

step "s6-update-3"
{
  UPDATE deadlock_detection_test SET some_val = 6 WHERE user_id = 3;
}

step "s6-update-4"
{
  UPDATE deadlock_detection_test SET some_val = 6 WHERE user_id = 4;
}

step "s6-update-5"
{
  UPDATE deadlock_detection_test SET some_val = 6 WHERE user_id = 5;
}

step "s6-update-6"
{
  UPDATE deadlock_detection_test SET some_val = 6 WHERE user_id = 6;
}

step "s6-update-7"
{
  UPDATE deadlock_detection_test SET some_val = 6 WHERE user_id = 7;
}

step "s6-finish"
{
  COMMIT;
}

# we disable the deamon during the regression tests in order to get consistent results
# thus we manually issue the deadlock detection 
session "deadlock-checker"

# we issue the checker not only when there are deadlocks to ensure that we never cancel
# backend inappropriately
step "deadlock-checker-call"
{
  SELECT check_distributed_deadlocks();
}

# simplest case, loop with two nodes
permutation "s1-begin" "s2-begin" "s1-update-1" "s2-update-2" "s2-update-1" "deadlock-checker-call" "s1-update-2"  "deadlock-checker-call" "s1-finish" "s2-finish"

# slightly more complex case, loop with three nodes
permutation "s1-begin" "s2-begin" "s3-begin"  "s1-update-1" "s2-update-2" "s3-update-3" "deadlock-checker-call" "s1-update-2" "s2-update-3" "s3-update-1" "deadlock-checker-call" "s3-finish" "s2-finish" "s1-finish"

# similar to the above (i.e., 3 nodes), but the cycle starts from the second node 
permutation "s1-begin" "s2-begin" "s3-begin"  "s2-update-1" "s1-update-1" "s2-update-2" "s3-update-3" "s3-update-2" "deadlock-checker-call" "s2-update-3" "deadlock-checker-call" "s3-finish" "s2-finish" "s1-finish"

# not connected graph
permutation "s1-begin" "s2-begin" "s3-begin" "s4-begin" "s1-update-1" "s2-update-2" "s3-update-3" "s3-update-2" "deadlock-checker-call" "s4-update-4" "s2-update-3" "deadlock-checker-call" "s3-finish" "s2-finish" "s1-finish" "s4-finish"

# still a not connected graph, but each smaller graph contains dependencies, one of which is a distributed deadlock
permutation "s1-begin" "s2-begin" "s3-begin" "s4-begin" "s4-update-1" "s1-update-1" "deadlock-checker-call" "s2-update-2" "s3-update-3" "s2-update-3" "s3-update-2" "deadlock-checker-call" "s3-finish" "s2-finish" "s4-finish" "s1-finish"

#  multiple deadlocks on a not connected graph
permutation "s1-begin" "s2-begin" "s3-begin" "s4-begin" "s1-update-1" "s4-update-4" "s2-update-2" "s3-update-3" "s3-update-2" "s4-update-1" "s1-update-4" "deadlock-checker-call" "s1-finish" "s4-finish" "s2-update-3" "deadlock-checker-call"  "s2-finish" "s3-finish" 

# a larger graph where the first node is in the distributed deadlock
permutation "s1-begin" "s2-begin" "s3-begin" "s4-begin" "s5-begin" "s6-begin" "s1-update-1" "s5-update-5" "s3-update-2" "s2-update-3" "s4-update-4" "s3-update-4" "deadlock-checker-call" "s6-update-6" "s4-update-6" "s1-update-5" "s5-update-1" "deadlock-checker-call" "s1-finish" "s5-finish" "s6-finish" "s4-finish" "s3-finish" "s2-finish"
 
# a larger graph where the deadlock starts from a middle node
permutation "s1-begin" "s2-begin" "s3-begin" "s4-begin" "s5-begin" "s6-begin" "s6-update-6" "s5-update-5" "s5-update-6" "s4-update-4" "s1-update-4" "s4-update-5" "deadlock-checker-call" "s2-update-3" "s3-update-2" "s2-update-2" "s3-update-3" "deadlock-checker-call" "s6-finish" "s5-finish" "s4-finish" "s1-finish" "s3-finish" "s2-finish" 

# a larger graph where the deadlock starts from the last node
permutation "s1-begin" "s2-begin" "s3-begin" "s4-begin" "s5-begin" "s6-begin" "s5-update-5" "s3-update-2" "s2-update-2" "s4-update-4" "s3-update-4" "s4-update-5" "s1-update-4" "deadlock-checker-call" "s6-update-6" "s5-update-6" "s6-update-5" "deadlock-checker-call" "s5-finish" "s6-finish" "s4-finish" "s3-finish"  "s1-finish" "s2-finish"

