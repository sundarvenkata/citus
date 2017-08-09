\c - - - :follower_port

-- the follower is in streaming replication, you can't do anything
CREATE TABLE the_table (a int, b int);

\c - - - :master_port

SELECT 1 FROM master_add_node('localhost', :worker_1_port);
SELECT 1 FROM master_add_node('localhost', :worker_2_port);

CREATE TABLE the_table (a int, b int);
SELECT create_distributed_table('the_table', 'a');

INSERT INTO the_table (a, b) VALUES (1, 1);
INSERT INTO the_table (a, b) VALUES (1, 2);

\c - - - :follower_port

\d
-- this should work, we're still sending queries to the primaries
SELECT * FROM the_table;

-- this is :follower_port but substitution doesn't work here
\c "port=57700 dbname=regression options='-c\ citus.use_secondary_nodes=always'"

-- this should fail because we haven't added any secondaries
SELECT * FROM the_table;

\c - - - :master_port

SELECT 1 FROM master_add_node('localhost', :follower_worker_1_port,
  groupid => (SELECT groupid FROM pg_dist_node WHERE nodeport = :worker_1_port),
  noderole => 'secondary');
SELECT 1 FROM master_add_node('localhost', :follower_worker_2_port,
  groupid => (SELECT groupid FROM pg_dist_node WHERE nodeport = :worker_2_port),
  noderole => 'secondary');

-- this is :follower_port but substitution doesn't work here
\c "port=57700 dbname=regression options='-c\ citus.use_secondary_nodes=always'"

-- now that we've added secondaries this should work
SELECT * FROM the_table;
