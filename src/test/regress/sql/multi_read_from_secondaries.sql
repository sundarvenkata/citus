ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1600000;

\c "dbname=regression options='-c\ citus.read_from_secondaries=always'"

CREATE TABLE the_table (a int, b int);

-- attempts to change metadata should fail while reading from secondaries
SELECT create_distributed_table('the_table', 'a');

\c "dbname=regression options='-c\ citus.read_from_secondaries=never'"

SELECT create_distributed_table('the_table', 'a');

INSERT INTO the_table (a, b) VALUES (1, 1);
INSERT INTO the_table (a, b) VALUES (2, 1);

-- simluate actually having secondary nodes
SELECT * FROM pg_dist_node;
UPDATE pg_dist_node SET noderole = 'secondary';

\c "dbname=regression options='-c\ citus.read_from_secondaries=always'"

-- inserts are disallowed
INSERT INTO the_table (a, b) VALUES (1, 2);

-- router selects are allowed
SELECT a FROM the_table WHERE a = 1;

-- real-time selects are not allowed
SELECT a FROM the_table;

\c "dbname=regression options='-c\ citus.read_from_secondaries=never'"
UPDATE pg_dist_node SET noderole = 'primary';
DROP TABLE the_table;
