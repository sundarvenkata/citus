/*-------------------------------------------------------------------------
 *
 * create_distributed_relation.c
 *	  Routines relation to the creation of distributed relations.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "access/genam.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/pg_am.h"
#include "catalog/pg_constraint_fn.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_trigger.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "commands/trigger.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/distribution_column.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_copy.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_utility.h"
#include "distributed/pg_dist_colocation.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/reference_table_utils.h"
#include "distributed/remote_commands.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_transaction.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "parser/parse_expr.h"
#include "parser/parse_node.h"
#include "parser/parse_relation.h"
#include "parser/parser.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/inval.h"


/* Replication model to use when creating distributed tables */
int ReplicationModel = REPLICATION_MODEL_COORDINATOR;


/* local function forward declarations */
static char AppropriateReplicationModel(char distributionMethod, int replicationFactor,
										bool viaDeprecatedAPI);
static void CreateHashDistributedTableShards(Oid relationId, Oid colocatedTableId,
											 bool localTableEmpty);
static void EnsureDistributionConfiguration(Oid relationId, Var *distributionColumn,
											char distributionMethod, uint32 colocationId,
											char replicationModel, bool viaDeprecatedAPI);
static void EnsureTableCanBeColocatedWith(Oid relationId, char replicationModel,
										  Oid distributionColumnType,
										  Oid sourceRelationId);
static void EnsureSchemaExistsOnAllNodes(Oid relationId);
static void EnsureLocalTableEmpty(Oid relationId);
static void EnsureTableNotDistributed(Oid relationId);
static char LookupDistributionMethod(Oid distributionMethodOid);
static Oid SupportFunctionForColumn(Var *partitionColumn, Oid accessMethodId,
									int16 supportFunctionNumber);
static void EnsureLocalTableEmptyIfNecessary(Oid relationId, char distributionMethod,
											 bool viaDepracatedAPI);
static bool LocalTableEmpty(Oid tableId);
static uint32 ColocationIdForNewTable(Oid relationId, Oid distributionColumnType,
									  char distributionMethod, char replicationModel,
									  char *colocateWithTableName);
static void CopyLocalDataIntoShards(Oid relationId);
static List * TupleDescColumnNameList(TupleDesc tupleDescriptor);
static bool RelationUsesIdentityColumns(TupleDesc relationDesc);

/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(master_create_distributed_table);
PG_FUNCTION_INFO_V1(create_distributed_table);
PG_FUNCTION_INFO_V1(create_reference_table);


/*
 * master_create_distributed_table accepts a table, distribution column and
 * method and performs the corresponding catalog changes.
 *
 * Note that this UDF is deprecated and cannot create colocated tables, so we
 * always use INVALID_COLOCATION_ID.
 */
Datum
master_create_distributed_table(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	text *distributionColumnText = PG_GETARG_TEXT_P(1);
	Oid distributionMethodOid = PG_GETARG_OID(2);

	char *distributionColumnName = NULL;
	Var *distributionColumn = NULL;
	char distributionMethod = 0;
	char colocationId = INVALID_COLOCATION_ID;
	bool viaDeprecatedAPI = true;

	Relation relation = NULL;

	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	/*
	 * Lock target relation with an exclusive lock - there's no way to make
	 * sense of this table until we've committed, and we don't want multiple
	 * backends manipulating this relation.
	 */
	relation = relation_open(relationId, ExclusiveLock);

	/*
	 * We need to call this here to ensure we are dealing with a table, more
	 * detailed checks will be perform in CreateDistributedTable.
	 */
	EnsureRelationKindSupported(relationId);

	distributionColumnName = text_to_cstring(distributionColumnText);
	distributionColumn = BuildDistributionKeyFromColumnName(relation,
															distributionColumnName);
	distributionMethod = LookupDistributionMethod(distributionMethodOid);

	CreateDistributedTable(relationId, distributionColumn, distributionMethod,
						   colocationId, viaDeprecatedAPI);

	relation_close(relation, NoLock);

	PG_RETURN_VOID();
}


/*
 * create_distributed_table gets a table name, distribution column,
 * distribution method and colocate_with option, then it creates a
 * distributed table.
 */
Datum
create_distributed_table(PG_FUNCTION_ARGS)
{
	Oid relationId = InvalidOid;
	text *distributionColumnText = NULL;
	Oid distributionMethodOid = InvalidOid;
	text *colocateWithTableNameText = NULL;

	Relation relation = NULL;
	char *distributionColumnName = NULL;
	Var *distributionColumn = NULL;
	AttrNumber columnIndex = InvalidAttrNumber;
	Oid distributionColumnType = InvalidOid;
	char distributionMethod = 0;

	char *colocateWithTableName = NULL;
	uint32 colocationId = INVALID_COLOCATION_ID;

	bool viaDeprecatedAPI = false;

	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	relationId = PG_GETARG_OID(0);
	distributionColumnText = PG_GETARG_TEXT_P(1);
	distributionMethodOid = PG_GETARG_OID(2);
	colocateWithTableNameText = PG_GETARG_TEXT_P(3);

	/*
	 * At the moment we only take lock to prevent DROP TABLE and ALTER TABLE,
	 * so that creating distribution column info is safe. More aggressive locks
	 * will be taken at CreateDistributedTable.
	 */
	relation = relation_open(relationId, AccessShareLock);

	/*
	 * We need to call this here to ensure we are dealing with a table, more
	 * detailed checks will be perform in CreateDistributedTable.
	 */
	EnsureRelationKindSupported(relationId);

	distributionColumnName = text_to_cstring(distributionColumnText);
	distributionColumn = BuildDistributionKeyFromColumnName(relation,
															distributionColumnName);
	columnIndex = get_attnum(relationId, distributionColumnName);
	distributionColumnType = get_atttype(relationId, columnIndex);
	distributionMethod = LookupDistributionMethod(distributionMethodOid);

	colocateWithTableName = text_to_cstring(colocateWithTableNameText);

	/*
	 * ColocationIdForNewTable returns a colocation id according to given
	 * constraints. If there is no such colocation group, it will create a one.
	 */
	colocationId = ColocationIdForNewTable(relationId, distributionColumnType,
										   distributionMethod, ReplicationModel,
										   colocateWithTableName);

	CreateDistributedTable(relationId, distributionColumn, distributionMethod,
						   colocationId, viaDeprecatedAPI);

	relation_close(relation, NoLock);

	PG_RETURN_VOID();
}


/*
 * CreateReferenceTable creates a distributed table with the given relationId. The
 * created table has one shard and replication factor is set to the active worker
 * count. In fact, the above is the definition of a reference table in Citus.
 */
Datum
create_reference_table(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);

	Relation relation = NULL;
	uint32 colocationId = INVALID_COLOCATION_ID;
	List *workerNodeList = NIL;
	int workerCount = 0;
	Var *distributionColumn = NULL;

	bool viaDeprecatedAPI = false;

	EnsureCoordinator();
	CheckCitusVersion(ERROR);

	/*
	 * At the moment we only take lock to prevent DROP TABLE and ALTER TABLE,
	 * so that creating distribution column info is safe. More aggressive locks
	 * will be taken at CreateDistributedTable.
	 */
	relation = relation_open(relationId, AccessShareLock);

	/*
	 * We need to call this here to ensure we are dealing with a table, more
	 * detailed checks will be perform in CreateDistributedTable.
	 */
	EnsureRelationKindSupported(relationId);

	workerNodeList = ActivePrimaryNodeList();
	workerCount = list_length(workerNodeList);

	/* if there are no workers, error out */
	if (workerCount == 0)
	{
		char *relationName = get_rel_name(relationId);

		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("cannot create reference table \"%s\"", relationName),
						errdetail("There are no active worker nodes.")));
	}

	colocationId = CreateReferenceTableColocationId();
	CreateDistributedTable(relationId, distributionColumn, DISTRIBUTE_BY_NONE,
						   colocationId, viaDeprecatedAPI);

	relation_close(relation, NoLock);

	PG_RETURN_VOID();
}


/*
 * CreateDistributedTable creates distributed table in the given configuration.
 * It took care of locking, replication settings, colocation, metadata creation,
 * creating shards and copying local data to shards.
 */
void
CreateDistributedTable(Oid relationId, Var *distributionColumn, char distributionMethod,
					   uint32 colocationId, bool viaDeprecatedAPI)
{
	Relation relation = NULL;

	char replicationModel = REPLICATION_MODEL_INVALID;
	int replicationFactor = ShardReplicationFactor;
	Oid colocatedTableId = INVALID_COLOCATION_ID;
	bool localTableEmpty = false;

	/*
	 * Lock target relation with an exclusive lock - there's no way to make
	 * sense of this table until we've committed, and we don't want multiple
	 * backends manipulating this relation.
	 */
	relation = relation_open(relationId, ExclusiveLock);

	EnsureDistributionConfiguration(relationId, distributionColumn, distributionMethod,
									colocationId, replicationModel, viaDeprecatedAPI);

	/* we need to calculate these variables before creating distributed metadata */
	localTableEmpty = LocalTableEmpty(relationId);
	colocatedTableId = ColocatedTableId(colocationId);
	if (colocatedTableId != InvalidOid)
	{
		replicationFactor = TableShardReplicationFactor(colocatedTableId);
	}
	replicationModel = AppropriateReplicationModel(distributionMethod, replicationFactor,
												   viaDeprecatedAPI);

	/* create an entry for distributed table in pg_dist_partition */
	InsertIntoPgDistPartition(relationId, distributionMethod, distributionColumn,
							  colocationId, replicationModel);

	/* foreign tables does not support TRUNCATE trigger */
	if (RegularTable(relationId))
	{
		CreateTruncateTrigger(relationId);
	}

	/* if we are using master_create_distributed_table, we don't need to continue */
	if (viaDeprecatedAPI)
	{
		relation_close(relation, NoLock);

		return;
	}

	/* create shards for hash distributed and reference tables */
	if (distributionMethod == DISTRIBUTE_BY_HASH)
	{
		CreateHashDistributedTableShards(relationId, colocatedTableId, localTableEmpty);
	}
	else if (distributionMethod == DISTRIBUTE_BY_NONE)
	{
		CreateReferenceTableShard(relationId);
	}

	/* copy over data for hash distributed and reference tables */
	if (distributionMethod == DISTRIBUTE_BY_HASH ||
		distributionMethod == DISTRIBUTE_BY_NONE)
	{
		if (RegularTable(relationId))
		{
			CopyLocalDataIntoShards(relationId);
		}
	}

	if (ShouldSyncTableMetadata(relationId))
	{
		CreateTableMetadataOnWorkers(relationId);
	}

	relation_close(relation, NoLock);
}


/*
 * AppropriateReplicationModel function returns appropriate replication model
 * depending on distributionMethod and global ReplicationModel variable
 */
static char
AppropriateReplicationModel(char distributionMethod, int replicationFactor,
							bool viaDeprecatedAPI)
{
	if (viaDeprecatedAPI)
	{
		if (ReplicationModel != REPLICATION_MODEL_COORDINATOR)
		{
			ereport(NOTICE, (errmsg("using statement-based replication"),
							 errdetail("The current replication_model setting is "
									   "'streaming', which is not supported by "
									   "master_create_distributed_table."),
							 errhint("Use create_distributed_table to use the streaming "
									 "replication model.")));
		}

		return REPLICATION_MODEL_COORDINATOR;
	}
	else if (distributionMethod == DISTRIBUTE_BY_NONE)
	{
		return REPLICATION_MODEL_2PC;
	}
	else if (distributionMethod == DISTRIBUTE_BY_HASH)
	{
		EnsureReplicationSettings(InvalidOid, ReplicationModel, replicationFactor);
		return ReplicationModel;
	}
	else
	{
		if (ReplicationModel != REPLICATION_MODEL_COORDINATOR)
		{
			ereport(NOTICE, (errmsg("using statement-based replication"),
							 errdetail("Streaming replication is supported only for "
									   "hash-distributed tables.")));
		}

		return REPLICATION_MODEL_COORDINATOR;
	}

	/* we should not reach to this point */
	return REPLICATION_MODEL_INVALID;
}


/*
 * CreateHashDistributedTableShards creates shards of given hash distributed table.
 */
static void
CreateHashDistributedTableShards(Oid relationId, Oid colocatedTableId,
								 bool localTableEmpty)
{
	bool useExclusiveConnection = false;

	/*
	 * Ensure schema exists on each worker node. We can not run this function
	 * transactionally, since we may create shards over separate sessions and
	 * shard creation depends on the schema being present and visible from all
	 * sessions.
	 */
	EnsureSchemaExistsOnAllNodes(relationId);

	if (RegularTable(relationId))
	{
		useExclusiveConnection = IsTransactionBlock() || !localTableEmpty;
	}

	if (colocatedTableId != InvalidOid)
	{
		CreateColocatedShards(relationId, colocatedTableId, useExclusiveConnection);
	}
	else
	{
		CreateShardsWithRoundRobinPolicy(relationId, ShardCount, ShardReplicationFactor,
										 useExclusiveConnection);
	}
}


/*
 * EnsureDistributionConfiguration checks whether Citus can support creation of
 * distributed tables in given configuration. If it cannot, we simply error out.
 */
static void
EnsureDistributionConfiguration(Oid relationId, Var *distributionColumn,
								char distributionMethod, uint32 colocationId,
								char replicationModel, bool viaDeprecatedAPI)
{
	Relation relation = NULL;
	TupleDesc relationDesc = NULL;
	char *relationName = NULL;

	relation = relation_open(relationId, AccessShareLock);

	EnsureTableOwner(relationId);
	EnsureTableNotDistributed(relationId);
	EnsureLocalTableEmptyIfNecessary(relationId, distributionMethod, viaDeprecatedAPI);

	relationDesc = RelationGetDescr(relation);
	relationName = RelationGetRelationName(relation);

	/* verify target relation does not use WITH (OIDS) PostgreSQL feature */
	if (relationDesc->tdhasoid)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot distribute relation: %s", relationName),
						errdetail("Distributed relations must not specify the WITH "
								  "(OIDS) option in their definitions.")));
	}

	/* verify target relation does not use identity columns */
	if (RelationUsesIdentityColumns(relationDesc))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot distribute relation: %s", relationName),
						errdetail("Distributed relations must not use GENERATED "
								  "... AS IDENTITY.")));
	}

	/* check for support function needed by specified partition method */
	if (distributionMethod == DISTRIBUTE_BY_HASH)
	{
		Oid hashSupportFunction = SupportFunctionForColumn(distributionColumn,
														   HASH_AM_OID, HASHPROC);
		if (hashSupportFunction == InvalidOid)
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
							errmsg("could not identify a hash function for type %s",
								   format_type_be(distributionColumn->vartype)),
							errdatatype(distributionColumn->vartype),
							errdetail("Partition column types must have a hash function "
									  "defined to use hash partitioning.")));
		}
	}
	else if (distributionMethod == DISTRIBUTE_BY_RANGE)
	{
		Oid btreeSupportFunction = SupportFunctionForColumn(distributionColumn,
															BTREE_AM_OID, BTORDER_PROC);
		if (btreeSupportFunction == InvalidOid)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("could not identify a comparison function for type %s",
							format_type_be(distributionColumn->vartype)),
					 errdatatype(distributionColumn->vartype),
					 errdetail("Partition column types must have a comparison function "
							   "defined to use range partitioning.")));
		}
	}

	ErrorIfUnsupportedConstraint(relation, distributionMethod, distributionColumn,
								 colocationId);

	relation_close(relation, NoLock);
}


/*
 * EnsureTableCanBeColocatedWith checks whether a relation can be created with given
 * distribution configuration so that it would be colocated with given table in
 * colocateWithTableName parameter.
 */
static void
EnsureTableCanBeColocatedWith(Oid relationId, char replicationModel,
							  Oid distributionColumnType, Oid sourceRelationId)
{
	DistTableCacheEntry *sourceTableEntry = DistributedTableCacheEntry(sourceRelationId);
	char sourceDistributionMethod = sourceTableEntry->partitionMethod;
	char sourceReplicationModel = sourceTableEntry->replicationModel;
	Var *sourceDistributionColumn = DistPartitionKey(sourceRelationId);
	Oid sourceDistributionColumnType = InvalidOid;

	/* reference tables have NULL distribution column */
	if (sourceDistributionColumn != NULL)
	{
		sourceDistributionColumnType = sourceDistributionColumn->vartype;
	}

	if (sourceDistributionMethod != DISTRIBUTE_BY_HASH)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot distribute relation"),
						errdetail(
							"Currently, colocate_with option is only supported "
							"for hash distributed tables.")));
	}

	if (sourceReplicationModel != replicationModel)
	{
		char *relationName = get_rel_name(relationId);
		char *sourceRelationName = get_rel_name(sourceRelationId);

		ereport(ERROR, (errmsg("cannot colocate tables %s and %s",
							   sourceRelationName, relationName),
						errdetail("Replication models don't match for %s and %s.",
								  sourceRelationName, relationName)));
	}

	if (sourceDistributionColumnType != distributionColumnType)
	{
		char *relationName = get_rel_name(relationId);
		char *sourceRelationName = get_rel_name(sourceRelationId);

		ereport(ERROR, (errmsg("cannot colocate tables %s and %s",
							   sourceRelationName, relationName),
						errdetail("Distribution column types don't match for "
								  "%s and %s.", sourceRelationName,
								  relationName)));
	}
}


/*
 * EnsureSchemaExistsOnAllNodes connects to all nodes with citus extension user
 * and creates the schema of the given relationId. The function errors out if the
 * command cannot be executed in any of the worker nodes.
 */
static void
EnsureSchemaExistsOnAllNodes(Oid relationId)
{
	List *workerNodeList = ActivePrimaryNodeList();
	ListCell *workerNodeCell = NULL;
	StringInfo applySchemaCreationDDL = makeStringInfo();

	Oid schemaId = get_rel_namespace(relationId);
	const char *createSchemaDDL = CreateSchemaDDLCommand(schemaId);
	uint64 connectionFlag = FORCE_NEW_CONNECTION;

	if (createSchemaDDL == NULL)
	{
		return;
	}

	appendStringInfo(applySchemaCreationDDL, "%s", createSchemaDDL);

	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		char *nodeName = workerNode->workerName;
		uint32 nodePort = workerNode->workerPort;
		MultiConnection *connection =
			GetNodeUserDatabaseConnection(connectionFlag, nodeName, nodePort, NULL,
										  NULL);

		ExecuteCriticalRemoteCommand(connection, applySchemaCreationDDL->data);
	}
}


/*
 * In cases where we can send local data to shards, we allow non-empty local
 * tables. EnsureLocalTableEmptyIfNecessary only performs emptiness checks
 * if we cannot sent local data to shards.
 */
static void
EnsureLocalTableEmptyIfNecessary(Oid relationId, char distributionMethod,
								 bool viaDepracatedAPI)
{
	if (viaDepracatedAPI)
	{
		EnsureLocalTableEmpty(relationId);
	}
	if (distributionMethod != DISTRIBUTE_BY_HASH &&
		distributionMethod != DISTRIBUTE_BY_NONE)
	{
		EnsureLocalTableEmpty(relationId);
	}
	else if (!RegularTable(relationId))
	{
		EnsureLocalTableEmpty(relationId);
	}
}


/*
 * EnsureLocalTableEmpty errors out if the local table is not empty.
 */
static void
EnsureLocalTableEmpty(Oid relationId)
{
	char *relationName = get_rel_name(relationId);
	bool localTableEmpty = LocalTableEmpty(relationId);

	if (!localTableEmpty)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						errmsg("cannot distribute relation \"%s\"", relationName),
						errdetail("Relation \"%s\" contains data.", relationName),
						errhint("Empty your table before distributing it.")));
	}
}


/*
 * EnsureTableNotDistributed errors out if the table is distributed.
 */
static void
EnsureTableNotDistributed(Oid relationId)
{
	char *relationName = get_rel_name(relationId);
	bool isDistributedTable = false;

	isDistributedTable = IsDistributedTable(relationId);

	if (isDistributedTable)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						errmsg("table \"%s\" is already distributed",
							   relationName)));
	}
}


/*
 * Check that the current replication factor setting is compatible with the
 * replication model of relationId, if valid. If InvalidOid, check that the
 * global replication model setting instead. Errors out if an invalid state
 * is detected.
 */
void
EnsureReplicationSettings(Oid relationId, char replicationModel, int replicationFactor)
{
	char *msgSuffix = "the streaming replication model";
	char *extraHint = " or setting \"citus.replication_model\" to \"statement\"";

	if (relationId != InvalidOid)
	{
		msgSuffix = "tables which use the streaming replication model";
		extraHint = "";
	}

	if (replicationModel == REPLICATION_MODEL_STREAMING && replicationFactor != 1)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("replication factors above one are incompatible with %s",
							   msgSuffix),
						errhint("Try again after reducing \"citus.shard_replication_"
								"factor\" to one%s.", extraHint)));
	}
}


/*
 * LookupDistributionMethod maps the oids of citus.distribution_type enum
 * values to pg_dist_partition.partmethod values.
 *
 * The passed in oid has to belong to a value of citus.distribution_type.
 */
static char
LookupDistributionMethod(Oid distributionMethodOid)
{
	HeapTuple enumTuple = NULL;
	Form_pg_enum enumForm = NULL;
	char distributionMethod = 0;
	const char *enumLabel = NULL;

	enumTuple = SearchSysCache1(ENUMOID, ObjectIdGetDatum(distributionMethodOid));
	if (!HeapTupleIsValid(enumTuple))
	{
		ereport(ERROR, (errmsg("invalid internal value for enum: %u",
							   distributionMethodOid)));
	}

	enumForm = (Form_pg_enum) GETSTRUCT(enumTuple);
	enumLabel = NameStr(enumForm->enumlabel);

	if (strncmp(enumLabel, "append", NAMEDATALEN) == 0)
	{
		distributionMethod = DISTRIBUTE_BY_APPEND;
	}
	else if (strncmp(enumLabel, "hash", NAMEDATALEN) == 0)
	{
		distributionMethod = DISTRIBUTE_BY_HASH;
	}
	else if (strncmp(enumLabel, "range", NAMEDATALEN) == 0)
	{
		distributionMethod = DISTRIBUTE_BY_RANGE;
	}
	else
	{
		ereport(ERROR, (errmsg("invalid label for enum: %s", enumLabel)));
	}

	ReleaseSysCache(enumTuple);

	return distributionMethod;
}


/*
 *	SupportFunctionForColumn locates a support function given a column, an access method,
 *	and and id of a support function. This function returns InvalidOid if there is no
 *	support function for the operator class family of the column, but if the data type
 *	of the column has no default operator class whatsoever, this function errors out.
 */
static Oid
SupportFunctionForColumn(Var *partitionColumn, Oid accessMethodId,
						 int16 supportFunctionNumber)
{
	Oid operatorFamilyId = InvalidOid;
	Oid supportFunctionOid = InvalidOid;
	Oid operatorClassInputType = InvalidOid;
	Oid columnOid = partitionColumn->vartype;
	Oid operatorClassId = GetDefaultOpClass(columnOid, accessMethodId);

	/* currently only support using the default operator class */
	if (operatorClassId == InvalidOid)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("data type %s has no default operator class for specified"
							   " partition method", format_type_be(columnOid)),
						errdatatype(columnOid),
						errdetail("Partition column types must have a default operator"
								  " class defined.")));
	}

	operatorFamilyId = get_opclass_family(operatorClassId);
	operatorClassInputType = get_opclass_input_type(operatorClassId);
	supportFunctionOid = get_opfamily_proc(operatorFamilyId, operatorClassInputType,
										   operatorClassInputType,
										   supportFunctionNumber);

	return supportFunctionOid;
}


/*
 * LocalTableEmpty function checks whether given local table contains any row and
 * returns false if there is any data. This function is only for local tables and
 * should not be called for distributed tables.
 */
static bool
LocalTableEmpty(Oid tableId)
{
	Oid schemaId = get_rel_namespace(tableId);
	char *schemaName = get_namespace_name(schemaId);
	char *tableName = get_rel_name(tableId);
	char *tableQualifiedName = quote_qualified_identifier(schemaName, tableName);

	int spiConnectionResult = 0;
	int spiQueryResult = 0;
	StringInfo selectExistQueryString = makeStringInfo();

	HeapTuple tuple = NULL;
	Datum hasDataDatum = 0;
	bool localTableEmpty = false;
	bool columnNull = false;
	bool readOnly = true;

	int rowId = 0;
	int attributeId = 1;

	AssertArg(!IsDistributedTable(tableId));

	spiConnectionResult = SPI_connect();
	if (spiConnectionResult != SPI_OK_CONNECT)
	{
		ereport(ERROR, (errmsg("could not connect to SPI manager")));
	}

	appendStringInfo(selectExistQueryString, SELECT_EXIST_QUERY, tableQualifiedName);

	spiQueryResult = SPI_execute(selectExistQueryString->data, readOnly, 0);
	if (spiQueryResult != SPI_OK_SELECT)
	{
		ereport(ERROR, (errmsg("execution was not successful \"%s\"",
							   selectExistQueryString->data)));
	}

	/* we expect that SELECT EXISTS query will return single value in a single row */
	Assert(SPI_processed == 1);

	tuple = SPI_tuptable->vals[rowId];
	hasDataDatum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, attributeId, &columnNull);
	localTableEmpty = !DatumGetBool(hasDataDatum);

	SPI_finish();

	return localTableEmpty;
}


/*
 * CreateTruncateTrigger creates a truncate trigger on table identified by relationId
 * and assigns citus_truncate_trigger() as handler.
 */
void
CreateTruncateTrigger(Oid relationId)
{
	CreateTrigStmt *trigger = NULL;
	StringInfo triggerName = makeStringInfo();
	bool internal = true;

	appendStringInfo(triggerName, "truncate_trigger");

	trigger = makeNode(CreateTrigStmt);
	trigger->trigname = triggerName->data;
	trigger->relation = NULL;
	trigger->funcname = SystemFuncName("citus_truncate_trigger");
	trigger->args = NIL;
	trigger->row = false;
	trigger->timing = TRIGGER_TYPE_BEFORE;
	trigger->events = TRIGGER_TYPE_TRUNCATE;
	trigger->columns = NIL;
	trigger->whenClause = NULL;
	trigger->isconstraint = false;

	CreateTrigger(trigger, NULL, relationId, InvalidOid, InvalidOid, InvalidOid,
				  internal);
}


/*
 * ColocationIdForNewTable returns a colocation id for hash-distributed table
 * according to given configuration. If there is no such configuration, it
 * creates one and returns colocation id of newly created colocation group.
 * This function should not be called for reference tables, instead you can
 * use CreateReferenceTableColocationId. For append and range distributed
 * tables, this function directly returns INVALID_COLOCATION_ID.
 */
static uint32
ColocationIdForNewTable(Oid relationId, Oid distributionColumnType,
						char distributionMethod, char replicationModel,
						char *colocateWithTableName)
{
	Relation relation = NULL;
	Relation pgDistColocation = NULL;
	uint32 colocationId = INVALID_COLOCATION_ID;

	/* reference tables get their colocation id via CreateReferenceTableColocationId */
	Assert(distributionMethod != DISTRIBUTE_BY_NONE);

	if (distributionMethod == DISTRIBUTE_BY_APPEND ||
		distributionMethod == DISTRIBUTE_BY_RANGE)
	{
		if (pg_strncasecmp(colocateWithTableName, "default", NAMEDATALEN) != 0)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot distribute relation"),
							errdetail("Currently, colocate_with option is only supported "
									  "for hash distributed tables.")));
		}
		return colocationId;
	}

	/*
	 * Get an access share lock on the relation to prevent DROP TABLE and
	 * ALTER TABLE
	 */
	relation = relation_open(relationId, AccessShareLock);

	/*
	 * Get an exclusive lock on the colocation system catalog. Therefore, we
	 * can be sure that there will no modifications on the colocation table
	 * until this transaction is committed.
	 */
	pgDistColocation = heap_open(DistColocationRelationId(), ExclusiveLock);

	if (distributionMethod == DISTRIBUTE_BY_HASH)
	{
		if (pg_strncasecmp(colocateWithTableName, "default", NAMEDATALEN) == 0)
		{
			/* check for default colocation group */
			colocationId = ColocationId(ShardCount, ShardReplicationFactor,
										distributionColumnType);

			if (colocationId == INVALID_COLOCATION_ID)
			{
				colocationId = CreateColocationGroup(ShardCount, ShardReplicationFactor,
													 distributionColumnType);
			}
		}
		else if (pg_strncasecmp(colocateWithTableName, "none", NAMEDATALEN) == 0)
		{
			colocationId = GetNextColocationId();
		}
		else
		{
			text *colocateWithTableNameText = cstring_to_text(colocateWithTableName);
			Oid sourceRelationId = ResolveRelationId(colocateWithTableNameText);

			EnsureTableCanBeColocatedWith(relationId, replicationModel,
										  distributionColumnType, sourceRelationId);

			colocationId = TableColocationId(sourceRelationId);
		}
	}

	heap_close(pgDistColocation, NoLock);
	heap_close(relation, NoLock);

	return colocationId;
}


/*
 * RegularTable function returns true if given table's relation kind is RELKIND_RELATION
 * (or RELKIND_PARTITIONED_TABLE for PG >= 10), otherwise it returns false.
 */
bool
RegularTable(Oid relationId)
{
	char relationKind = get_rel_relkind(relationId);

#if (PG_VERSION_NUM >= 100000)
	if (relationKind == RELKIND_RELATION || relationKind == RELKIND_PARTITIONED_TABLE)
#else
	if (relationKind == RELKIND_RELATION)
#endif
	{
		return true;
	}

	return false;
}

/*
 * CopyLocalDataIntoShards copies data from the local table, which is hidden
 * after converting it to a distributed table, into the shards of the distributed
 * table.
 *
 * This function uses CitusCopyDestReceiver to invoke the distributed COPY logic.
 * We cannot use a regular COPY here since that cannot read from a table. Instead
 * we read from the table and pass each tuple to the CitusCopyDestReceiver which
 * opens a connection and starts a COPY for each shard placement that will have
 * data.
 *
 * We could call the planner and executor here and send the output to the
 * DestReceiver, but we are in a tricky spot here since Citus is already
 * intercepting queries on this table in the planner and executor hooks and we
 * want to read from the local table. To keep it simple, we perform a heap scan
 * directly on the table.
 *
 * Any writes on the table that are started during this operation will be handled
 * as distributed queries once the current transaction commits. SELECTs will
 * continue to read from the local table until the current transaction commits,
 * after which new SELECTs will be handled as distributed queries.
 *
 * After copying local data into the distributed table, the local data remains
 * in place and should be truncated at a later time.
 */
static void
CopyLocalDataIntoShards(Oid distributedRelationId)
{
	DestReceiver *copyDest = NULL;
	List *columnNameList = NIL;
	Relation distributedRelation = NULL;
	TupleDesc tupleDescriptor = NULL;
	bool stopOnFailure = true;

	EState *estate = NULL;
	HeapScanDesc scan = NULL;
	HeapTuple tuple = NULL;
	ExprContext *econtext = NULL;
	MemoryContext oldContext = NULL;
	TupleTableSlot *slot = NULL;
	uint64 rowsCopied = 0;

	/* take an ExclusiveLock to block all operations except SELECT */
	distributedRelation = heap_open(distributedRelationId, ExclusiveLock);

	/*
	 * All writes have finished, make sure that we can see them by using the
	 * latest snapshot. We use GetLatestSnapshot instead of
	 * GetTransactionSnapshot since the latter would not reveal all writes
	 * in serializable or repeatable read mode. Note that subsequent reads
	 * from the distributed table would reveal those writes, temporarily
	 * violating the isolation level. However, this seems preferable over
	 * dropping the writes entirely.
	 */
	PushActiveSnapshot(GetLatestSnapshot());

	/* get the table columns */
	tupleDescriptor = RelationGetDescr(distributedRelation);
	slot = MakeSingleTupleTableSlot(tupleDescriptor);
	columnNameList = TupleDescColumnNameList(tupleDescriptor);

	/* initialise per-tuple memory context */
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	econtext->ecxt_scantuple = slot;

	copyDest =
		(DestReceiver *) CreateCitusCopyDestReceiver(distributedRelationId,
													 columnNameList, estate,
													 stopOnFailure);

	/* initialise state for writing to shards, we'll open connections on demand */
	copyDest->rStartup(copyDest, 0, tupleDescriptor);

	/* begin reading from local table */
	scan = heap_beginscan(distributedRelation, GetActiveSnapshot(), 0, NULL);

	oldContext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		/* materialize tuple and send it to a shard */
		ExecStoreTuple(tuple, slot, InvalidBuffer, false);
		copyDest->receiveSlot(slot, copyDest);

		/* clear tuple memory */
		ResetPerTupleExprContext(estate);

		/* make sure we roll back on cancellation */
		CHECK_FOR_INTERRUPTS();

		if (rowsCopied == 0)
		{
			ereport(NOTICE, (errmsg("Copying data from local table...")));
		}

		rowsCopied++;

		if (rowsCopied % 1000000 == 0)
		{
			ereport(DEBUG1, (errmsg("Copied %ld rows", rowsCopied)));
		}
	}

	if (rowsCopied % 1000000 != 0)
	{
		ereport(DEBUG1, (errmsg("Copied %ld rows", rowsCopied)));
	}

	MemoryContextSwitchTo(oldContext);

	/* finish reading from the local table */
	heap_endscan(scan);

	/* finish writing into the shards */
	copyDest->rShutdown(copyDest);

	/* free memory and close the relation */
	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);
	heap_close(distributedRelation, NoLock);

	PopActiveSnapshot();
}


/*
 * TupleDescColumnNameList returns a list of column names for the given tuple
 * descriptor as plain strings.
 */
static List *
TupleDescColumnNameList(TupleDesc tupleDescriptor)
{
	List *columnNameList = NIL;
	int columnIndex = 0;

	for (columnIndex = 0; columnIndex < tupleDescriptor->natts; columnIndex++)
	{
		Form_pg_attribute currentColumn = tupleDescriptor->attrs[columnIndex];
		char *columnName = NameStr(currentColumn->attname);

		if (currentColumn->attisdropped)
		{
			continue;
		}

		columnNameList = lappend(columnNameList, columnName);
	}

	return columnNameList;
}


/*
 * RelationUsesIdentityColumns returns whether a given relation uses the SQL
 * GENERATED ... AS IDENTITY features supported as of PostgreSQL 10.
 */
static bool
RelationUsesIdentityColumns(TupleDesc relationDesc)
{
#if (PG_VERSION_NUM >= 100000)
	int attributeIndex = 0;

	for (attributeIndex = 0; attributeIndex < relationDesc->natts; attributeIndex++)
	{
		Form_pg_attribute attributeForm = relationDesc->attrs[attributeIndex];

		if (attributeForm->attidentity != '\0')
		{
			return true;
		}
	}
#endif

	return false;
}
