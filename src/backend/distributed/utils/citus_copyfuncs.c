/*-------------------------------------------------------------------------
 *
 * citus_readfuncs.c
 *    Citus specific node functions
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2012-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"


#if (PG_VERSION_NUM >= 90600)


#include "distributed/citus_nodefuncs.h"
#include "utils/datum.h"


/*
 * Macros to simplify copying of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.  Note that these
 * hard-wire the convention that the local variables in a Copy routine are
 * named 'newnode' and 'from'.
 */
static inline Node *
CitusSetTag(Node *node, int tag)
{
	CitusNode *citus_node = (CitusNode *) node;
	citus_node->citus_tag = tag;
	return node;
}


#define CITUS_COPY_LOCALS(nodeTypeName) \
	nodeTypeName * newnode = (nodeTypeName *) \
							 CitusSetTag((Node *) newnodeE, T_ ## nodeTypeName); \
	nodeTypeName *from = (nodeTypeName *) oldnodeE

/* Copy a simple scalar field (int, float, bool, enum, etc) */
#define COPY_SCALAR_FIELD(fldname) \
	(newnode->fldname = from->fldname)

/* Copy a field that is a pointer to some kind of Node or Node tree */
#define COPY_NODE_FIELD(fldname) \
	(newnode->fldname = copyObject(from->fldname))

/* Copy a field that is a pointer to a Bitmapset */
#define COPY_BITMAPSET_FIELD(fldname) \
	(newnode->fldname = bms_copy(from->fldname))

/* Copy a field that is a pointer to a C string, or perhaps NULL */
#define COPY_STRING_FIELD(fldname) \
	(newnode->fldname = from->fldname ? pstrdup(from->fldname) : (char *) NULL)

/* Copy a field that is a pointer to a simple palloc'd object of size sz */
#define COPY_POINTER_FIELD(fldname, sz) \
	do { \
		Size _size = (sz); \
		newnode->fldname = palloc(_size); \
		memcpy(newnode->fldname, from->fldname, _size); \
	} while (0)

/* Copy a parse location field (for Copy, this is same as scalar case) */
#define COPY_LOCATION_FIELD(fldname) \
	(newnode->fldname = from->fldname)


static void
copyJobInfo(Job *newnode, Job *from)
{
	CitusSetTag((Node *) newnode, T_Job);
	COPY_SCALAR_FIELD(jobId);
	COPY_NODE_FIELD(jobQuery);
	COPY_NODE_FIELD(taskList);
	COPY_NODE_FIELD(dependedJobList);
	COPY_SCALAR_FIELD(subqueryPushdown);
	COPY_SCALAR_FIELD(requiresMasterEvaluation);
	COPY_SCALAR_FIELD(deferredPruning);
}


void
CopyNodeJob(COPYFUNC_ARGS)
{
	CITUS_COPY_LOCALS(Job);

	copyJobInfo(newnode, from);
}


void
CopyNodeMultiPlan(COPYFUNC_ARGS)
{
	CITUS_COPY_LOCALS(MultiPlan);

	COPY_SCALAR_FIELD(operation);
	COPY_SCALAR_FIELD(hasReturning);

	COPY_NODE_FIELD(workerJob);
	COPY_NODE_FIELD(masterQuery);
	COPY_SCALAR_FIELD(routerExecutable);
	COPY_NODE_FIELD(planningError);
}


void
CopyNodeShardInterval(COPYFUNC_ARGS)
{
	CITUS_COPY_LOCALS(ShardInterval);

	COPY_SCALAR_FIELD(relationId);
	COPY_SCALAR_FIELD(storageType);
	COPY_SCALAR_FIELD(valueTypeId);
	COPY_SCALAR_FIELD(valueTypeLen);
	COPY_SCALAR_FIELD(valueByVal);
	COPY_SCALAR_FIELD(minValueExists);
	COPY_SCALAR_FIELD(maxValueExists);

	if (!from->minValueExists)
	{
		newnode->minValue = datumCopy(from->minValue,
									  from->valueByVal,
									  from->valueTypeLen);
	}
	if (!from->maxValueExists)
	{
		newnode->maxValue = datumCopy(from->maxValue,
									  from->valueByVal,
									  from->valueTypeLen);
	}

	COPY_SCALAR_FIELD(shardId);
}


void
CopyNodeMapMergeJob(COPYFUNC_ARGS)
{
	CITUS_COPY_LOCALS(MapMergeJob);
	int arrayLength;
	int i;

	copyJobInfo(&newnode->job, &from->job);

	COPY_NODE_FIELD(reduceQuery);
	COPY_SCALAR_FIELD(partitionType);
	COPY_NODE_FIELD(partitionColumn);
	COPY_SCALAR_FIELD(partitionCount);
	COPY_SCALAR_FIELD(sortedShardIntervalArrayLength);

	arrayLength = from->sortedShardIntervalArrayLength;

	/* now build & read sortedShardIntervalArray */
	newnode->sortedShardIntervalArray =
		(ShardInterval **) palloc(arrayLength * sizeof(ShardInterval *));

	for (i = 0; i < arrayLength; ++i)
	{
		/* can't use COPY_NODE_FIELD, no field names */
		newnode->sortedShardIntervalArray[i] =
			copyObject(from->sortedShardIntervalArray[i]);
	}

	COPY_NODE_FIELD(mapTaskList);
	COPY_NODE_FIELD(mergeTaskList);
}


void
CopyNodeShardPlacement(COPYFUNC_ARGS)
{
	CITUS_COPY_LOCALS(ShardPlacement);

	COPY_SCALAR_FIELD(placementId);
	COPY_SCALAR_FIELD(shardId);
	COPY_SCALAR_FIELD(shardLength);
	COPY_SCALAR_FIELD(shardState);
	COPY_STRING_FIELD(nodeName);
	COPY_SCALAR_FIELD(nodePort);
	COPY_SCALAR_FIELD(partitionMethod);
	COPY_SCALAR_FIELD(colocationGroupId);
	COPY_SCALAR_FIELD(representativeValue);
}


void
CopyNodeRelationShard(COPYFUNC_ARGS)
{
	CITUS_COPY_LOCALS(RelationShard);
	COPY_SCALAR_FIELD(relationId);
	COPY_SCALAR_FIELD(shardId);
}


void
CopyNodeTask(COPYFUNC_ARGS)
{
	CITUS_COPY_LOCALS(Task);

	COPY_SCALAR_FIELD(taskType);
	COPY_SCALAR_FIELD(jobId);
	COPY_SCALAR_FIELD(taskId);
	COPY_STRING_FIELD(queryString);
	COPY_SCALAR_FIELD(anchorShardId);
	COPY_NODE_FIELD(taskPlacementList);
	COPY_NODE_FIELD(dependedTaskList);
	COPY_SCALAR_FIELD(partitionId);
	COPY_SCALAR_FIELD(upstreamTaskId);
	COPY_NODE_FIELD(shardInterval);
	COPY_SCALAR_FIELD(assignmentConstrained);
	COPY_NODE_FIELD(taskExecution);
	COPY_SCALAR_FIELD(upsertQuery);
	COPY_SCALAR_FIELD(replicationModel);
	COPY_SCALAR_FIELD(insertSelectQuery);
	COPY_NODE_FIELD(relationShardList);
}


void
CopyNodeDeferredErrorMessage(COPYFUNC_ARGS)
{
	CITUS_COPY_LOCALS(DeferredErrorMessage);

	COPY_SCALAR_FIELD(code);
	COPY_STRING_FIELD(message);
	COPY_STRING_FIELD(detail);
	COPY_STRING_FIELD(hint);
	COPY_STRING_FIELD(filename);
	COPY_SCALAR_FIELD(linenumber);
	COPY_STRING_FIELD(functionname);
}


#endif /* PG_VERSION_NUM >= 90600) */
