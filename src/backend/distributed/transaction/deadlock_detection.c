/*-------------------------------------------------------------------------
 *
 * deadlock_detection.c
 *
 *  Functions for performing distributed deadlock detection.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "access/hash.h"
#include "distributed/backend_data.h"
#include "distributed/deadlock_detection.h"
#include "distributed/hash_helpers.h"
#include "distributed/lock_graph.h"
#include "distributed/metadata_cache.h"
#include "distributed/transaction_identifier.h"
#include "nodes/pg_list.h"
#include "utils/hsearch.h"


typedef struct TransactionNode
{
	DistributedTransactionId transactionId;

	/* list of TransactionNode that this distributed transaction is waiting for */
	List *waitsFor;

	bool *deadlocked;
	bool visited;
} TransactionNode;


static HTAB * FlattenWaitGraph(WaitGraph *waitGraph);
static uint32 DistributedTransactionIdHash(const void *key, Size keysize);
static int DistributedTransactionIdCompare(const void *a, const void *b, Size keysize);
static TransactionNode * GetTransactionNode(HTAB *distributedTransactionHash,
											DistributedTransactionId *transactionId);


/*
 * DetectDistributedDeadlocks detects whether distributed transactions started
 * from this node are involved in any distributed deadlock and kills transactions
 * to stop the deadlock if that is the case.
 */
bool
DetectDistributedDeadlocks(void)
{
	WaitGraph *waitGraph = NULL;
	HTAB *distributedTransactionHash = NULL;
	TransactionNode *currentNode = NULL;
	int localNodeId = GetLocalGroupId();
	int currentBackend = 0;

	waitGraph = BuildGlobalWaitGraph();
	distributedTransactionHash = FlattenWaitGraph(waitGraph);

	/* go through all local processes, process the ones in a distributed transaction */
	for (currentBackend = 0; currentBackend < MaxBackends; currentBackend++)
	{
		BackendData backendData;
		PGPROC *localProcess = NULL;
		HASH_SEQ_STATUS status;
		TransactionNode *resetNode = NULL;
		List *todoList = NIL;

		localProcess = &ProcGlobal->allProcs[currentBackend];

		/* skip if the PGPROC slot is unused */
		if (localProcess->pid == 0)
		{
			continue;
		}

		GetBackendDataForProc(localProcess, &backendData);

		/* skip if not a coordinator process */
		if (!backendData.isCoordinator)
		{
			continue;
		}

		/* get the distributed transaction for the local process */
		currentNode = GetTransactionNode(distributedTransactionHash,
										 &backendData.transactionId);

		/* reset all visited fields */
		hash_seq_init(&status, distributedTransactionHash);

		while ((resetNode = (TransactionNode *) hash_seq_search(&status)) != 0)
		{
			resetNode->visited = false;
		}

		currentNode->visited = true;

		/* start with transactions the current node is waiting for */
		todoList = list_copy(currentNode->waitsFor);

		while (todoList != NIL)
		{
			TransactionNode *visitNode = linitial(todoList);
			ListCell *recurseCell = NULL;

			todoList = list_delete_first(todoList);

			/* found the original node, meaning there's a deadlock */
			if (visitNode == currentNode)
			{
				ereport(WARNING, (errmsg("killing the transaction running in process %d "
										 "a to resolve a distributed deadlock",
										 localProcess->pid)));

				KillTransactionDueToDeadlock(localProcess);
				return true;
			}

			/* don't revisit nodes */
			if (visitNode->visited)
			{
				continue;
			}

			visitNode->visited = true;

			/* prepend to the list to perform depth-first search */
			foreach(recurseCell, visitNode->waitsFor)
			{
				todoList = lcons(lfirst(recurseCell), todoList);
			}
		}
	}

	return false;
}


/*
 * FlattenWaitGraph convers a wait graph which is represented as a set of
 * wait edges between all backends in the Citus cluster, to a set of nodes,
 * one for each distributed transaction.
 */
static HTAB *
FlattenWaitGraph(WaitGraph *waitGraph)
{
	HASHCTL info;
	uint32 hashFlags = 0;
	HTAB *distributedTransactionHash = NULL;
	int edgeIndex = 0;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(DistributedTransactionId);
	info.entrysize = sizeof(TransactionNode);
	info.hash = DistributedTransactionIdHash;
	info.match = DistributedTransactionIdCompare;
	info.hcxt = CurrentMemoryContext;
	hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_COMPARE);

	distributedTransactionHash = hash_create("deadlock detection", 64, &info, hashFlags);

	for (edgeIndex = 0; edgeIndex < waitGraph->edgeCount; edgeIndex++)
	{
		WaitEdge *edge = &waitGraph->edges[edgeIndex];
		TransactionNode *waitingTransaction = NULL;
		TransactionNode *blockingTransaction = NULL;

		DistributedTransactionId waitingId = {
			edge->waitingNodeId,
			edge->waitingTransactionNum,
			edge->waitingTransactionStamp
		};

		DistributedTransactionId blockingId = {
			edge->blockingNodeId,
			edge->blockingTransactionNum,
			edge->blockingTransactionStamp
		};

		waitingTransaction = GetTransactionNode(distributedTransactionHash, &waitingId);
		blockingTransaction = GetTransactionNode(distributedTransactionHash, &blockingId);

		waitingTransaction->waitsFor = lappend(waitingTransaction->waitsFor, blockingTransaction);
	}

	return distributedTransactionHash;
}


static uint32
DistributedTransactionIdHash(const void *key, Size keysize)
{
	DistributedTransactionId *entry = (DistributedTransactionId *) key;
	uint32 hash = 0;

	hash = hash_uint32(entry->initiatorNodeIdentifier);
	hash = hash_combine(hash, hash_any((unsigned char *) &entry->transactionNumber,
									   sizeof(int64)));
	hash = hash_combine(hash, hash_any((unsigned char *) &entry->timestamp,
									   sizeof(TimestampTz)));

	return hash;
}



static int
DistributedTransactionIdCompare(const void *a, const void *b, Size keysize)
{
	DistributedTransactionId *xactIdA = (DistributedTransactionId *) a;
	DistributedTransactionId *xactIdB = (DistributedTransactionId *) b;

	/* NB: Not used for sorting, just equality... */
	if (xactIdA->initiatorNodeIdentifier != xactIdB->initiatorNodeIdentifier ||
		xactIdA->transactionNumber != xactIdB->transactionNumber ||
		xactIdA->timestamp != xactIdB->timestamp)
	{
		return 1;
	}
	else
	{
		return 0;
	}
}


static TransactionNode *
GetTransactionNode(HTAB *distributedTransactionHash,
					   DistributedTransactionId *transactionId)
{
	bool found = false;
	TransactionNode *backend =

	backend = (TransactionNode *) hash_search(distributedTransactionHash, transactionId,
											  HASH_ENTER, &found);
	if (!found)
	{
		backend->waitsFor = NIL;
		backend->visited = false;
	}

	return backend;
}
