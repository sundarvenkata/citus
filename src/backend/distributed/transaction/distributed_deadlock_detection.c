/*-------------------------------------------------------------------------
 *
 * distributed_deadlock_detection.c
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
#include "distributed/distributed_deadlock_detection.h"
#include "distributed/hash_helpers.h"
#include "distributed/listutils.h"
#include "distributed/lock_graph.h"
#include "distributed/metadata_cache.h"
#include "distributed/transaction_identifier.h"
#include "nodes/pg_list.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"


static bool CheckDeadlockForTransactionNode(TransactionNode *startingTransactionNode,
											List **deadlockPath);
static void FindDeadlockPath(List *waitingTransactionList, List **deadlockPath);
static void RemoveElementsUntilTransactionNodeFound(List *waitingTransactionList,
													TransactionNode *transactionNode);
static void ResetVisitedFields(HTAB *adjacencyList);
static void AssocateDistributedTransactionWithBackendProc(TransactionNode *
														  transactionNode);
static TransactionNode * GetOrCreateTransactionNode(HTAB *adjacencyList,
													DistributedTransactionId *
													transactionId);
static uint32 DistributedTransactionIdHash(const void *key, Size keysize);
static int DistributedTransactionIdCompareHash(const void *a, const void *b, Size
											   keysize);
static int DistributedTransactionIdCompare(const void *a, const void *b);


PG_FUNCTION_INFO_V1(check_distributed_deadlocks);


/*
 * check_distributed_deadlocks is the external API for manually
 * checking for distributed deadlocks. For the details, see
 * CheckForDistributedDeadlocks().
 */
Datum
check_distributed_deadlocks(PG_FUNCTION_ARGS)
{
	bool deadlockFound = CheckForDistributedDeadlocks();

	return BoolGetDatum(deadlockFound);
}


/*
 * CheckForDistributedDeadlocks is the entry point for detecing
 * distributed deadlocks.
 *
 * In plain words, the function first builds a wait graph by
 * adding the wait edges from the local node and then adding the
 * remote wait edges to form a global wait graph. Later, the wait
 * graph is converted into another graph representation (adjacency
 * lists) for more efficient searches. Finally, a DFS is done on
 * the adjacency lists. Finding a cycle in the graph unveils a
 * distributed deadlock. Upon finding a deadlock, the youngest
 * participant backend is killed.
 *
 * The function returns true if a deadlock is found. Otherwise, returns
 * false.
 */
bool
CheckForDistributedDeadlocks(void)
{
	WaitGraph *waitGraph = BuildGlobalWaitGraph();
	HTAB *adjacencyLists = BuildAdjacencyListsForWaitGraph(waitGraph);
	HASH_SEQ_STATUS status;
	TransactionNode *transactionNode = NULL;

	/*
	 * We iterate on transaction nodes and search for deadlocks where the
	 * starting node is the given transaction node.
	 */
	hash_seq_init(&status, adjacencyLists);
	while ((transactionNode = (TransactionNode *) hash_seq_search(&status)) != 0)
	{
		bool deadlockFound = false;
		List *deadlockPath = NIL;

		/* we're only interested in finding deadlocks originating from this node */
		if (transactionNode->transactionId.initiatorNodeIdentifier != GetLocalGroupId())
		{
			continue;
		}

		ResetVisitedFields(adjacencyLists);

		deadlockFound = CheckDeadlockForTransactionNode(transactionNode, &deadlockPath);
		if (deadlockFound)
		{
			TransactionNode *youngestTransaction = transactionNode;
			ListCell *participantTransactionCell = NULL;

			/* there should be at least two transactions to get into a deadlock */
			Assert(list_length(deadlockPath) > 1);

			/*
			 * We search for the youngest participant for two reasons
			 * (i) predictable results (ii) kill the youngest transaction
			 * (i.e., if a DDL continues for 1 hour and deadlocks with a
			 * SELECT continues for 10 msec, we prefer to kill the SELECT).
			 */
			foreach(participantTransactionCell, deadlockPath)
			{
				TransactionNode *currentNode =
					(TransactionNode *) lfirst(participantTransactionCell);

				AssocateDistributedTransactionWithBackendProc(currentNode);

				if (youngestTransaction->transactionId.timestamp >
					currentNode->transactionId.timestamp)
				{
					youngestTransaction = currentNode;
				}
			}

			/* we should find the backend */
			Assert(youngestTransaction->initiatorProc != NULL);

			KillBackendDueToDeadlock(youngestTransaction->initiatorProc);

			hash_seq_term(&status);

			return true;
		}
	}

	return false;
}


/*
 * CheckDeadlockForDistributedTransaction does a DFS starting with the given
 * transaction node and checks for a cycle (i.e., the node can be reached again
 * while traversing the graph).
 *
 * Finding a cycle  indicates a distributed deadlock and the function returns
 * true on that case.
 */
static bool
CheckDeadlockForTransactionNode(TransactionNode *startingTransactionNode,
								List **deadlockPath)
{
	List *waitingTransactionNodes = startingTransactionNode->waitsFor;

	/* traverse the graph */
	while (waitingTransactionNodes != NIL)
	{
		TransactionNode *waitingTransactionNode =
			(TransactionNode *) linitial(waitingTransactionNodes);
		ListCell *currentWaitForCell = NULL;

		waitingTransactionNodes = list_delete_first(waitingTransactionNodes);

		/* cycle found, let the caller know about the cycle */
		if (waitingTransactionNode == startingTransactionNode)
		{
			*deadlockPath = list_make1(waitingTransactionNode);

			FindDeadlockPath(waitingTransactionNodes, deadlockPath);

			return true;
		}

		/* don't need to revisit the node again */
		if (waitingTransactionNode->transactionVisited)
		{
			continue;
		}

		waitingTransactionNode->transactionVisited = true;

		/* prepend to the list to continue depth-first search */
		foreach(currentWaitForCell, waitingTransactionNode->waitsFor)
		{
			TransactionNode *waitForTransaction = (TransactionNode *) lfirst(
				currentWaitForCell);

			/*
			 * We add both the waiting node and its waiter to simplify finding the
			 * deadlock path.
			 */
			waitingTransactionNodes =
				lcons(waitingTransactionNode, waitingTransactionNodes);
			waitingTransactionNodes = lcons(waitForTransaction, waitingTransactionNodes);
		}
	}

	return false;
}


/*
 * FindDeadlockPath gets a waitingTransactionList which involves a deadlock
 * (i.e., cycle in the graph) and fills the deadlockPath by backtracing on
 * the waiting list.
 *
 * The function iterates over the waiting list, adds the element to the
 * deadlock path and removes all other branches that starts with the
 * given node on the waiting graph.
 */
static void
FindDeadlockPath(List *waitingTransactionList, List **deadlockPath)
{
	while (waitingTransactionList != NIL)
	{
		TransactionNode *waitingTransaction = linitial(waitingTransactionList);

		*deadlockPath = lappend(*deadlockPath, waitingTransaction);

		waitingTransactionList = list_delete_first(waitingTransactionList);
		RemoveElementsUntilTransactionNodeFound(waitingTransactionList,
												waitingTransaction);
	}
}


/*
 * RemoveElementsUntilTransactionNodeFound gets a transaction list and a
 * transactionNode. The function iterates on the list and removes all the
 * nodes until the transactionNode is found. There could be more than one
 * transactionNodes and the function removes all of the paths that reaches
 * the given transactionNode.
 */
static void
RemoveElementsUntilTransactionNodeFound(List *waitingTransactionList,
										TransactionNode *transactionNode)
{
	int toBeDeletedNodeCount = 0;
	int nodeIndex = 0;

	while (waitingTransactionList != NIL)
	{
		TransactionNode *waitingTransaction = linitial(waitingTransactionList);

		++toBeDeletedNodeCount;

		if (waitingTransaction->transactionId.transactionNumber ==
			transactionNode->transactionId.transactionNumber)
		{
			for (nodeIndex = 0; nodeIndex < toBeDeletedNodeCount; ++nodeIndex)
			{
				waitingTransactionList = list_delete_first(waitingTransactionList);
			}

			/* we'll search other occurences of the transactionNode */
			toBeDeletedNodeCount = 0;
		}
	}
}


/*
 * ResetVisitedFields goes over all the elements of the input adjacency list
 * and sets transactionVisited to false.
 */
static void
ResetVisitedFields(HTAB *adjacencyList)
{
	HASH_SEQ_STATUS status;
	TransactionNode *resetNode = NULL;

	/* reset all visited fields */
	hash_seq_init(&status, adjacencyList);

	while ((resetNode = (TransactionNode *) hash_seq_search(&status)) != 0)
	{
		resetNode->transactionVisited = false;
	}
}


/*
 * AssocateDistributedTransactionWithBackendProc gets a transaction node
 * and searches the corresponding backend. Once found, transactionNodes'
 * initiatorProc is set to it.
 *
 * The function goes over all the backends, checks for the backend with
 * the same transaction number as the given transaction node.
 */
static void
AssocateDistributedTransactionWithBackendProc(TransactionNode *transactionNode)
{
	int backendIndex = 0;

	for (backendIndex = 0; backendIndex < MaxBackends; ++backendIndex)
	{
		PGPROC *currentProc = &ProcGlobal->allProcs[backendIndex];
		BackendData currentBackendData;
		DistributedTransactionId *currentTransactionId = NULL;

		/* we're not interested in processes that are not active or waiting on a lock */
		if (currentProc->pid <= 0)
		{
			continue;
		}

		GetBackendDataForProc(currentProc, &currentBackendData);

		/* we're only interested in distribtued transactions */
		if (!IsInDistributedTransaction(&currentBackendData))
		{
			continue;
		}

		currentTransactionId = &currentBackendData.transactionId;

		if (currentTransactionId->transactionNumber !=
			transactionNode->transactionId.transactionNumber)
		{
			continue;
		}

		/* at the point we should only have transactions initiated by this node */
		Assert(currentTransactionId->initiatorNodeIdentifier == GetLocalGroupId());

		transactionNode->initiatorProc = currentProc;

		break;
	}
}


/*
 * BuildAdjacencyListsForWaitGraph converts the input wait graph to
 * an adjacency list for further processing.
 *
 * The input wait graph consists of set of wait edges between all
 * backends in the Citus cluster.
 *
 * We represent the adjacency list with an HTAB structure. Each node is
 * represented with a DistributedTransactionId and each edge is represented with
 * a TransactionNode structure.
 *
 * While iterating over the input wait edges, we follow the algorithm
 * below:
 *    for each edge in waitGraph:
 *      - find the corresponding nodes for waiting and
 *        blocking transactions in the adjacency list
 *          - if not found, add new node(s) to the list
 *      - Add blocking transaction to the waiting transaction's waitFor
 *        list
 *
 *  The format of the adjacency list becomes the following:
 *      [transactionId] = [transactionNode->waitsFor {list of waiting transaction nodes}]
 */
HTAB *
BuildAdjacencyListsForWaitGraph(WaitGraph *waitGraph)
{
	HASHCTL info;
	uint32 hashFlags = 0;
	HTAB *adjacencyList = NULL;
	int edgeIndex = 0;
	int edgeCount = waitGraph->edgeCount;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(DistributedTransactionId);
	info.entrysize = sizeof(TransactionNode);
	info.hash = DistributedTransactionIdHash;
	info.match = DistributedTransactionIdCompareHash;
	info.hcxt = CurrentMemoryContext;
	hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_COMPARE);

	adjacencyList = hash_create("distributed deadlock detection", 64, &info, hashFlags);

	for (edgeIndex = 0; edgeIndex < edgeCount; edgeIndex++)
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

		waitingTransaction =
			GetOrCreateTransactionNode(adjacencyList, &waitingId);
		blockingTransaction =
			GetOrCreateTransactionNode(adjacencyList, &blockingId);

		waitingTransaction->waitsFor = lappend(waitingTransaction->waitsFor,
											   blockingTransaction);
	}

	return adjacencyList;
}


/*
 * GetOrCreateTransactionNode searches distributedTransactionHash for the given
 * given transactionId. If the transaction is not found, a new transaction node
 * with the given transaction identifier is added.
 */
static TransactionNode *
GetOrCreateTransactionNode(HTAB *adjacencyList, DistributedTransactionId *transactionId)
{
	TransactionNode *transactionNode = NULL;
	bool found = false;

	transactionNode = (TransactionNode *) hash_search(adjacencyList, transactionId,
													  HASH_ENTER, &found);
	if (!found)
	{
		transactionNode->waitsFor = NIL;
	}

	return transactionNode;
}


/*
 * DistributedTransactionIdHash returns hashed value for a given distributed
 * transaction id.
 */
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


/*
 * Just a wrapper around DistributedTransactionIdCompare(). Used for hash compare
 * function thus requires a different signature.
 */
static int
DistributedTransactionIdCompareHash(const void *a, const void *b, Size keysize)
{
	return DistributedTransactionIdCompare(a, b);
}


/*
 * DistributedTransactionIdCompare compares DistributedTransactionId's a and b
 * and returns -1 if a < b, 1 if a > b, 0 if they are equal.
 *
 * DistributedTransactionId are first compared by their timestamp, then transaction
 * number, then node identifier.
 */
static int
DistributedTransactionIdCompare(const void *a, const void *b)
{
	DistributedTransactionId *xactIdA = (DistributedTransactionId *) a;
	DistributedTransactionId *xactIdB = (DistributedTransactionId *) b;

	if (!TimestampDifferenceExceeds(xactIdB->timestamp, xactIdA->timestamp, 0))
	{
		/* ! (B <= A) = A < B */
		return -1;
	}
	else if (!TimestampDifferenceExceeds(xactIdA->timestamp, xactIdB->timestamp, 0))
	{
		/* ! (A <= B) = A > B */
		return 1;
	}
	else if (xactIdA->transactionNumber < xactIdB->transactionNumber)
	{
		return -1;
	}
	else if (xactIdA->transactionNumber > xactIdB->transactionNumber)
	{
		return 1;
	}
	else if (xactIdA->initiatorNodeIdentifier < xactIdB->initiatorNodeIdentifier)
	{
		return -1;
	}
	else if (xactIdA->initiatorNodeIdentifier > xactIdB->initiatorNodeIdentifier)
	{
		return 1;
	}
	else
	{
		return 0;
	}
}
