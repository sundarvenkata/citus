/*
 * backend_data.h
 *
 * Data structure definition for managing backend data and related function
 * declarations.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef BACKEND_DATA_H
#define BACKEND_DATA_H


#include "datatype/timestamp.h"
#include "distributed/transaction_identifier.h"
#include "nodes/pg_list.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/s_lock.h"


/*
 * Each backend's active distributed transaction information is tracked via
 * BackendData in shared memory.
 */
typedef struct BackendData
{
	Oid databaseId;
	slock_t mutex;
	DistributedTransactionId transactionId;
	bool killedDueToDeadlock;
	bool isCoordinator;
} BackendData;


extern void InitializeBackendManagement(void);
extern void InitializeBackendData(void);
extern void UnSetDistributedTransactionId(void);
extern void AssignDistributedTransactionId(void);
extern void GetBackendDataForProc(PGPROC *proc, BackendData *result);
extern void KillTransactionDueToDeadlock(PGPROC *proc);
extern bool GotKilledDueToDeadlock(void);

#endif /* BACKEND_DATA_H */
