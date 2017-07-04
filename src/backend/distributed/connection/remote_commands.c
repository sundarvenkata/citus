/*-------------------------------------------------------------------------
 *
 * remote_commands.c
 *   Helpers to make it easier to execute command on remote nodes.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "pgstat.h"

#include "libpq-fe.h"

#include "distributed/connection_management.h"
#include "distributed/remote_commands.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "storage/latch.h"


/*
 * MultiConnectionWaiter is used to wait for results on a set of connections.
 * The connections array contains all pending connections, starting at currentOffset.
 * After a call to AwaitReadyConnections, any connections which are still
 * pending are moved to the end of the array.
 */
typedef struct MultiConnectionWaiter
{
	/* number of connections to wait for */
	int connectionCount;

	/* connections to wait for, pending connections are moved to the end */
	MultiConnection **connections;

	/* pending connections start at this offset */
	int currentOffset;

	/* pre-allocated event array for WaitEventSetWait results */
	WaitEvent *events;
} MultiConnectionWaiter;


/* GUC, determining whether statements sent to remote nodes are logged */
bool LogRemoteCommands = false;


static bool FinishConnectionIO(MultiConnection *connection, bool raiseInterrupts);
static MultiConnectionWaiter * CreateMultiConnectionWaiter(List *connectionList);
static List * AwaitReadyConnections(MultiConnectionWaiter *waiter,
									bool raiseInterrupts);
static void WaitForReadyConnections(MultiConnectionWaiter *waiter,
									WaitEventSet *waitEventSet, bool *connectionReady,
									bool raiseInterrupts);
static void FreeMultiConnectionWaiter(MultiConnectionWaiter *waiter);


/* simple helpers */

/*
 * IsResponseOK checks whether the result is a successful one.
 */
bool
IsResponseOK(PGresult *result)
{
	ExecStatusType resultStatus = PQresultStatus(result);

	if (resultStatus == PGRES_SINGLE_TUPLE || resultStatus == PGRES_TUPLES_OK ||
		resultStatus == PGRES_COMMAND_OK)
	{
		return true;
	}

	return false;
}


/*
 * ForgetResults clears a connection from pending activity.
 *
 * Note that this might require network IO. If that's not acceptable, use
 * NonblockingForgetResults().
 */
void
ForgetResults(MultiConnection *connection)
{
	while (true)
	{
		PGresult *result = NULL;
		const bool dontRaiseErrors = false;

		result = GetRemoteCommandResult(connection, dontRaiseErrors);
		if (result == NULL)
		{
			break;
		}
		if (PQresultStatus(result) == PGRES_COPY_IN)
		{
			PQputCopyEnd(connection->pgConn, NULL);

			/* TODO: mark transaction as failed, once we can. */
		}
		PQclear(result);
	}
}


/*
 * NonblockingForgetResults clears a connection from pending activity if doing
 * so does not require network IO. Returns true if successful, false
 * otherwise.
 */
bool
NonblockingForgetResults(MultiConnection *connection)
{
	PGconn *pgConn = connection->pgConn;

	if (PQstatus(pgConn) != CONNECTION_OK)
	{
		return false;
	}

	Assert(PQisnonblocking(pgConn));

	while (true)
	{
		PGresult *result = NULL;

		/* just in case there's a lot of results */
		CHECK_FOR_INTERRUPTS();

		/*
		 * If busy, there might still be results already received and buffered
		 * by the OS. As connection is in non-blocking mode, we can check for
		 * that without blocking.
		 */
		if (PQisBusy(pgConn))
		{
			if (PQflush(pgConn) == -1)
			{
				/* write failed */
				return false;
			}
			if (PQconsumeInput(pgConn) == 0)
			{
				/* some low-level failure */
				return false;
			}
		}

		/* clearing would require blocking IO, return */
		if (PQisBusy(pgConn))
		{
			return false;
		}

		result = PQgetResult(pgConn);
		if (PQresultStatus(result) == PGRES_COPY_IN)
		{
			/* in copy, can't reliably recover without blocking */
			return false;
		}

		if (result == NULL)
		{
			return true;
		}

		PQclear(result);
	}

	pg_unreachable();
}


/*
 * SqlStateMatchesCategory returns true if the given sql state (which may be
 * NULL if unknown) is in the given error category. Note that we use
 * ERRCODE_TO_CATEGORY macro to determine error category of the sql state and
 * expect the caller to use the same macro for the error category.
 */
bool
SqlStateMatchesCategory(char *sqlStateString, int category)
{
	bool sqlStateMatchesCategory = false;
	int sqlState = 0;
	int sqlStateCategory = 0;

	if (sqlStateString == NULL)
	{
		return false;
	}

	sqlState = MAKE_SQLSTATE(sqlStateString[0], sqlStateString[1], sqlStateString[2],
							 sqlStateString[3], sqlStateString[4]);

	sqlStateCategory = ERRCODE_TO_CATEGORY(sqlState);
	if (sqlStateCategory == category)
	{
		sqlStateMatchesCategory = true;
	}

	return sqlStateMatchesCategory;
}


/* report errors & warnings */

/*
 * Report libpq failure that's not associated with a result.
 */
void
ReportConnectionError(MultiConnection *connection, int elevel)
{
	char *nodeName = connection->hostname;
	int nodePort = connection->port;

	ereport(elevel, (errmsg("connection error: %s:%d", nodeName, nodePort),
					 errdetail("%s", PQerrorMessage(connection->pgConn))));
}


/*
 * ReportResultError reports libpq failure associated with a result.
 */
void
ReportResultError(MultiConnection *connection, PGresult *result, int elevel)
{
	/* we release PQresult when throwing an error because the caller can't */
	PG_TRY();
	{
		char *sqlStateString = PQresultErrorField(result, PG_DIAG_SQLSTATE);
		char *messagePrimary = PQresultErrorField(result, PG_DIAG_MESSAGE_PRIMARY);
		char *messageDetail = PQresultErrorField(result, PG_DIAG_MESSAGE_DETAIL);
		char *messageHint = PQresultErrorField(result, PG_DIAG_MESSAGE_HINT);
		char *messageContext = PQresultErrorField(result, PG_DIAG_CONTEXT);

		char *nodeName = connection->hostname;
		int nodePort = connection->port;
		int sqlState = ERRCODE_INTERNAL_ERROR;

		if (sqlStateString != NULL)
		{
			sqlState = MAKE_SQLSTATE(sqlStateString[0],
									 sqlStateString[1],
									 sqlStateString[2],
									 sqlStateString[3],
									 sqlStateString[4]);
		}

		/*
		 * If the PGresult did not contain a message, the connection may provide a
		 * suitable top level one. At worst, this is an empty string.
		 */
		if (messagePrimary == NULL)
		{
			char *lastNewlineIndex = NULL;

			messagePrimary = PQerrorMessage(connection->pgConn);
			lastNewlineIndex = strrchr(messagePrimary, '\n');

			/* trim trailing newline, if any */
			if (lastNewlineIndex != NULL)
			{
				*lastNewlineIndex = '\0';
			}
		}

		ereport(elevel, (errcode(sqlState), errmsg("%s", messagePrimary),
						 messageDetail ? errdetail("%s", messageDetail) : 0,
						 messageHint ? errhint("%s", messageHint) : 0,
						 messageContext ? errcontext("%s", messageContext) : 0,
						 errcontext("while executing command on %s:%d",
									nodeName, nodePort)));
	}
	PG_CATCH();
	{
		PQclear(result);
		PG_RE_THROW();
	}
	PG_END_TRY();
}


/*
 * LogRemoteCommand logs commands send to remote nodes if
 * citus.log_remote_commands wants us to do so.
 */
void
LogRemoteCommand(MultiConnection *connection, const char *command)
{
	if (!LogRemoteCommands)
	{
		return;
	}

	ereport(LOG, (errmsg("issuing %s", command),
				  errdetail("on server %s:%d", connection->hostname, connection->port)));
}


/* wrappers around libpq functions, with command logging support */


/*
 * ExecuteCriticalRemoteCommand executes a remote command that is critical
 * to the transaction. If the command fails then the transaction aborts.
 */
void
ExecuteCriticalRemoteCommand(MultiConnection *connection, const char *command)
{
	int querySent = 0;
	PGresult *result = NULL;
	bool raiseInterrupts = true;

	querySent = SendRemoteCommand(connection, command);
	if (querySent == 0)
	{
		ReportConnectionError(connection, ERROR);
	}

	result = GetRemoteCommandResult(connection, raiseInterrupts);
	if (!IsResponseOK(result))
	{
		ReportResultError(connection, result, ERROR);
	}

	PQclear(result);
	ForgetResults(connection);
}


/*
 * ExecuteOptionalRemoteCommand executes a remote command. If the command fails a WARNING
 * is emitted but execution continues.
 *
 * could return 0, QUERY_SEND_FAILED, or RESPONSE_NOT_OKAY
 * result is only set if there was no error
 */
int
ExecuteOptionalRemoteCommand(MultiConnection *connection, const char *command,
							 PGresult **result)
{
	int querySent = 0;
	PGresult *localResult = NULL;
	bool raiseInterrupts = true;

	querySent = SendRemoteCommand(connection, command);
	if (querySent == 0)
	{
		ReportConnectionError(connection, WARNING);
		return QUERY_SEND_FAILED;
	}

	localResult = GetRemoteCommandResult(connection, raiseInterrupts);
	if (!IsResponseOK(localResult))
	{
		ReportResultError(connection, localResult, WARNING);
		PQclear(localResult);
		ForgetResults(connection);
		return RESPONSE_NOT_OKAY;
	}

	*result = localResult;
	return 0;
}


/*
 * SendRemoteCommandParams is a PQsendQueryParams wrapper that logs remote commands,
 * and accepts a MultiConnection instead of a plain PGconn. It makes sure it can
 * send commands asynchronously without blocking (at the potential expense of
 * an additional memory allocation). The command string can only include a single
 * command since PQsendQueryParams() supports only that.
 */
int
SendRemoteCommandParams(MultiConnection *connection, const char *command,
						int parameterCount, const Oid *parameterTypes,
						const char *const *parameterValues)
{
	PGconn *pgConn = connection->pgConn;
	int rc = 0;

	LogRemoteCommand(connection, command);

	/*
	 * Don't try to send command if connection is entirely gone
	 * (PQisnonblocking() would crash).
	 */
	if (!pgConn)
	{
		return 0;
	}

	Assert(PQisnonblocking(pgConn));

	rc = PQsendQueryParams(pgConn, command, parameterCount, parameterTypes,
						   parameterValues, NULL, NULL, 0);

	return rc;
}


/*
 * SendRemoteCommand is a PQsendQuery wrapper that logs remote commands, and
 * accepts a MultiConnection instead of a plain PGconn. It makes sure it can
 * send commands asynchronously without blocking (at the potential expense of
 * an additional memory allocation). The command string can include multiple
 * commands since PQsendQuery() supports that.
 */
int
SendRemoteCommand(MultiConnection *connection, const char *command)
{
	PGconn *pgConn = connection->pgConn;
	int rc = 0;

	LogRemoteCommand(connection, command);

	/*
	 * Don't try to send command if connection is entirely gone
	 * (PQisnonblocking() would crash).
	 */
	if (!pgConn)
	{
		return 0;
	}

	Assert(PQisnonblocking(pgConn));

	rc = PQsendQuery(pgConn, command);

	return rc;
}


/*
 * ReadFirstColumnAsText reads the first column of result tuples from the given
 * PGresult struct and returns them in a StringInfo list.
 */
List *
ReadFirstColumnAsText(PGresult *queryResult)
{
	List *resultRowList = NIL;
	const int columnIndex = 0;
	int64 rowIndex = 0;
	int64 rowCount = 0;

	ExecStatusType status = PQresultStatus(queryResult);
	if (status == PGRES_TUPLES_OK)
	{
		rowCount = PQntuples(queryResult);
	}

	for (rowIndex = 0; rowIndex < rowCount; rowIndex++)
	{
		char *rowValue = PQgetvalue(queryResult, rowIndex, columnIndex);

		StringInfo rowValueString = makeStringInfo();
		appendStringInfoString(rowValueString, rowValue);

		resultRowList = lappend(resultRowList, rowValueString);
	}

	return resultRowList;
}


/*
 * GetRemoteCommandResult is a wrapper around PQgetResult() that handles interrupts.
 *
 * If raiseInterrupts is true and an interrupt arrives, e.g. the query is
 * being cancelled, CHECK_FOR_INTERRUPTS() will be called, which then throws
 * an error.
 *
 * If raiseInterrupts is false and an interrupt arrives that'd otherwise raise
 * an error, GetRemoteCommandResult returns NULL, and the transaction is
 * marked as having failed. While that's not a perfect way to signal failure,
 * callers will usually treat that as an error, and it's easy to use.
 *
 * Handling of interrupts is important to allow queries being cancelled while
 * waiting on remote nodes. In a distributed deadlock scenario cancelling
 * might be the only way to resolve the deadlock.
 */
PGresult *
GetRemoteCommandResult(MultiConnection *connection, bool raiseInterrupts)
{
	PGconn *pgConn = connection->pgConn;
	PGresult *result = NULL;

	/*
	 * Short circuit tests around the more expensive parts of this
	 * routine. This'd also trigger a return in the, unlikely, case of a
	 * failed/nonexistant connection.
	 */
	if (!PQisBusy(pgConn))
	{
		return PQgetResult(connection->pgConn);
	}

	if (!FinishConnectionIO(connection, raiseInterrupts))
	{
		return NULL;
	}

	/* no IO should be necessary to get result */
	Assert(!PQisBusy(pgConn));

	result = PQgetResult(connection->pgConn);

	return result;
}


/*
 * PutRemoteCopyData is a wrapper around PQputCopyData() that handles
 * interrupts.
 *
 * Returns false if PQputCopyData() failed, true otherwise.
 */
bool
PutRemoteCopyData(MultiConnection *connection, const char *buffer, int nbytes)
{
	PGconn *pgConn = connection->pgConn;
	int copyState = 0;

	if (PQstatus(pgConn) != CONNECTION_OK)
	{
		return false;
	}

	Assert(PQisnonblocking(pgConn));

	copyState = PQputCopyData(pgConn, buffer, nbytes);

	if (copyState == 1)
	{
		/* successful */
		return true;
	}
	else if (copyState == -1)
	{
		return false;
	}
	else
	{
		bool allowInterrupts = true;
		return FinishConnectionIO(connection, allowInterrupts);
	}
}


/*
 * PutRemoteCopyEnd is a wrapper around PQputCopyEnd() that handles
 * interrupts.
 *
 * Returns false if PQputCopyEnd() failed, true otherwise.
 */
bool
PutRemoteCopyEnd(MultiConnection *connection, const char *errormsg)
{
	PGconn *pgConn = connection->pgConn;
	int copyState = 0;

	if (PQstatus(pgConn) != CONNECTION_OK)
	{
		return false;
	}

	Assert(PQisnonblocking(pgConn));

	copyState = PQputCopyEnd(pgConn, errormsg);

	if (copyState == 1)
	{
		/* successful */
		return true;
	}
	else if (copyState == -1)
	{
		return false;
	}
	else
	{
		bool allowInterrupts = true;
		return FinishConnectionIO(connection, allowInterrupts);
	}
}


/*
 * FinishConnectionIO performs pending IO for the connection, while accepting
 * interrupts.
 *
 * See GetRemoteCommandResult() for documentation of interrupt handling
 * behaviour.
 *
 * Returns true if IO was successfully completed, false otherwise.
 */
static bool
FinishConnectionIO(MultiConnection *connection, bool raiseInterrupts)
{
	PGconn *pgConn = connection->pgConn;
	int socket = PQsocket(pgConn);

	Assert(pgConn);
	Assert(PQisnonblocking(pgConn));

	if (raiseInterrupts)
	{
		CHECK_FOR_INTERRUPTS();
	}

	/* perform the necessary IO */
	while (true)
	{
		int sendStatus = 0;
		int rc = 0;
		int waitFlags = WL_POSTMASTER_DEATH | WL_LATCH_SET;

		/* try to send all pending data */
		sendStatus = PQflush(pgConn);

		/* if sending failed, there's nothing more we can do */
		if (sendStatus == -1)
		{
			return false;
		}
		else if (sendStatus == 1)
		{
			waitFlags |= WL_SOCKET_WRITEABLE;
		}

		/* if reading fails, there's not much we can do */
		if (PQconsumeInput(pgConn) == 0)
		{
			return false;
		}
		if (PQisBusy(pgConn))
		{
			waitFlags |= WL_SOCKET_READABLE;
		}

		if ((waitFlags & (WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE)) == 0)
		{
			/* no IO necessary anymore, we're done */
			return true;
		}

#if (PG_VERSION_NUM >= 100000)
		rc = WaitLatchOrSocket(MyLatch, waitFlags, socket, 0, PG_WAIT_EXTENSION);
#else
		rc = WaitLatchOrSocket(MyLatch, waitFlags, socket, 0);
#endif

		if (rc & WL_POSTMASTER_DEATH)
		{
			ereport(ERROR, (errmsg("postmaster was shut down, exiting")));
		}

		if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);

			/* if allowed raise errors */
			if (raiseInterrupts)
			{
				CHECK_FOR_INTERRUPTS();
			}

			/*
			 * If raising errors allowed, or called within in a section with
			 * interrupts held, return instead, and mark the transaction as
			 * failed.
			 */
			if (InterruptHoldoffCount > 0 && (QueryCancelPending || ProcDiePending))
			{
				connection->remoteTransaction.transactionFailed = true;
				break;
			}
		}
	}

	return false;
}


/*
 * WaitForAllConnections blocks until all connections in the list are no
 * longer busy, meaning the pending command has either finished or failed.
 */
void
WaitForAllConnections(List *connectionList, bool raiseInterrupts)
{
	MultiConnectionWaiter *waiter = CreateMultiConnectionWaiter(connectionList);

	while (AwaitReadyConnections(waiter, raiseInterrupts) != NIL)
	{
		/* call AwaitReadyConnections until it processed all connections */
	}

	FreeMultiConnectionWaiter(waiter);
}


/*
 * CreateMultiConnectionWaiter creates a MultiConnectionWaiter for the given list
 * of connections. Connections should not be in a closed state.
 */
static MultiConnectionWaiter *
CreateMultiConnectionWaiter(List *connectionList)
{
	MultiConnectionWaiter *waiter = NULL;
	ListCell *connectionCell = NULL;
	int totalConnectionCount = list_length(connectionList);

	waiter = (MultiConnectionWaiter *) palloc0(sizeof(MultiConnectionWaiter));
	waiter->connectionCount = 0;
	waiter->currentOffset = 0;

	waiter->connections =
		(MultiConnection **) palloc(totalConnectionCount * sizeof(MultiConnection *));

	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);

		waiter->connections[waiter->connectionCount] = connection;
		waiter->connectionCount++;
	}

	waiter->events = (WaitEvent *) palloc0(totalConnectionCount * sizeof(WaitEvent));

	return waiter;
}


/*
 * AwaitReadyConnections waits for I/O events and returns a list of connections
 * that finished or failed. Each connection is returned at most once across multiple
 * calls.
 *
 * When all connections are ready and were returned as part of a list,
 * AwaytReadyConnections returns an empty list.
 *
 * To keep track of which connections were already returned, pending connections
 * are moved to the end of the connections array, starting at currentOffset.
 */
static List *
AwaitReadyConnections(MultiConnectionWaiter *waiter, bool raiseInterrupts)
{
	MultiConnection **allConnections = waiter->connections;
	bool connectionReady[waiter->connectionCount];
	bool foundReadyConnections = false;
	List *readyConnectionList = NIL;
	WaitEventSet *waitEventSet = NULL;
	int connectionCount = waiter->connectionCount;
	int pendingConnectionsStartIndex = waiter->currentOffset;
	int pendingConnectionCount = connectionCount - pendingConnectionsStartIndex;
	int connectionIndex = 0;

	if (waiter->currentOffset == connectionCount)
	{
		/* already processed all connections */
		return NIL;
	}

	/*
	 * Ideally, we'd store the WaitEventSet in MultiConnectionWaiter and reuse it across
	 * different calls to AwaitReadyConnections. However, we currently don't have a
	 * way to stop listening for a connection in a WaitEventSet once it's ready, since
	 * ModifyWaitEvent has an assert preventing the caller from clearing the event mask
	 * event masks. This should be revisited in future postgres versions.
	 */
	waitEventSet = CreateWaitEventSet(CurrentMemoryContext, pendingConnectionCount + 2);

	for (connectionIndex = pendingConnectionsStartIndex;
		 connectionIndex < connectionCount; connectionIndex++)
	{
		MultiConnection *connection = allConnections[connectionIndex];
		int socket = PQsocket(connection->pgConn);
		int eventMask = WL_SOCKET_READABLE;
		bool connectionFailed = false;

		if (PQstatus(connection->pgConn) == CONNECTION_BAD)
		{
			connectionFailed = true;
		}
		else
		{
			int sendStatus = PQflush(connection->pgConn);
			if (sendStatus == -1)
			{
				connectionFailed = true;
			}
			else if (sendStatus == 1)
			{
				/* more data to flush */
				eventMask |= WL_SOCKET_WRITEABLE;
			}
		}

		if (!connectionFailed && PQisBusy(connection->pgConn))
		{
			connectionReady[connectionIndex] = false;
			AddWaitEventToSet(waitEventSet, eventMask, socket, NULL, (void *) connection);
		}
		else
		{
			connectionReady[connectionIndex] = true;
			foundReadyConnections = true;
		}
	}

	/* if we already detected some failures we skip waiting */
	if (!foundReadyConnections)
	{
		AddWaitEventToSet(waitEventSet, WL_POSTMASTER_DEATH, PGINVALID_SOCKET, NULL,
						  NULL);
		AddWaitEventToSet(waitEventSet, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);

		PG_TRY();
		{
			WaitForReadyConnections(waiter, waitEventSet, connectionReady,
									raiseInterrupts);
		}
		PG_CATCH();
		{
			/* make sure the epoll file descriptor is closed */
			FreeWaitEventSet(waitEventSet);
			PG_RE_THROW();
		}
		PG_END_TRY();
	}

	FreeWaitEventSet(waitEventSet);

	/* move connections that are not yet ready to the end of the array */
	for (connectionIndex = waiter->currentOffset; connectionIndex < connectionCount;
		 connectionIndex++)
	{
		if (connectionReady[connectionIndex])
		{
			MultiConnection *connection = allConnections[connectionIndex];

			/* connection is ready, add to ready list */
			readyConnectionList = lappend(readyConnectionList, connection);

			/* reuse the slot; move a non-ready connection from the front */
			allConnections[connectionIndex] = allConnections[waiter->currentOffset];

			waiter->currentOffset++;
		}
	}

	return readyConnectionList;
}


/*
 * WaitForReadyConnections waits for connections using the given WaitEventSet.
 */
static void
WaitForReadyConnections(MultiConnectionWaiter *waiter, WaitEventSet *waitEventSet,
						bool *connectionReady, bool raiseInterrupts)
{
	int connectionCount = waiter->connectionCount;
	int pendingConnectionsStartIndex = waiter->currentOffset;
	int pendingConnectionCount = connectionCount - pendingConnectionsStartIndex;
	bool foundReadyConnections = false;
	bool cancelled = false;
	int connectionIndex = 0;

	while (!foundReadyConnections)
	{
		int eventIndex = 0;
		int eventCount = 0;
		long timeout = -1;

		/* wait for I/O events */
#if (PG_VERSION_NUM >= 100000)
		eventCount = WaitEventSetWait(waitEventSet, timeout, waiter->events,
									  pendingConnectionCount, WAIT_EVENT_CLIENT_READ);
#else
		eventCount = WaitEventSetWait(waitEventSet, timeout, waiter->events,
									  pendingConnectionCount);
#endif

		for (; eventIndex < eventCount; eventIndex++)
		{
			WaitEvent *event = &waiter->events[eventIndex];
			MultiConnection *connection = NULL;
			bool connectionIsReady = false;

			if (event->events & WL_POSTMASTER_DEATH)
			{
				ereport(ERROR, (errmsg("postmaster was shut down, exiting")));
			}

			if (event->events & WL_LATCH_SET)
			{
				ResetLatch(MyLatch);

				if (raiseInterrupts)
				{
					CHECK_FOR_INTERRUPTS();
				}

				if (InterruptHoldoffCount > 0 && (QueryCancelPending || ProcDiePending))
				{
					cancelled = true;
					break;
				}

				continue;
			}

			connection = (MultiConnection *) event->user_data;
			connectionIndex = event->pos + pendingConnectionsStartIndex;

			if (event->events & WL_SOCKET_READABLE)
			{
				int receiveStatus = PQconsumeInput(connection->pgConn);
				if (receiveStatus == 0)
				{
					/* receive failed, done with this connection */
					connectionIsReady = true;
				}
				else if (!PQisBusy(connection->pgConn))
				{
					/* result was received */
					connectionIsReady = true;
				}
			}
			if (event->events & WL_SOCKET_WRITEABLE)
			{
				int sendStatus = PQflush(connection->pgConn);
				if (sendStatus == -1)
				{
					/* send failed, done with this connection */
					connectionIsReady = true;
				}
				else if (sendStatus == 0)
				{
					/* done writing, only wait for read events */
					ModifyWaitEvent(waitEventSet, connectionIndex, WL_SOCKET_READABLE,
									NULL);
				}
			}

			if (connectionIsReady)
			{
				/* reuse this slot for a different connection */
				connectionReady[connectionIndex] = true;
				foundReadyConnections = true;
			}
		}
	}

	/* return all remaining connections if we need to cancel */
	if (cancelled)
	{
		for (connectionIndex = waiter->currentOffset;
			 connectionIndex < connectionCount; connectionIndex++)
		{
			connectionReady[connectionIndex] = true;
		}
	}
}


/*
 * FreeMultiConnectionWaiter frees a previously allocated MultiConnectionWaiter.
 */
static void
FreeMultiConnectionWaiter(MultiConnectionWaiter *waiter)
{
	Assert(waiter->connections != NULL);
	Assert(waiter->events != NULL);

	pfree(waiter->connections);
	pfree(waiter->events);
	pfree(waiter);
}
