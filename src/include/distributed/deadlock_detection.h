/*-------------------------------------------------------------------------
 *
 * deadlock_detection.h
 *
 *   Functions for detecting and resolving distributed deadlocks.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef DEADLOCK_DETECTION_H
#define DEADLOCK_DETECTION_H


#include "fmgr.h"
#include "utils/elog.h"


extern bool DetectDistributedDeadlocks(void);
extern void DeadlockLogHook(ErrorData *edata);


#endif /* DEADLOCK_DETECTION_H */
