// Copyright (c) 2017 Alexey Tourbin
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#ifndef _GNU_SOURCE
#define _GNU_SOURCE // fputs_unlocked
#endif

#include <stdbool.h>
#include <stdio.h>
#include <assert.h>
#include <pthread.h>
#include <rpm/rpmlib.h>

// A thread-safe strerror(3) replacement.
static const char *xstrerror(int errnum)
{
    // Some of the great minds say that sys_errlist is deprecated.
    // Well, at least it's thread-safe, and it does not deadlock.
    if (errnum > 0 && errnum < sys_nerr)
	return sys_errlist[errnum];
    return "Unknown error";
}

// A job queue entry which needs to be processed: the header blob has to be
// loaded, queried, and the formatted output string put back to str, with
// the stage updated accordingly.  The strings then will be picked up and
// printed in the original order.
struct qent {
    union { void *blob; char *str; };
    enum { STAGE_BLOB, STAGE_COOKING, STAGE_STR } stage;
};

// The number of entries in the job queue.
// Should be just high enough to provide parallelism.
#define NQ 8

// The job queue.
struct {
    pthread_mutex_t mutex;
    pthread_cond_t can_produce;
    pthread_cond_t can_consume;
    int nq;
    struct qent q[NQ];
} Q = {
    PTHREAD_MUTEX_INITIALIZER,
    PTHREAD_COND_INITIALIZER,
    PTHREAD_COND_INITIALIZER,
};

#define PROG "pkglist-query"

#define die(fmt, args...) \
do { \
    fprintf(stderr, PROG ": " fmt "\n", ##args); \
    exit(2); \
} while (0)

// After formatting is done, put the string back and flush the queue,
// picking up earlier strings and printing them in the original order.
void putBack(void *blob, Header h, char *str)
{
    int i;
    // Flush the queue.
    for (i = 0; i < Q.nq; i++) {
	struct qent *qe = &Q.q[i];
	if (qe->stage == STAGE_STR) {
	    if (fputs_unlocked(qe->str, stdout) == EOF)
		die("%s: %m", "fputs");
	    free(qe->str);
	}
	else if (qe->stage == STAGE_COOKING && qe->blob == blob) {
	    if (fputs_unlocked(str, stdout) == EOF)
		die("%s: %m", "fputs");
	    free(str);
	    headerFree(h);
	    // The blob has been freed on behalf of headerFree.
	    blob = NULL;
	}
	else
	    break;
    }
    // Advance the queue.
    Q.nq -= i;
    memmove(Q.q, Q.q + i, Q.nq * sizeof(struct qent));
    // Was it put back?
    if (blob == NULL)
	return;
    // Still need to put back.
    for (i = 1; i < Q.nq; i++) {
	struct qent *qe = &Q.q[i];
	if (qe->stage == STAGE_COOKING && qe->blob == blob) {
	    qe->str = str;
	    qe->stage = STAGE_STR;
	    headerFree(h);
	    blob = NULL;
	    break;
	}
    }
    // Must have put back by now.
    assert(blob == NULL);
}

// This routine is executed by the helper thread.
void *worker(void *fmt)
{
    void *blob = NULL;
    Header h = NULL;
    char *str = NULL;
    while (1) {
	// Lock the mutex.
	int err = pthread_mutex_lock(&Q.mutex);
	if (err) die("%s: %s", "pthread_mutex_lock", xstrerror(err));
	// See if they're possible waiting to produce.
	bool waiting = Q.nq == NQ;
	// Put back the job from the previous iteration.
	if (blob) {
	    putBack(blob, h, str);
	    blob = NULL, h = NULL, str = NULL;
	}
	// If they're possibly waiting to produce, let them know.
	if (waiting && Q.nq < NQ) {
	    err = pthread_cond_signal(&Q.can_produce);
	    if (err) die("%s: %s", "pthread_cond_signal", xstrerror(err));
	}
	// Try to fetch a blob from the queue.
	while (1) {
	    for (int i = 0; i < Q.nq; i++) {
		struct qent *qe = &Q.q[i];
		if (qe->stage == STAGE_BLOB) {
		    // NULL blob works as a sentinel.
		    if (qe->blob == NULL)
			goto loopbreak;
		    qe->stage = STAGE_COOKING;
		    blob = qe->blob;
		    break;
		}
	    }
	    if (blob)
		break;
	    // Wait until something is queued.
	    err = pthread_cond_wait(&Q.can_consume, &Q.mutex);
	    if (err) die("%s: %s", "pthread_cond_wait", xstrerror(err));
	}
    loopbreak:
	// Unlock the mutex.
	err = pthread_mutex_unlock(&Q.mutex);
	if (err) die("%s: %s", "pthread_mutex_unlock", xstrerror(err));
	// Handle the end of the queue.
	if (blob == NULL)
	    return NULL;
	// Do the job.
	h = headerImport(blob, 0, HEADERIMPORT_FAST);
	if (!h) die("headerImport: import failed");
	const char *fmterr = "format failed";
	str = headerFormat(h, fmt, &fmterr);
	if (!str) die("headerFormat: %s", fmterr);
    }
}

// ex:set ts=8 sts=4 sw=4 noet:
