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

// Check if the worker needs aid from the main thread.
void *needAid1(void)
{
    for (int i = 0; i < Q.nq; i++) {
	struct qent *qe = &Q.q[i];
	if (qe->stage == STAGE_BLOB) {
	    qe->stage = STAGE_COOKING;
	    return qe->blob;
	}
    }
    return NULL;
}

// If there are at least two blobs, returns the first one.
void *needAid2(void)
{
    struct qent *qe0 = NULL;
    for (int i = 0; i < Q.nq; i++) {
	struct qent *qe = &Q.q[i];
	if (qe->stage == STAGE_BLOB) {
	    if (qe0) {
		qe0->stage = STAGE_COOKING;
		return qe0->blob;
	    }
	    qe0 = qe;
	}
    }
    return NULL;
}

// The main thread then can help the worker.
void aid(void *blob, const char *fmt)
{
    // Unlock the mutex.
    int err = pthread_mutex_unlock(&Q.mutex);
    if (err) die("%s: %s", "pthread_mutex_unlock", xstrerror(err));
    // Do the job.
    Header h = headerImport(blob, 0, HEADERIMPORT_FAST);
    if (!h) die("headerImport: import failed");
    const char *fmterr = "format failed";
    char *str = headerFormat(h, fmt, &fmterr);
    if (!str) die("headerFormat: %s", fmterr);
    // Lock the mutex.
    err = pthread_mutex_lock(&Q.mutex);
    if (err) die("%s: %s", "pthread_mutex_lock", xstrerror(err));
    // Put back.
    putBack(blob, h, str);
}

// Dispatch the blob, called from the main thread.
void processBlob(void *blob, const char *fmt)
{
    // Lock the mutex.
    int err = pthread_mutex_lock(&Q.mutex);
    if (err) die("%s: %s", "pthread_mutex_lock", xstrerror(err));
    // Try to help while the queue is full.  But we also need
    // to keep the worker busy while decoding the next blob,
    // so don't grab the very last one.
    while (Q.nq == NQ) {
	void *blob = needAid2();
	if (blob) {
	    aid(blob, fmt);
	    continue;
	}
	// Wait until the queue is flushed.
	err = pthread_cond_wait(&Q.can_produce, &Q.mutex);
	if (err) die("%s: %s", "pthread_cond_wait", xstrerror(err));
    }
    // Put the blob to the queue.
    Q.q[Q.nq++] = (struct qent) { { blob }, STAGE_BLOB };
    // If they're waiting to consume, let them know.
    err = pthread_cond_signal(&Q.can_consume);
    if (err) die("%s: %s", "pthread_cond_signal", xstrerror(err));
    // Unlock the mutex.
    err = pthread_mutex_unlock(&Q.mutex);
    if (err) die("%s: %s", "pthread_mutex_unlock", xstrerror(err));
}

// Drain the queue and join the worker thread.
void finish(pthread_t thread, const char *fmt)
{
    // Lock the mutex.
    int err = pthread_mutex_lock(&Q.mutex);
    if (err) die("%s: %s", "pthread_mutex_lock", xstrerror(err));
    // Help as much as possible.
    while (1) {
	void *blob = needAid1();
	if (blob)
	    aid(blob, fmt);
	else
	    break;
    }
    // Still need to wait if the queue is full.
    while (Q.nq == NQ) {
	err = pthread_cond_wait(&Q.can_produce, &Q.mutex);
	if (err) die("%s: %s", "pthread_cond_wait", xstrerror(err));
    }
    // Put the sentinel.
    Q.q[Q.nq++] = (struct qent) { { NULL }, STAGE_BLOB };
    // Let them know.
    err = pthread_cond_signal(&Q.can_consume);
    if (err) die("%s: %s", "pthread_cond_signal", xstrerror(err));
    // Unlock the mutex.
    err = pthread_mutex_unlock(&Q.mutex);
    if (err) die("%s: %s", "pthread_mutex_unlock", xstrerror(err));
    // Join the worker thread.
    err = pthread_join(thread, NULL);
    if (err) die("%s: %s", "pthread_join", xstrerror(err));
}

#include <unistd.h>
#include <zpkglist.h>

void processFd(int fd, const char *fname, const char *fmt)
{
    const char *err[2];
    struct zpkglistReader *z;
    const char *func = "zpkglistFdopen";
    ssize_t ret = zpkglistFdopen(&z, fd, err);
    if (ret > 0) {
	void *blob;
	func = "zpkglistNextMalloc";
	while ((ret = zpkglistNextMalloc(z, &blob, NULL, false, err)) > 0)
	    processBlob(blob, fmt);
	zpkglistFree(z);
    }
    close(fd);
    if (ret < 0) {
	if (strcmp(func, err[0]) == 0 || strncmp(err[0], "zpkglist", 8) == 0)
	    die("%s: %s: %s", fname, err[0], err[1]);
	else
	    die("%s: %s: %s: %s", fname, func, err[0], err[1]);
    }
}

#include <getopt.h>
#include <fcntl.h> // O_RDONLY

const struct option longopts[] = {
    { "help", no_argument, NULL, 'h' },
    { NULL },
};

int main(int argc, char **argv)
{
    bool usage = false;
    int c;
    while ((c = getopt_long(argc, argv, "h", longopts, NULL)) != -1)
	usage = true;
    if (usage) {
usage:	fprintf(stderr, "Usage: " PROG " FMT [PKGLIST...]\n");
	return 1;
    }
    argc -= optind, argv += optind;
    if (argc < 1) {
	fprintf(stderr, PROG ": not enought arguments\n");
	goto usage;
    }
    const char *fmt = argv[0];
    argc--, argv++;
    if (argc < 1 && isatty(0)) {
	fprintf(stderr, PROG ": refusing to read binary data from a terminal\n");
	goto usage;
    }
    char *assume_argv[] = { "-", NULL };
    if (argc < 1)
	argc = 1, argv = assume_argv;
    pthread_t thread;
    int err = pthread_create(&thread, NULL, worker, (void *) fmt);
    if (err) die("%s: %s", "pthread_create", xstrerror(err));
    for (int i = 0; i < argc; i++) {
	int fd = 0;
	const char *fname = argv[i];
	if (strcmp(argv[i], "-") == 0)
	    fname = "<stdin>";
	else {
	    fd = open(fname, O_RDONLY);
	    if (fd < 0)
		die("%s: open: %m", fname);
	}
	processFd(fd, fname, fmt);
    }
    finish(thread, fmt);
    if (fflush_unlocked(stdout) == EOF)
	die("%s: %m", "fflush");
    return 0;
}

// ex:set ts=8 sts=4 sw=4 noet:
