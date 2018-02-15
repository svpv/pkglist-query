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

#include <stdbool.h>
#include <stdint.h>
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
    union { void *blob; uintptr_t cookie; char *str; };
    union { unsigned blobSize; unsigned len; };
    enum { STAGE_BLOB, STAGE_COOKING, STAGE_STR } stage;
};

// The maximum number of entries in the job queue.
// Good parallelism can be achieved only with a somewhat big queue:
// - src.rpm headers can be as small as 1K, while zstd decompression
//   operates in 128K chunks; see also MINBYTES below;
// - on the other hand, a big header (e.g. with many %{Filenames}) can
//   take a lot of time to headerFormat, and if the second thread fills
//   the remaining slots quickly, the only alternative for it is to stall.
#define NQ 128

// The job queue.
struct {
    pthread_mutex_t mutex;
    pthread_cond_t can_produce;
    pthread_cond_t can_consume;
    // An ever increasing sequence number used as a cookie.
    uintptr_t seq;
    // The sum of the blob sizes of STAGE_BLOB entries.
    size_t blobBytes;
    // The total number of STAGE_BLOB entries in the queue.
    int nblob;
    int nq;
    struct qent q[NQ];
} Q = {
    PTHREAD_MUTEX_INITIALIZER,
    PTHREAD_COND_INITIALIZER,
    PTHREAD_COND_INITIALIZER,
};

#define PROG "pkglist-query"
#define warn(fmt, args...) fprintf(stderr, "%s: " fmt "\n", PROG, ##args)
#define die(fmt, args...) warn(fmt, ##args), exit(128) // like git

// Search a blob in Q.q starting with qe.
static inline struct qent *findBlob(struct qent *qe)
{
    // Cf. Quicker sequential search in [Knuth, Vol.3, p.398].
    while (1) {
	if (qe[0].stage == STAGE_BLOB) break;
	if (qe[1].stage == STAGE_BLOB) { qe += 1; break; }
	if (qe[2].stage == STAGE_BLOB) { qe += 2; break; }
	if (qe[3].stage == STAGE_BLOB) { qe += 3; break; }
	qe += 4;
    }
    return qe;
}

// After formatting is done, put the string back and flush the queue,
// picking up earlier strings and printing them in the original order.
void putBack(uintptr_t cookie, char *str, size_t len)
{
    int i;
    // Flush the queue.
    for (i = 0; i < Q.nq; i++) {
	struct qent *qe = &Q.q[i];
	if (qe->stage == STAGE_STR) {
	    if (fwrite_unlocked(qe->str, 1, qe->len, stdout) != qe->len)
		die("%s: %m", "fwrite");
	    free(qe->str);
	}
	else if (qe->cookie == cookie) {
	    if (fwrite_unlocked(str, 1, len, stdout) != len)
		die("%s: %m", "fwrite");
	    free(str);
	    str = NULL;
	}
	else
	    break;
    }
    // Advance the queue.
    Q.nq -= i;
    memmove(Q.q, Q.q + i, Q.nq * sizeof(struct qent));
    // Was it put back?
    if (str == NULL)
	return;
    // Still need to put back.
    struct qent *qe = Q.q + 1;
    while (1) {
	if (qe[0].cookie == cookie) break;
	if (qe[1].cookie == cookie) { qe += 1; break; }
	if (qe[2].cookie == cookie) { qe += 2; break; }
	if (qe[3].cookie == cookie) { qe += 3; break; }
	qe += 4;
    }
    qe->str = str;
    assert(len < ~0U);
    qe->len = len;
    qe->stage = STAGE_STR;
}

// This routine is executed by the helper thread.
void *worker(void *fmt)
{
    uintptr_t cookie = 0;
    char *str = NULL;
    size_t len = 0;
    while (1) {
	// Lock the mutex.
	int err = pthread_mutex_lock(&Q.mutex);
	if (err) die("%s: %s", "pthread_mutex_lock", xstrerror(err));
	// See if they're possibly waiting to produce.
	bool waiting = Q.nq == NQ;
	// Put back the job from the previous iteration.
	if (str) {
	    putBack(cookie, str, len);
	    cookie = 0, str = NULL, len = 0;
	}
	// If they're possibly waiting to produce, let them know.
	if (waiting && Q.nq < NQ) {
	    err = pthread_cond_signal(&Q.can_produce);
	    if (err) die("%s: %s", "pthread_cond_signal", xstrerror(err));
	}
	// Try to fetch a blob from the queue.
	void *blob;
	unsigned blobSize;
	while (1) {
	    if (Q.nblob) {
		struct qent *qe = findBlob(Q.q);
		blob = qe->blob;
		blobSize = qe->blobSize;
		// Make an odd cookie so that it never equals
		// an aligned pointer such as malloc'd blob or str.
		cookie = qe->cookie = (Q.seq++, Q.seq++);
		qe->stage = STAGE_COOKING;
		Q.nblob--, Q.blobBytes -= blobSize;
		break;
	    }
	    // Wait until something is queued.
	    err = pthread_cond_wait(&Q.can_consume, &Q.mutex);
	    if (err) die("%s: %s", "pthread_cond_wait", xstrerror(err));
	}
	// Got a blob, unlock the mutex.
	err = pthread_mutex_unlock(&Q.mutex);
	if (err) die("%s: %s", "pthread_mutex_unlock", xstrerror(err));
	// Handle the end of the queue.
	if (blob == NULL)
	    return NULL;
	// Do the job.
	Header h = headerImport(blob, blobSize, HEADERIMPORT_FAST);
	if (!h) die("headerImport: import failed");
	const char *fmterr = "format failed";
	str = headerFormat(h, fmt, &fmterr);
	if (!str) die("headerFormat: %s", fmterr);
	// The blob is freed on behalf of headerFree.
	headerFree(h);
	len = strlen(str);
    }
}

// Check if the worker needs aid from the main thread.
struct qent *needAid1(void)
{
    if (Q.nblob < 1)
	return NULL;
    return findBlob(Q.q);
}

// If there are at least two blobs, returns the first one.
struct qent *needAid2(void)
{
    if (Q.nblob < 2)
	return NULL;
    return findBlob(Q.q);
}

// If the number of blobs in the queue is roughly below this size,
// the main thread will keep pumping up the queue with new blobs.
// Otherwise, the main thread will also consider the possibility of
// helping the worker thread to cope with the already loaded blobs.
#define MINBLOB 16
// Also, because decompressors operate in chunks, the total byte count
// shouldn't drop much below the chunk size.
#define MINBYTES (128<<10)

static_assert(MINBLOB >= 4, "MINBLOB is not too small");
static_assert(NQ >= 2 * MINBLOB, "NQ is not too small");

// An advanced strategy for the main thread.
struct qent *needMoreAid(void)
{
    // Too few bytes left?  Time to recharge the decompressor.
    if (Q.blobBytes < MINBYTES)
	return NULL;
    // Too few blobs?
    if (Q.nblob < MINBLOB * 3 / 4)
	return NULL;
    struct qent *qe = findBlob(Q.q);
    // The blob shouldn't be too big, as compared to other blobs.
    // But note that we're averaging over MINBLOB, not Q.nblob.
    // This means that, if the blob seems too big for now, it might
    // be taken next time, after the main thread pushes another blob.
    if (qe->blobSize > Q.blobBytes / MINBLOB)
	return NULL;
    return qe;
}

// The main thread then can help the worker.
void aid(struct qent *qe, const char *fmt)
{
    void *blob = qe->blob;
    unsigned blobSize = qe->blobSize;
    // We're under the same lock as needAid(),
    // complete the transition to the cooking stage.
    uintptr_t cookie = qe->cookie = (Q.seq++, Q.seq++);
    qe->stage = STAGE_COOKING;
    Q.nblob--, Q.blobBytes -= blobSize;
    // Unlock the mutex.
    int err = pthread_mutex_unlock(&Q.mutex);
    if (err) die("%s: %s", "pthread_mutex_unlock", xstrerror(err));
    // Do the job.
    Header h = headerImport(blob, blobSize, HEADERIMPORT_FAST);
    if (!h) die("headerImport: import failed");
    const char *fmterr = "format failed";
    char *str = headerFormat(h, fmt, &fmterr);
    if (!str) die("headerFormat: %s", fmterr);
    headerFree(h);
    size_t len = strlen(str);
    // Lock the mutex.
    err = pthread_mutex_lock(&Q.mutex);
    if (err) die("%s: %s", "pthread_mutex_lock", xstrerror(err));
    // Put back.
    putBack(cookie, str, len);
}

// Dispatch the blob, called from the main thread.
void processBlob(void *blob, unsigned blobSize, const char *fmt)
{
    // Lock the mutex.
    int err = pthread_mutex_lock(&Q.mutex);
    if (err) die("%s: %s", "pthread_mutex_lock", xstrerror(err));
    // Try to help while the queue is full.  But we also need
    // to keep the worker busy while decoding the next blob,
    // so don't grab the very last one.
    while (Q.nq == NQ) {
	struct qent *qe = needAid2();
	if (qe) {
	    aid(qe, fmt);
	    continue;
	}
	// Wait until the queue is flushed.
	err = pthread_cond_wait(&Q.can_produce, &Q.mutex);
	if (err) die("%s: %s", "pthread_cond_wait", xstrerror(err));
    }
    // Put the blob to the queue.
    Q.q[Q.nq++] = (struct qent) { { blob }, { blobSize }, STAGE_BLOB };
    Q.nblob++, Q.blobBytes += blobSize;
    // If they're possibly waiting to consume, let them know.
    if (Q.nblob == 1) {
	err = pthread_cond_signal(&Q.can_consume);
	if (err) die("%s: %s", "pthread_cond_signal", xstrerror(err));
    }
    // See if more help is desirable.
    while (1) {
	struct qent *qe = needMoreAid();
	if (!qe)
	    break;
	aid(qe, fmt);
    }
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
	struct qent *qe = needAid1();
	if (qe)
	    aid(qe, fmt);
	else
	    break;
    }
    // Still need to wait if the queue is full.
    while (Q.nq == NQ) {
	err = pthread_cond_wait(&Q.can_produce, &Q.mutex);
	if (err) die("%s: %s", "pthread_cond_wait", xstrerror(err));
    }
    // Put the sentinel.
    Q.q[Q.nq++] = (struct qent) { { NULL }, { 0 }, STAGE_BLOB };
    Q.nblob++;
    // Let them know.
    err = pthread_cond_signal(&Q.can_consume);
    if (err) die("%s: %s", "pthread_cond_signal", xstrerror(err));
    // Unlock the mutex.
    err = pthread_mutex_unlock(&Q.mutex);
    if (err) die("%s: %s", "pthread_mutex_unlock", xstrerror(err));
    // Join the worker thread.
    err = pthread_join(thread, NULL);
    if (err) die("%s: %s", "pthread_join", xstrerror(err));
    // Verify the bookkeeping.
    assert(Q.nblob == 0), assert(Q.blobBytes == 0);
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
	    processBlob(blob, ret, fmt);
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
	warn("not enough arguments");
	goto usage;
    }
    const char *fmt = argv[0];
    argc--, argv++;
    if (argc < 1 && isatty(0)) {
	warn("refusing to read binary data from a terminal");
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
