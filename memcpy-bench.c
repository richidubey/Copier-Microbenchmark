#include <errno.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#include "copier.h"

#define GRANULARITY 1024
#define DEFAULT_WARMUP 3
#define DEFAULT_ITERATIONS 20
#define DEFAULT_EVICT_MB 128

enum backend_mode {
	BACKEND_LIBC = 0,
	BACKEND_COPIER = 1,
	BACKEND_BOTH = 2,
};

enum condition_mode {
	COND_COLD = 0,
	COND_PINNED = 1,
	COND_SAME = 2,
	COND_ALL = 3,
};

static void usage(const char *prog)
{
	fprintf(stderr,
		"Usage: %s <backend> <condition> <size_mb> [iterations] [warmup]\n"
		"  backend   : libc | copier | both\n"
		"  condition : cold | pinned | same | all\n"
		"  size_mb   : payload size in MiB\n"
		"  iterations: measured iterations (default %d)\n"
		"  warmup    : warmup iterations (default %d)\n",
		prog, DEFAULT_ITERATIONS, DEFAULT_WARMUP);
}

static enum backend_mode parse_backend(const char *value)
{
	if (strcmp(value, "libc") == 0)
		return BACKEND_LIBC;
	if (strcmp(value, "copier") == 0)
		return BACKEND_COPIER;
	if (strcmp(value, "both") == 0)
		return BACKEND_BOTH;
	return -1;
}

static enum condition_mode parse_condition(const char *value)
{
	if (strcmp(value, "cold") == 0)
		return COND_COLD;
	if (strcmp(value, "pinned") == 0)
		return COND_PINNED;
	if (strcmp(value, "same") == 0)
		return COND_SAME;
	if (strcmp(value, "all") == 0)
		return COND_ALL;
	return -1;
}

static const char *backend_name(enum backend_mode mode)
{
	switch (mode) {
	case BACKEND_LIBC:
		return "libc";
	case BACKEND_COPIER:
		return "copier";
	case BACKEND_BOTH:
		return "both";
	}
	return "unknown";
}

static const char *condition_name(enum condition_mode mode)
{
	switch (mode) {
	case COND_COLD:
		return "cold";
	case COND_PINNED:
		return "pinned";
	case COND_SAME:
		return "same";
	case COND_ALL:
		return "all";
	}
	return "unknown";
}

static long elapsed_ns(const struct timespec *start, const struct timespec *end)
{
	return (end->tv_sec - start->tv_sec) * 1000000000L +
	       (end->tv_nsec - start->tv_nsec);
}

static void fill_source(uint8_t *buf, size_t bytes, uint8_t seed)
{
	size_t i;
	for (i = 0; i < bytes; i++)
		buf[i] = (uint8_t)(seed + (i % 251));
}

static uint64_t sum_destination(const uint8_t *buf, size_t bytes)
{
	size_t i;
	uint64_t sum = 0;
	for (i = 0; i < bytes; i += 64)
		sum += buf[i];
	if (bytes > 0)
		sum += buf[bytes - 1];
	return sum;
}

static void touch_buffer(uint8_t *buf, size_t bytes)
{
	size_t i;
	for (i = 0; i < bytes; i += 64)
		buf[i] ^= (uint8_t)i;
}

static int alloc_aligned(void **ptr, size_t bytes)
{
	if (posix_memalign(ptr, 4096, bytes) != 0)
		return -1;
	memset(*ptr, 0, bytes);
	return 0;
}

static int alloc_cold_dst(void **ptr, size_t bytes)
{
	// mmap guarantees fresh, unpopulated virtual pages. 
    // They will page-fault and zero-fill ONLY when the memcpy actually writes to them.
	*ptr = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if (*ptr == MAP_FAILED)
		return -1;
	return 0;
}

static void compute_mean_std(const long *values, int count, double *mean, double *stddev)
{
	int i;
	double sum = 0.0;
	double variance = 0.0;

	for (i = 0; i < count; i++)
		sum += (double)values[i];
	*mean = sum / (double)count;

	for (i = 0; i < count; i++) {
		double diff = (double)values[i] - *mean;
		variance += diff * diff;
	}
	variance /= (double)count;
	*stddev = sqrt(variance);
}

static int run_case(enum backend_mode backend,
		    enum condition_mode condition,
		    size_t bytes,
		    int iterations,
		    int warmup)
{
	int total_rounds = iterations + warmup;
	int queuefd = -1;
	int pinned = (condition == COND_PINNED);
	size_t evict_bytes = (size_t)DEFAULT_EVICT_MB * 1024UL * 1024UL;
	uint8_t *evict_buf = NULL;
	uint8_t *reusable_dst = NULL;
	long *copy_records = NULL;
	long *total_records = NULL;
	volatile uint16_t *descriptors = NULL;
	struct queues_for_u2u *queues = NULL;
	uint64_t checksum = 0;
	int rc = 1;
	int round;

	if (alloc_aligned((void **)&evict_buf, evict_bytes) != 0)
		goto out;
	memset(evict_buf, 1, evict_bytes);

	if (condition == COND_SAME || condition == COND_PINNED) {
		if (alloc_aligned((void **)&reusable_dst, bytes) != 0)
			goto out;
		if (pinned && mlock(reusable_dst, bytes) != 0)
			perror("mlock reusable_dst");
	}

	copy_records = calloc((size_t)iterations, sizeof(long));
	total_records = calloc((size_t)iterations, sizeof(long));
	if (copy_records == NULL || total_records == NULL)
		goto out;

	if (backend == BACKEND_COPIER) {
		queuefd = createCpThread();
		if (queuefd < 0) {
			perror("createCpThread");
			goto out;
		}
		queues = mmapQueues(queuefd);
		if (queues == MAP_FAILED || queues == NULL) {
			perror("mmapQueues");
			goto out;
		}
		descriptors = calloc((bytes + GRANULARITY - 1) / GRANULARITY,
				     sizeof(uint16_t));
		if (descriptors == NULL)
			goto out;
		if (mlock((void *)descriptors,
			  ((bytes + GRANULARITY - 1) / GRANULARITY) * sizeof(uint16_t)) != 0)
			perror("mlock descriptors");
	}

	for (round = 0; round < total_rounds; round++) {
		uint8_t *src = NULL;
		uint8_t *dst = reusable_dst;
		struct timespec copy_start, copy_end, total_end;
		int record_index = round - warmup;

		if (alloc_aligned((void **)&src, bytes) != 0)
			goto out;
		fill_source(src, bytes, (uint8_t)(round + 1));

		if (condition == COND_COLD) {
			if (alloc_cold_dst((void **)&dst, bytes) != 0) {
				free(src);
				goto out;
			}
			touch_buffer(evict_buf, evict_bytes);
		} else if (condition == COND_PINNED) {
			touch_buffer(evict_buf, evict_bytes);
		}

		if (pinned && mlock(src, bytes) != 0)
			perror("mlock src");

		if (clock_gettime(CLOCK_MONOTONIC, &copy_start) != 0) {
			free(src);
			if (condition == COND_COLD)
				munmap(dst, bytes);
			goto out;
		}

		if (backend == BACKEND_LIBC) {
			memcpy(dst, src, bytes);
		} else {
			size_t block;
			size_t block_count = (bytes + GRANULARITY - 1) / GRANULARITY;

			memset((void *)descriptors, 0,
			       ((bytes + GRANULARITY - 1) / GRANULARITY) * sizeof(uint16_t));
			memcpyAsync(dst, src, &(queues->cp_queue), descriptors, bytes, GRANULARITY);
			for (block = 0; block < block_count; block++) {
				size_t remaining = bytes - block * GRANULARITY;
				size_t expected = remaining < GRANULARITY ? remaining : GRANULARITY;
				while (descriptors[block] < expected)
					;
			}
		}

		clock_gettime(CLOCK_MONOTONIC, &copy_end);
		checksum ^= sum_destination(dst, bytes);
		clock_gettime(CLOCK_MONOTONIC, &total_end);

		if (record_index >= 0) {
			copy_records[record_index] = elapsed_ns(&copy_start, &copy_end);
			total_records[record_index] = elapsed_ns(&copy_start, &total_end);
		}

		if (pinned)
			munlock(src, bytes);
		free(src);
		if (condition == COND_COLD)
			munmap(dst, bytes);
	}

	{
		double copy_mean, copy_std, total_mean, total_std;
		compute_mean_std(copy_records, iterations, &copy_mean, &copy_std);
		compute_mean_std(total_records, iterations, &total_mean, &total_std);
		printf("%s,%s,%zu,%d,%.2f,%.2f,%.2f,%.2f,%llu\n",
		       backend_name(backend),
		       condition_name(condition),
		       bytes / (1024UL * 1024UL),
		       iterations,
		       copy_mean,
		       copy_std,
		       total_mean,
		       total_std,
		       (unsigned long long)checksum);
	}

	rc = 0;

out:
	if (descriptors != NULL) {
		munlock((void *)descriptors,
			((bytes + GRANULARITY - 1) / GRANULARITY) * sizeof(uint16_t));
		free((void *)descriptors);
	}
	if (queues != NULL && queues != MAP_FAILED)
		munmapQueues(queues);
	if (queuefd >= 0)
		delCpThread(queuefd);
	free(copy_records);
	free(total_records);
	if (reusable_dst != NULL) {
		if (pinned)
			munlock(reusable_dst, bytes);
		free(reusable_dst);
	}
	free(evict_buf);
	return rc;
}

int main(int argc, char *argv[])
{
	enum backend_mode backend;
	enum condition_mode condition;
	size_t size_mb;
	size_t bytes;
	int iterations = DEFAULT_ITERATIONS;
	int warmup = DEFAULT_WARMUP;
	int rc = 0;

	if (argc < 4) {
		usage(argv[0]);
		return 1;
	}

	backend = parse_backend(argv[1]);
	condition = parse_condition(argv[2]);
	size_mb = strtoull(argv[3], NULL, 10);
	if ((int)backend < 0 || (int)condition < 0 || size_mb == 0) {
		usage(argv[0]);
		return 1;
	}
	if (argc > 4)
		iterations = atoi(argv[4]);
	if (argc > 5)
		warmup = atoi(argv[5]);
	if (iterations <= 0 || warmup < 0) {
		usage(argv[0]);
		return 1;
	}

	bytes = size_mb * 1024UL * 1024UL;

	printf("backend,condition,size_mb,iterations,copy_mean_ns,copy_std_ns,total_mean_ns,total_std_ns,checksum\n");

	if (backend == BACKEND_BOTH) {
		if (condition == COND_ALL) {
			rc |= run_case(BACKEND_LIBC, COND_COLD, bytes, iterations, warmup);
			rc |= run_case(BACKEND_LIBC, COND_PINNED, bytes, iterations, warmup);
			rc |= run_case(BACKEND_LIBC, COND_SAME, bytes, iterations, warmup);
			rc |= run_case(BACKEND_COPIER, COND_COLD, bytes, iterations, warmup);
			rc |= run_case(BACKEND_COPIER, COND_PINNED, bytes, iterations, warmup);
			rc |= run_case(BACKEND_COPIER, COND_SAME, bytes, iterations, warmup);
		} else {
			rc |= run_case(BACKEND_LIBC, condition, bytes, iterations, warmup);
			rc |= run_case(BACKEND_COPIER, condition, bytes, iterations, warmup);
		}
	} else if (condition == COND_ALL) {
		rc |= run_case(backend, COND_COLD, bytes, iterations, warmup);
		rc |= run_case(backend, COND_PINNED, bytes, iterations, warmup);
		rc |= run_case(backend, COND_SAME, bytes, iterations, warmup);
	} else {
		rc |= run_case(backend, condition, bytes, iterations, warmup);
	}

	return rc ? 1 : 0;
}
