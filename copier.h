#include <linux/types.h>
#include <linux/kernel.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <unistd.h>
#include "def.h"

#define CACHE_LINE_SIZE 64
#define DEFUALT_CP_ENTRY_NUM (4096)
#define DEFUALT_SYNC_ENTRY_NUM (16)

#define QUEUE_TYPE_U2U 4

#define STATUS_WAITING 1
#define STATUS_DONE 3
#define STATUS_SPARSE 5

struct u2u_cp_entry {
	void *from;
	void *to;
	void *base;
	unsigned long size;
	volatile uint16_t *descriptors;
	int8_t status;
	unsigned int granularity;
};

struct u2u_sync_entry {
	void *start_addr;
	unsigned long size;
};

struct u2u_sync_queue {
	volatile unsigned int thread_read_index __attribute__((aligned(CACHE_LINE_SIZE)));
	volatile unsigned int write_index __attribute__((aligned(CACHE_LINE_SIZE)));
	struct u2u_sync_entry entries[DEFUALT_SYNC_ENTRY_NUM] __attribute__((aligned(CACHE_LINE_SIZE)));
};

struct u2u_cp_queue {
	volatile unsigned int thread_read_index __attribute__((aligned(CACHE_LINE_SIZE)));
	volatile unsigned int write_index __attribute__((aligned(CACHE_LINE_SIZE)));
	struct u2u_cp_entry entries[DEFUALT_CP_ENTRY_NUM] __attribute__((aligned(CACHE_LINE_SIZE)));
};

struct queues_for_u2u {
	struct u2u_sync_queue sync_queue;
	struct u2u_cp_queue cp_queue;
};

#define createCpThread() syscall(602, -1, U2U_COPIER_THREAD_CORE, QUEUE_TYPE_U2U);
#define delCpThread(queue_fd) syscall(603, -1, queue_fd, QUEUE_TYPE_U2U);

struct queues_for_u2u *mmapQueues(int fd);
void munmapQueues(struct queues_for_u2u *queues);
size_t memcpyAsync(void *dst, void *src, struct u2u_cp_queue *cpQueue, volatile uint16_t *descriptors, size_t n, int granularity);
size_t memcpyAsyncLifo(void *dst, void *src, struct u2u_cp_queue *cpQueue, volatile uint16_t *descriptors, size_t n);
void smartBufferGetAligned(void *io_base, int blockNum, size_t length, volatile uint16_t *descriptors, struct u2u_sync_queue *syncQueue, int granularity);
void smartBufferGetAlignedOnlyWait(void *io_base, int blockNum, size_t length, volatile uint16_t *descriptors);
