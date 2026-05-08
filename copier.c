#include "copier.h"
#include <bits/time.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>
#include <stdatomic.h>


#include <x86intrin.h>

inline void smartBufferGet(void *io_base, unsigned long offset, unsigned long length, volatile uint16_t *descriptors,
			   struct u2u_sync_queue *syncQueue)
{
	return;
}

inline void smartBufferGetAligned(void *io_base, int blockNum, size_t length, volatile uint16_t *descriptors, struct u2u_sync_queue *syncQueue, int granularity)
{
	if (descriptors[blockNum] < length) {
		unsigned int write_index = syncQueue->write_index;
		struct u2u_sync_entry *sync_entry = &syncQueue->entries[write_index];
		sync_entry->size = length;
		sync_entry->start_addr = io_base + blockNum * granularity;
		syncQueue->write_index = (write_index + 1) % DEFUALT_SYNC_ENTRY_NUM;
		while (descriptors[blockNum] < length)
			;
		// printf("blockNum = %d, descriptors[blockNum] = %d\n", blockNum, descriptors[blockNum]);
	}
}

inline void smartBufferGetAlignedOnlyWait(void *io_base, int blockNum, size_t length, volatile uint16_t *descriptors)
{
	if (descriptors[blockNum] < length) {
		// end = __rdtsc();
		// cycles_sync += end - start;
		// times_sync++;
		while (descriptors[blockNum] < length)
			;
	}
	// else{
	// 	end = __rdtsc();
	// 	cycles_sync += end - start;
	// 	times_sync++;
	// }
	
}

inline size_t memcpyAsync(void *dst, void *src, struct u2u_cp_queue *cpQueue, volatile uint16_t *descriptors, size_t n, int granularity)
{
	unsigned int write_index = cpQueue->write_index;
	struct u2u_cp_entry *cp_entry = &cpQueue->entries[write_index];
	cp_entry->base = dst;
	cp_entry->from = src;
	cp_entry->to = dst;
	cp_entry->size = n;
	cp_entry->status = STATUS_WAITING;
	cp_entry->granularity = granularity;
	cp_entry->descriptors = descriptors;
	cpQueue->write_index = (write_index + 1) % DEFUALT_CP_ENTRY_NUM;
	return n;
}

// inline size_t memcpyAsyncLifo(void *dst, void *src, struct u2u_cp_queue *cpQueue, volatile uint16_t *descriptors, size_t n)
// {
// 	memset((void *)descriptors, 0, (n + COPY_GRANULARITY - 1) / COPY_GRANULARITY * sizeof(uint16_t));
// 	struct u2u_cp_entry *cp_entry = &cpQueue->entries[cpQueue->write_index];
// 	cp_entry->base = dst;
// 	cp_entry->from = src;
// 	cp_entry->to = dst;
// 	cp_entry->size = n;
// 	cp_entry->status = STATUS_WAITING;
// 	cp_entry->granularity = COPY_GRANULARITY;
// 	cp_entry->descriptors = descriptors;
// 	cp_entry->lifo = 1;
// 	cpQueue->write_index = (cpQueue->write_index + 1) % DEFUALT_CP_ENTRY_NUM;
// 	return n;
// }

inline struct queues_for_u2u *mmapQueues(int fd)
{
	return (struct queues_for_u2u *)mmap(NULL, sizeof(struct queues_for_u2u), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
}

inline void munmapQueues(struct queues_for_u2u *queues)
{
	munmap(queues, sizeof(struct queues_for_u2u));
}
