#ifndef __WORD_COUNT_H__
#define __WORD_COUNT_H__

#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>
#include <stdint.h>
#include <stddef.h>

#include <mpi.h>

//Hash Function
#define SEED_LENGTH 65
typedef uint64_t Hash;

// 64 MB
#define CHUNK_SIZE 64 * 1024 * 1024
#define WORD_LENGTH 32
#define BUCKET_SIZE 100

typedef struct KeyValue {
	char key[WORD_LENGTH];
	uint64_t value;
} KeyValue;

// Function for reading the file
char** readFile(char* filename, int rank, int num_ranks, int* iterations);

// Functions for the Map() phase
void tokenize(char* text_array);
Hash getDestRank(const char *word, size_t length, int num_ranks);
void updatingBuckets(int num_ranks, char* new_word, int* word_counter, KeyValue** buckets, int flag);
KeyValue** map(int rank, int num_ranks, int iterations, char** text, int *sdispls);

// Functions for redistributing the <key, value> pairs
void createKeyValueDatatype(MPI_Datatype *MPI_KeyValue);

void reduce(KeyValue** buckets, int rank, int num_ranks, int *sdispls);

#endif //__WORD_COUNT_H__
