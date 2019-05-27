#ifndef __WORD_COUNT_H__
#define __WORD_COUNT_H__

#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>

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

char** readFile();
void tokenize();
Hash getDestRank();
KeyValue** map();


#endif