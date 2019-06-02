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
#include <inttypes.h>

#include <mpi.h>

//Hash Function
#define SEED_LENGTH 65
typedef uint64_t Hash;

/* 64 MB */
#define CHUNK_SIZE 64 * 1024 * 1024
#define WORD_LENGTH 32
#define BUCKET_SIZE 100

/* Structure to store the <key,value> pairs */
typedef struct KeyValue 
{
	char key[WORD_LENGTH];
	uint64_t value;
} KeyValue;

void initialization();

/* Function for reading the file */
void readFile(char* filename);

/* Functions for the Map() phase */
void tokenize(char* text_array);
Hash getDestRank(const char *word, size_t length);
void updatingBuckets(char* new_word);
void map(char* text_array);

/* Functions for redistributing the <key, value> pairs */
void createKeyValueDatatype();
void redistributeKeyValues();

/* Functions for the Reduce() phase */
void reduce();

/* Functions for writing the file */
MPI_Datatype createOutputDatatype(int localSize, int* allSizes);
void writeFile();

/* Function for cleaning up the memory */
void cleanup();

#endif //__WORD_COUNT_H__