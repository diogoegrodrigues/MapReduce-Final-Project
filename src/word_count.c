#include <ctype.h>
#include <stdint.h>

#include "word_count.h"

const char 			 key_seed[SEED_LENGTH] = "b4967483cf3fa84a3a233208c129471ebc49bdd3176c8fb7a2c50720eb349461";
const unsigned short *key_seed_num 		   = (unsigned short*)key_seed;

struct Config 
{
	/* MPI rank information */
	int rank, num_ranks;

	/* Number of iterations to read the all file */
	uint64_t iterations;

	/* Buffer to store the 64 MB chunks read from the file in each iteration.
	They are 2 so we can read from the file using collective I/O and 
	map the <key, value> pairs in parallel */
	char* textInput;
	char* textInput2;

	/* Buckets to store the <key, value> pairs for each rank */
	KeyValue** buckets;

	/* New MPI_Datatype to be possible to send the <key, value> pairs */
	MPI_Datatype MPI_KeyValue;

	/* Variables to process all the redistribution of the <key, value> pairs */
	/* Send */
	uint64_t sendBufSize;
	int* sendBucketSizes;
	int* sendDispls;
	KeyValue* sendBuf;
	/* Receive */
	uint64_t recvBufSize;
	int* recvBucketSizes;
	int* recvDispls;
	KeyValue* recvBuf;

	/* Variable to count all the <key, value> pairs after the reduction phase and array to store them */
	uint64_t counter;
	KeyValue* reducedBuf;

	/* Array to store the sizes for writing the output file */
	int* allSizes;
};

struct Config config;

void initialization()
{
	uint64_t i;

	MPI_Comm_rank(MPI_COMM_WORLD, &config.rank);
	MPI_Comm_size(MPI_COMM_WORLD, &config.num_ranks);

	config.textInput = (char*) malloc(CHUNK_SIZE * sizeof(char));
	config.textInput2 = (char*) malloc(CHUNK_SIZE * sizeof(char));

	config.buckets = (KeyValue**) malloc(config.num_ranks * sizeof(KeyValue*));
	for(i = 0; i < config.num_ranks; i++)
		config.buckets[i] = (KeyValue*) malloc(BUCKET_SIZE * sizeof(KeyValue));

  	config.sendBufSize = 0;
	config.sendBucketSizes = (int*) calloc(config.num_ranks, sizeof(int));
	config.sendDispls = (int*) calloc(config.num_ranks, sizeof(int));

	config.recvBufSize = 0;
	config.recvBucketSizes = (int*) calloc(config.num_ranks, sizeof(int));
	config.recvDispls = (int*) calloc(config.num_ranks, sizeof(int));

	config.allSizes = (int*) calloc(config.num_ranks, sizeof(int));
}


void readFile(char* filename)
{
	MPI_File fh;
	MPI_Offset filesize;
	MPI_Request request;

  	if(config.rank == 0)
	{
		MPI_File_open(MPI_COMM_SELF, filename, MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);
		MPI_File_get_size(fh, &filesize);
		MPI_File_close(&fh);

		printf("File Size in bytes: %llu\nFile Size in MB: %f\n\n", filesize, (double)(filesize*0.00000095367432));
	}

	MPI_Bcast(&filesize, 1, MPI_LONG, 0, MPI_COMM_WORLD);

	MPI_Aint length = CHUNK_SIZE * sizeof(char);
	MPI_Aint extent = config.num_ranks * length;
	MPI_Offset disp = config.rank * length;
	MPI_Datatype contig, filetype;

	float aux = (float)filesize / (float)extent;

	if( (aux != 0) && (aux - (uint64_t)aux == 0) )
		config.iterations = (uint64_t)aux;
	else
		config.iterations = (uint64_t)aux + 1;

	MPI_Type_contiguous(CHUNK_SIZE, MPI_CHAR, &contig);
	MPI_Type_create_resized(contig, 0, extent, &filetype);
	MPI_Type_commit(&filetype);

	MPI_File_open(MPI_COMM_WORLD, filename, MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);
	MPI_File_set_view(fh, disp, MPI_CHAR, filetype, "native", MPI_INFO_NULL);

	MPI_File_read_all(fh, config.textInput, CHUNK_SIZE, MPI_CHAR, MPI_STATUS_IGNORE);
		
	uint64_t i;
	for(i = 1; i < config.iterations; i++)
	{
		MPI_File_iread_all(fh, config.textInput2, CHUNK_SIZE, MPI_CHAR, &request);
		
		map(config.textInput);

		MPI_Wait(&request, MPI_STATUS_IGNORE);

		strcpy(config.textInput, config.textInput2);
	}

	map(config.textInput);

	MPI_File_close(&fh);
}

void tokenize(char* text_array)
{
	char* p;

	for (p = text_array; *p; p++)
		if (!isalnum(*p)) *p = ' ';
}

Hash getDestRank(const char *word, size_t length)
{
    Hash hash = 0;

    uint64_t i;
    for (i = 0; i < length; i++)
    {
        Hash num_char = (Hash)word[i];
        Hash seed     = (Hash)key_seed_num[(i % SEED_LENGTH)];

        hash += num_char * seed * (i + 1);
    }

    return (uint64_t)(hash % (uint64_t)config.num_ranks);
}

void updatingBuckets(char* new_word)
{
	uint64_t i, flag = 1;

	uint64_t destRank = getDestRank(new_word, strlen(new_word) + 1);

	for(i = 0; i <= config.sendBucketSizes[destRank]; i++)
	{
		if(!strcmp(config.buckets[destRank][i].key, new_word))
		{
			config.buckets[destRank][i].value++;

			flag = 0;
			break;
		}
	}

	if(flag)
	{
		config.sendBucketSizes[destRank]++;
		config.sendBufSize++;

		if( (config.sendBucketSizes[destRank] % BUCKET_SIZE) == 0 )
		{
			config.buckets[destRank] = (KeyValue*) realloc(config.buckets[destRank], (config.sendBucketSizes[destRank] * 2) * sizeof(KeyValue));
			if(config.buckets[destRank] == NULL)
			{
				fprintf(stderr, "Error in realloc\n");
				MPI_Finalize();
				exit(1);
			}
		}

		for(i = 0; i < strlen(new_word); i++)
			config.buckets[destRank][config.sendBucketSizes[destRank] - 1].key[i] = new_word[i];

		config.buckets[destRank][config.sendBucketSizes[destRank] - 1].key[i] = '\0';

		config.buckets[destRank][config.sendBucketSizes[destRank] - 1].value = 1;
	}

	flag = 1;
}

void map(char* text_array)
{
	uint64_t i, j, splitString;
	
	if(strlen(text_array) != 0)
	{
		tokenize(text_array);

		char* new_word = strtok(text_array, " ");
		while (new_word)
		{
			if( strlen(new_word) < WORD_LENGTH )
			{
				updatingBuckets(new_word);
			}
			else
			{
				splitString = (uint64_t)(strlen(new_word)/WORD_LENGTH) + 1;

				char** aux_word = (char**) malloc(splitString * sizeof(char*));

				for(i = 0; i < splitString; i++)
				{
					if(i != splitString - 1)
					{
						aux_word[i] = (char*) malloc(WORD_LENGTH * sizeof(char));

						for(j = 0; j < WORD_LENGTH - 1; j++)
							aux_word[i][j] = new_word[j + i*(WORD_LENGTH-1)];;

						aux_word[i][j] = '\0';
					}
					else
					{
						aux_word[i] = (char*) malloc((strlen(new_word) - (((splitString - 1) * (WORD_LENGTH - 1))) + 1) * sizeof(char));

						for(j = 0; j < ( strlen(new_word) - ( (splitString - 1) * (WORD_LENGTH - 1) ) ); j++)
							aux_word[i][j] = new_word[j + i*(WORD_LENGTH-1)];

						aux_word[i][j] = '\0';
					}
				}

				for(i = 0; i < splitString; i++)
				{
					updatingBuckets(aux_word[i]);
					free(aux_word[i]);
				}

				free(aux_word);
			}

			new_word = strtok(NULL, " ");
		}
	}
}

void createKeyValueDatatype()
{
	const int count = 2;
	int array_of_blocklengths[2] = {WORD_LENGTH, 1};
	MPI_Aint array_of_displacements[2] = {offsetof(KeyValue, key), offsetof(KeyValue, value)};
	MPI_Datatype array_of_types[2] = {MPI_CHAR, MPI_UINT64_T};
	
    MPI_Type_create_struct(count, array_of_blocklengths, array_of_displacements, array_of_types, &config.MPI_KeyValue);
    MPI_Type_commit(&config.MPI_KeyValue);
}

void redistributeKeyValues()
{
	uint64_t i, j;

	MPI_Alltoall(config.sendBucketSizes, 1, MPI_INT, config.recvBucketSizes, 1, MPI_INT,  MPI_COMM_WORLD);

	/* The else condition shown below is in the case of a bucket that is empty. 
	This just happen if a process not read anything from the file 
	(case of the wikipedia_test_small.txt) */
	for(i = 0; i < config.num_ranks; i++)
	{	
		if(config.recvBucketSizes[i] > 0)
		{
			config.recvBufSize += config.recvBucketSizes[i];
		} 
		else 
		{
			config.recvBufSize += 1;
			config.recvBucketSizes[i] = 1;
		}
	}

	config.sendBuf = (KeyValue*) malloc(config.sendBufSize * sizeof(KeyValue));
	config.recvBuf = (KeyValue*) malloc(config.recvBufSize * sizeof(KeyValue));
	
	uint64_t sDispAux = 0;
	uint64_t rDispAux = 0;
	
	for(i = 0; i < config.num_ranks; i++)
	{
		config.sendDispls[i] = sDispAux;
		config.recvDispls[i] = rDispAux;

		if(config.sendBucketSizes[i] > 0)
		{
			for(j = 0; j < config.sendBucketSizes[i]; j++)
			{
				config.sendBuf[j + sDispAux] = config.buckets[i][j];
			}

			sDispAux += config.sendBucketSizes[i];
		} 
		else 
		{
			config.sendBuf[sDispAux].key[0] = '\0';
			config.sendBuf[sDispAux].value = 0;
			sDispAux += 1;
			config.sendBucketSizes[i] = 1;
		}

		if(config.recvBucketSizes[i] > 0)
		{
			rDispAux += config.recvBucketSizes[i];
		} 
		else 
		{
			rDispAux += 1;
			config.recvBucketSizes[i] = 1;
		}
	}
	
	MPI_Alltoallv(config.sendBuf, config.sendBucketSizes, config.sendDispls, config.MPI_KeyValue, config.recvBuf, config.recvBucketSizes, config.recvDispls, config.MPI_KeyValue, MPI_COMM_WORLD);
}

void reduce()
{
	uint64_t i, j;
	uint64_t aux = 0, aux2 = 1, auxValue = 0;
	config.counter = 0;

	uint64_t start = config.recvDispls[1];
	uint64_t size = config.recvBucketSizes[0];

	config.reducedBuf = (KeyValue*) malloc(config.recvBufSize * sizeof(KeyValue));

	for(i = 0; i < config.recvBufSize; i++)
	{
		if(i == size)
		{
			size += config.recvBucketSizes[++aux];
			start = config.recvDispls[++aux2];
		}

		if(strlen(config.recvBuf[i].key) != 0)
		{
			if(aux2 != config.num_ranks)
			{
				for(j = start; j < config.recvBufSize; j++)
				{
					if(strcmp(config.recvBuf[i].key, config.recvBuf[j].key) == 0)
					{
						auxValue += config.recvBuf[j].value;

						config.recvBuf[j].key[0] = '\0';
					}
				}
			}
			
			config.counter++;
			config.reducedBuf[config.counter - 1] = config.recvBuf[i];
			config.reducedBuf[config.counter - 1].value += auxValue;
			auxValue = 0;
		}
	}
}


MPI_Datatype createOutputDatatype(int localSize, int* allSizes)
{
	int startPosition = 0, totalSize = 0;
	uint64_t i;

    for(i = 0; i < config.num_ranks; i++)
    {
    	if(i < config.rank)
    		startPosition += allSizes[i];

    	totalSize += allSizes[i];
    }

    // Datatype that permits write results to file with collective I/O
    MPI_Datatype outputDatatype;
    MPI_Type_create_subarray(1, &totalSize, &localSize, &startPosition, MPI_ORDER_C, MPI_CHAR, &outputDatatype);
    MPI_Type_commit(&outputDatatype);

    return outputDatatype;
}

void writeFile()
{
	MPI_Request request;

	uint64_t i, localSize = 0;
	for(i = 0; i < config.counter; i++) 
	{
        localSize += strlen(config.reducedBuf[i].key);

        localSize += (uint64_t)(log10(abs(config.reducedBuf[i].value))) + 1;

        // For the semicolon, space and newline
        localSize += 3;
    }

    MPI_Iallgather(&localSize, 1, MPI_INT, config.allSizes, 1, MPI_INT, MPI_COMM_WORLD, &request);
	
    char* outputBuf = (char *) malloc((localSize + 1) * sizeof(char));

    uint64_t aux = 0;
    for(i = 0; i < config.counter; i++)
    	aux += sprintf(&outputBuf[aux], "%s; %"PRIu64"\n", config.reducedBuf[i].key, config.reducedBuf[i].value);

    outputBuf[aux] = '\0';

    MPI_Wait(&request, MPI_STATUS_IGNORE);

    MPI_Datatype outputDatatype = createOutputDatatype(localSize, config.allSizes);

    MPI_File fh;
    MPI_File_open(MPI_COMM_WORLD, "results.txt", MPI_MODE_CREATE | MPI_MODE_WRONLY, MPI_INFO_NULL, &fh);
    MPI_File_set_view(fh, 0, MPI_CHAR, outputDatatype, "native", MPI_INFO_NULL);
    MPI_File_write_all(fh, outputBuf, localSize, MPI_CHAR, MPI_STATUS_IGNORE);
    MPI_File_close(&fh);

    free(outputBuf);
}

void cleanup()
{
	free(config.textInput);
	free(config.textInput2);

	uint64_t i;
	for(i = 0; i < config.num_ranks; i++)
		free(config.buckets[i]);
	free(config.buckets);

	free(config.sendBucketSizes);
	free(config.sendDispls);
	free(config.sendBuf);
	free(config.recvBucketSizes);
	free(config.recvDispls);
	free(config.recvBuf);
	free(config.reducedBuf);
	free(config.allSizes);
}