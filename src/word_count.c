#include "word_count.h"

const char 			 key_seed[SEED_LENGTH] = "b4967483cf3fa84a3a233208c129471ebc49bdd3176c8fb7a2c50720eb349461";
const unsigned short *key_seed_num 		   = (unsigned short*)key_seed;

struct Config {
	/* MPI rank information */
	int rank, num_ranks;

	/* Number of iterations to read the all file */
	int iterations;

	/* Buffer to store all the data read from the file */
	char** text;

	/* Buckets to store the <key, value> pairs for each rank */
	KeyValue** buckets;

	/* New MPI_Datatype to be possible to send the <key, value> pairs */
	MPI_Datatype MPI_KeyValue;

	/* Variables to process all the redistribution of the <key, value> pairs */
	int sendBufSize;
	int* sendBucketSizes;
	int recvBufSize;
	int* recvBucketSizes;
	int* sendDispls;
	int* recvDispls;
	KeyValue* sendBuf;
	KeyValue* recvBuf;

	KeyValue* reducedBuf;
};

struct Config config;

void readFile(char* filename)
{
	MPI_File fh;
	MPI_Offset filesize;

	MPI_Comm_rank(MPI_COMM_WORLD, &config.rank);
  	MPI_Comm_size(MPI_COMM_WORLD, &config.num_ranks);

  	if(config.rank == 0)
	{
		MPI_File_open(MPI_COMM_SELF, filename, MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);
		MPI_File_get_size(fh, &filesize);
		MPI_File_close(&fh);

		printf("File Size in bytes: %llu\nFile Size in MB: %f\n\n", filesize, (double)(filesize*0.00000095367432));
	}

	MPI_Bcast(&filesize, 1, MPI_INT, 0, MPI_COMM_WORLD);

	MPI_Aint length = CHUNK_SIZE * sizeof(char);
	MPI_Aint extent = config.num_ranks * length;
	MPI_Offset disp = config.rank * length;
	MPI_Datatype contig, filetype;

	float aux = (float)filesize / (float)extent;

	if( (aux != 0) && (aux - (int)aux == 0) )
		config.iterations = (int)aux;
	else
		config.iterations = (int)aux + 1;

	MPI_Type_contiguous(CHUNK_SIZE, MPI_CHAR, &contig);
	MPI_Type_create_resized(contig, 0, extent, &filetype);
	MPI_Type_commit(&filetype);

	int i;
	config.text = (char**) malloc(config.iterations * sizeof(char*));
	for(i = 0; i < config.iterations; i++)
		config.text[i] = (char*) malloc(CHUNK_SIZE * sizeof(char));

	MPI_File_open(MPI_COMM_WORLD, filename, MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);
	MPI_File_set_view(fh, disp, MPI_CHAR, filetype, "native", MPI_INFO_NULL);

	for(i = 0; i < config.iterations; i++)
		MPI_File_read_all(fh, config.text[i], CHUNK_SIZE, MPI_CHAR, MPI_STATUS_IGNORE);

	MPI_File_close(&fh);

	for(i = 0; i < config.iterations; i++)
	{
		if(strlen(config.text[i])==0)
			printf("Rank: %d\ntext[%d]: empty!!!\n\n", config.rank, i);
		else
			printf("Rank: %d\nstrlen: %lu\ntext[%d][0]: %c\n\n", config.rank, strlen(config.text[i]), i, config.text[i][0]);
	}
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
	int i, flag = 1;

	int destRank = getDestRank(new_word, strlen(new_word) + 1);

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
			config.buckets[destRank] = (KeyValue*) realloc(config.buckets[destRank], 2 * config.sendBucketSizes[destRank] * sizeof(KeyValue));
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


void map()
{
	int i, j, x, splitString;

	config.sendBufSize = 0;

	config.sendBucketSizes = (int*) calloc(config.num_ranks, sizeof(int));

	config.buckets = (KeyValue**) malloc(config.num_ranks * sizeof(KeyValue*));

	for(i = 0; i < config.num_ranks; i++)
		config.buckets[i] = (KeyValue*) malloc(BUCKET_SIZE * sizeof(KeyValue));

	for(i = 0; i < config.iterations; i++)
	{
		if(strlen(config.text[i]) != 0)
		{
			tokenize(config.text[i]);

			char* new_word = strtok(config.text[i], " ");
			while (new_word)
			{
				if( (strlen(new_word) + 1) < WORD_LENGTH )
				{
					updatingBuckets(new_word);
				}
				else
				{
					int splitString = (int)(strlen(new_word)/WORD_LENGTH) + 1;

					char** aux_word = (char**) malloc(splitString * sizeof(char*));

					for(j = 0; j < splitString; j++)
					{
						if(j != splitString-1)
						{
							aux_word[j] = (char*) malloc(WORD_LENGTH * sizeof(char));

							for(x = 0; x < WORD_LENGTH - 1; x++)
								aux_word[j][x] = new_word[x + j*(WORD_LENGTH-1)];;

							aux_word[j][x] = '\0';
						}
						else
						{
							aux_word[j] = (char*) malloc(( strlen(new_word) - ( (splitString - 1) * (WORD_LENGTH - 1) ) ) * sizeof(char));

							for(x = 0; x < ( strlen(new_word) - ( (splitString - 1) * (WORD_LENGTH - 1) ) ); x++)
								aux_word[j][x] = new_word[x + j*(WORD_LENGTH-1)];

							aux_word[j][x] = '\0';
						}
					}

					for(j = 0; j < splitString; j++)
						updatingBuckets(aux_word[j]);
				}

				new_word = strtok(NULL, " ");
			}
		}
	}

	/*
	printf("\nRank: %d\ntotalWords: %d\n", config.rank, config.sendBufSize);
	for(i = 0; i < config.num_ranks; i++)
	{
		printf("config.sendBucketSizes[%d]: %d\n", i, config.sendBucketSizes[i]);
		for(j = 0; j < 10; j++)
			printf("config.buckets[%d][%d].key: %s\nconfig.buckets[%d][%d].value: %d\n", i, j, config.buckets[i][j].key, i, j, config.buckets[i][j].value);
	}
	*/
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
	config.recvBufSize = 0;
	config.recvBucketSizes = (int*) calloc(config.num_ranks, sizeof(int));
	config.sendDispls = (int*) calloc(config.num_ranks, sizeof(int));
	config.recvDispls = (int*) calloc(config.num_ranks, sizeof(int));

	for(int i = 0; i < config.num_ranks; i++)
	{
		if(config.sendBucketSizes[i] == 0)
		{
			config.sendBufSize += 1;
		}
	}

	MPI_Alltoall(config.sendBucketSizes, 1, MPI_INT, config.recvBucketSizes, 1, MPI_INT,  MPI_COMM_WORLD);

	//IF A BUCKET IS EMPTY IN CASE OF A PROCESS NOT READ ANYTHING (SMALL FILE CASE)
	
	for(int i = 0; i < config.num_ranks; i++)
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
	
	int sDispAux = 0;
	int rDispAux = 0;
	
	int i;
	for(i = 0; i < config.num_ranks; i++)
	{
		config.sendDispls[i] = sDispAux;
		config.recvDispls[i] = rDispAux;

		if(config.sendBucketSizes[i] > 0)
		{
			for(int j = 0; j < config.sendBucketSizes[i]; j++)
			{
				config.sendBuf[j+sDispAux] = config.buckets[i][j];
			}

			sDispAux += config.sendBucketSizes[i];
		} 
		else 
		{
			config.sendBuf[sDispAux].value = (uint64_t)-1;
			sDispAux += 1;
			config.sendBucketSizes[i] += 1;
		}

		if(config.recvBucketSizes > 0)
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
	
	/*
	if(config.rank == 0) 
	{
		for(int i = 0; i < config.recvBufSize; i++)
		{
			printf("config.recvBuf[%d].key: %s\n", i, config.recvBuf[i].key);
			printf("config.recvBuf[%d].value: %ld\n", i, config.recvBuf[i].value);
		}
	}
	*/
}

void reduce()
{
	int i, j;
	int aux = 0, aux2 = 1, auxValue = 0, counter = 0;

	int start = config.recvDispls[1];
	int size = config.recvBucketSizes[0];

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
			if (aux2 != config.num_ranks)
			{
				for (j = start; j < config.recvBufSize; j++)
				{
					if (strcmp(config.recvBuf[i].key, config.recvBuf[j].key) == 0)
					{
						auxValue += config.recvBuf[j].value;

						config.recvBuf[j].key[0] = '\0';

						//if(strlen(config.recvBuf[j].key) == 0)
						//	printf("position: %d WORD: %s\n", config.recvBuf[i].key);
					}
				}
			}
			
			counter++;
			config.reducedBuf[counter - 1] = config.recvBuf[i];
			config.reducedBuf[counter - 1].value += auxValue;
			auxValue = 0;
		}
	}

	if (config.rank == 0)
	{
		printf("config.recvBufSize: %d counter: %d\n", config.recvBufSize, counter);
		for(i = 0; i < counter-1; i++)
			printf("position: %d WORD: %s VALUE: %d\n", i, config.reducedBuf[i].key, config.reducedBuf[i].value);
	}
}