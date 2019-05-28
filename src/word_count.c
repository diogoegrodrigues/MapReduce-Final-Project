#include "word_count.h"

const char 			 key_seed[SEED_LENGTH] = "b4967483cf3fa84a3a233208c129471ebc49bdd3176c8fb7a2c50720eb349461";
const unsigned short *key_seed_num 		   = (unsigned short*)key_seed;

char** readFile(char* filename, int rank, int num_ranks, int* iterations)
{
	MPI_File fh;
	MPI_Offset filesize;

	if(rank == 0)
	{
		MPI_File_open(MPI_COMM_SELF, filename, MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);
		MPI_File_get_size(fh, &filesize);
		MPI_File_close(&fh);

		printf("File Size in bytes: %llu\nFile Size in MB: %f\n\n", filesize, (double)(filesize*0.00000095367432));
	}

	MPI_Bcast(&filesize, 1, MPI_INT, 0, MPI_COMM_WORLD);

	MPI_Aint length = CHUNK_SIZE * sizeof(char);
	MPI_Aint extent = num_ranks * length;
	MPI_Offset disp = rank * length;
	MPI_Datatype contig, filetype;

	float aux = (float)(filesize / extent);
	if( (aux != 0) && (aux - (int)aux == 0) )
		(*iterations) = (int)aux;
	else
		(*iterations) = (int)aux + 1;

	MPI_Type_contiguous(CHUNK_SIZE, MPI_CHAR, &contig);
	MPI_Type_create_resized(contig, 0, extent, &filetype);
	MPI_Type_commit(&filetype);

	int i;
	char** buffer = (char**) malloc((*iterations) * sizeof(char*));
	for(i = 0; i < (*iterations); i++)
		buffer[i] = (char*) malloc(CHUNK_SIZE * sizeof(char));

	MPI_File_open(MPI_COMM_WORLD, filename, MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);
	MPI_File_set_view(fh, disp, MPI_CHAR, filetype, "native", MPI_INFO_NULL);

	for(i = 0; i < (*iterations); i++)
		MPI_File_read_all(fh, buffer[i], CHUNK_SIZE, MPI_CHAR, MPI_STATUS_IGNORE);

	MPI_File_close(&fh);

	for(i = 0; i < (*iterations); i++)
	{
		if(strlen(buffer[i])==0)
			printf("Rank: %d\nbuffer[%d]: empty!!!\n\n", rank, i);
		else
			printf("Rank: %d\nsizeof: %lu\nstrlen: %lu\nbuffer[%d][0]: %c\n\n", rank, sizeof(buffer[i]), strlen(buffer[i]), i, buffer[i][0]);
	}

	return buffer;
}

void tokenize(char* text_array)
{
	char* p;

	for (p = text_array; *p; p++)
		if (!isalnum(*p)) *p = ' ';

	return;
}

Hash getDestRank(const char *word, size_t length, int num_ranks)
{
    Hash hash = 0;

    uint64_t i;
    for (i = 0; i < length; i++)
    {
        Hash num_char = (Hash)word[i];
        Hash seed     = (Hash)key_seed_num[(i % SEED_LENGTH)];

        hash += num_char * seed * (i + 1);
    }

    return (uint64_t)(hash % (uint64_t)num_ranks);
}

void updatingBuckets(int num_ranks, char* new_word, int* word_counter, KeyValue** buckets, int flag)
{
	int i;

	int destRank = getDestRank(new_word, strlen(new_word) + 1, num_ranks);

	for(i = 0; i <= word_counter[destRank]; i++)
	{
		if(!strcmp(buckets[destRank][i].key, new_word))
		{
			buckets[destRank][i].value++;

			flag = 0;
			break;
		}
	}

	if(flag)
	{
		for(i = 0; i < strlen(new_word); i++)
		{
			buckets[destRank][word_counter[destRank]].key[i] = new_word[i];
			//printf("Character: %c ", buckets[destRank][word_counter[destRank]].key[i]);
		}

		buckets[destRank][word_counter[destRank]].key[i] = '\0';

		//printf("\nbuckets[%d][%d].key: %s\nstrlen(buckets[%d][%d].key): %lu\n\n", destRank, word_counter[destRank], buckets[destRank][word_counter[destRank]].key, destRank, word_counter[destRank], strlen(buckets[destRank][word_counter[destRank]].key));

		buckets[destRank][word_counter[destRank]].value = 1;

		word_counter[destRank]++;
		//printf("word_counter[destRank]: %d\n", word_counter[destRank]);

		if( (word_counter[destRank] % BUCKET_SIZE) == 0 )
		{
			//printf("\n------------------- REALLOC -------------------\n");
			buckets[destRank] = (KeyValue*) realloc(buckets[destRank], 2 * word_counter[destRank] * sizeof(KeyValue));
			if(buckets[destRank] == NULL)
			{
				fprintf(stderr, "Error in realloc\n");
				MPI_Finalize();
				exit(1);
			}
		}
	}
}

KeyValue** map(int rank, int num_ranks, int iterations, char** text, int *sbucket_sizes)
{
	int i, j, flag = 1;

	int* word_counter = (int*) calloc(num_ranks, sizeof(int));

	KeyValue** buckets = (KeyValue**) malloc(num_ranks * sizeof(KeyValue*));

	for(i = 0; i < num_ranks; i++)
		buckets[i] = (KeyValue*) malloc(BUCKET_SIZE * sizeof(KeyValue));

	//printf("sizeof(buckets[0][0].key): %lu\n", sizeof(buckets[0][0].key));

	for(i = 0; i < iterations; i++)
	{
		if(strlen(text[i]) != 0)
		{
			tokenize(text[i]);

			char* new_word = strtok(text[i], " ");
			while (new_word)
			{
				if( (strlen(new_word) + 1) < WORD_LENGTH )
				{
					updatingBuckets(num_ranks, new_word, word_counter, buckets, flag);
				}
				else
				{
					int l, x;
					int splitString = (int)(strlen(new_word)/WORD_LENGTH) + 1;

					char** aux_word = (char**) malloc(splitString * sizeof(char*));

					for(l = 0; l < splitString; l++)
					{
						if(l != splitString-1)
						{
							aux_word[l] = (char*) malloc(WORD_LENGTH * sizeof(char));

							for(x = 0; x < WORD_LENGTH - 1; x++)
								aux_word[l][x] = new_word[x + l*(WORD_LENGTH-1)];;

							aux_word[l][x] = '\0';
						}
						else
						{
							aux_word[l] = (char*) malloc(( strlen(new_word) - ( (splitString - 1) * (WORD_LENGTH - 1) ) ) * sizeof(char));

							for(x = 0; x < ( strlen(new_word) - ( (splitString - 1) * (WORD_LENGTH - 1) ) ); x++)
								aux_word[l][x] = new_word[x + l*(WORD_LENGTH-1)];

							aux_word[l][x] = '\0';
						}
					}

					for(l = 0; l < splitString; l++)
						updatingBuckets(num_ranks, aux_word[l], word_counter, buckets, flag);
				}

				flag = 1;
				new_word = strtok(NULL, " ");
			}
		}
	}

	for(int i = 0; i < num_ranks; i++)
	{
		sbucket_sizes[i] = word_counter[i];
	}

	return buckets;
}

void reduce(KeyValue** buckets, int rank, int num_ranks, int *sbucket_sizes)
{
	printf("RANK %d IN REDUCE\n", rank);
	int sbuf_size = 0;
	int rbuf_size = 0;
	int *rbucket_sizes = (int*)malloc(num_ranks * sizeof(int));
	int *scounts = (int*)malloc(num_ranks * sizeof(int));
	int *rcounts = (int*)malloc(num_ranks * sizeof(int));
	int *sdispls = (int*)malloc(num_ranks * sizeof(int));
	int *rdispls = (int*)malloc(num_ranks * sizeof(int));
	for(int i = 0; i < num_ranks; i++)
	{
		printf("\nRANK: %d\nSEND\nSIZE OF BUCKET[%d]: %d\n", rank,  i, sbucket_sizes[i]);
		if(sbucket_sizes[i] > 0)
		{
			sbuf_size += sbucket_sizes[i];
		} else {
			sbuf_size += 1;
		}
	}

	for (int i = 0; i < num_ranks; i++)
	{
			 rbucket_sizes[i] = 0;
	}
	MPI_Alltoall(sbucket_sizes, 1, MPI_INT, rbucket_sizes, 1, MPI_INT,  MPI_COMM_WORLD);

	for(int i = 0; i < num_ranks; i++)
	{
		printf("\nRANK: %d\nRECV\nSIZE OF BUCKET[%d]: %d\n", rank, i, rbucket_sizes[i]);
		rbuf_size += rbucket_sizes[i];
		scounts[i] = i;
		rcounts[i] = rank;
	}
	KeyValue *sendbuf = (KeyValue*)malloc(sbuf_size * sizeof(KeyValue));
	KeyValue *recvbuf = (KeyValue*)malloc(rbuf_size * sizeof(KeyValue));
	int rdisp = 0;
	int sdisp = 0;
	for(int i = 0; i < num_ranks; i++)
	{
		sdispls[i] = sdisp;
		rdispls[i] = rdisp;
		if(sbucket_sizes[i] > 0)
		{
			for(int j = 0; j < sbucket_sizes[i]; j++)
			{
				sendbuf[j+sdisp] = buckets[i][j];
			}
			sdisp += sbucket_sizes[i];
		} else {
			sendbuf[sdisp].value = (uint64_t)-1;
			sdisp += 1;
		}
		if(rbucket_sizes > 0)
		{
			rdisp = rbucket_sizes[i];
		} else {
			rdisp += 1;
		}
	}
	if(rank== 0)
	{
		printf("SENDBUF[0].value: %ld\n", sendbuf[0].value);
		printf("BUCKETS[0][0].value: %ld\n", buckets[0][0].value);
		printf("SENDBUF[0].key: %s\n", sendbuf[0].key);
		printf("BUCKETS[0][0].key: %s\n", buckets[0][0].key);
		printf("RANK 0: SIZE: %d\n", sbuf_size);
	} else {
		printf("RANK 1: SENDBUF[0].value: %ld\n", sendbuf[0].value);
		printf("RANK 1: SIZE: %d\n", sbuf_size);
	}
	MPI_Alltoallv(sendbuf, scounts, sdispls, MPI_INT, recvbuf, rcounts, rdispls, MPI_INT, MPI_COMM_WORLD);
}
