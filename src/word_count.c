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

	float aux = (float)filesize / (float)extent;

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
		word_counter[destRank]++;

		if( (word_counter[destRank] % BUCKET_SIZE) == 0 )
		{
			buckets[destRank] = (KeyValue*) realloc(buckets[destRank], 2 * word_counter[destRank] * sizeof(KeyValue));
			if(buckets[destRank] == NULL)
			{
				fprintf(stderr, "Error in realloc\n");
				MPI_Finalize();
				exit(1);
			}
		}

		for(i = 0; i < strlen(new_word); i++)
			buckets[destRank][word_counter[destRank] - 1].key[i] = new_word[i];

		buckets[destRank][word_counter[destRank] - 1].key[i] = '\0';

		buckets[destRank][word_counter[destRank] - 1].value = 1;
	}
}

KeyValue** map(int rank, int num_ranks, int iterations, char** text, int *sdispls)
{
	int i, j, flag = 1;

	int* word_counter = (int*) calloc(num_ranks, sizeof(int));

	KeyValue** buckets = (KeyValue**) malloc(num_ranks * sizeof(KeyValue*));

	for(i = 0; i < num_ranks; i++)
		buckets[i] = (KeyValue*) malloc(BUCKET_SIZE * sizeof(KeyValue));

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

	for(i = 0; i < num_ranks; i++)
	{
		printf("Rank: %d word_counter[%d]: %d\n", rank, i, word_counter[i]);
		for(j = 0; j < 10; j++)
			printf("buckets[%d][%d].key: %s\nbuckets[%d][%d].value: %d\n", i, j, buckets[i][j].key, i, j, buckets[i][j].value);
	}

	/*
	for(int i = 0; i < num_ranks; i++)
	{
		sdispls[i] = word_counter[i];
	}
	*/

	return buckets;
}

void createKeyValueDatatype(MPI_Datatype *MPI_KeyValue)
{
	const int count = 2;
	int array_of_blocklengths[2] = {WORD_LENGTH, 1};
	MPI_Aint array_of_displacements[2] = {offsetof(KeyValue, key), offsetof(KeyValue, value)};
	MPI_Datatype array_of_types[2] = {MPI_CHAR, MPI_UINT64_T};
	
    MPI_Type_create_struct(count, array_of_blocklengths, array_of_displacements, array_of_types, MPI_KeyValue);
    MPI_Type_commit(MPI_KeyValue);
    
    return;
}

void reduce(KeyValue** buckets, int rank, int num_ranks, int *sdispls)
{
	printf("RANK %d IN REDUCE\n", rank);
	int buf_size = 0;
	for(int i = 0; i < num_ranks; i++)
	{
		printf("\nRANK: %d\nSEND\nSIZE OF BUCKET[%d]: %d\n", rank,  i, sdispls[i]);
		buf_size += sdispls[i];
	}

	int *rdispls = (int*)malloc(num_ranks * sizeof(int));
	MPI_Alltoall(&sdispls, 1, MPI_INT, &rdispls, 1, MPI_INT,  MPI_COMM_WORLD);
	
	/*
	for(int i = 0; i < num_ranks; i++)
	{
		printf("\nRANK: %d\nRECV\nSIZE OF BUCKET[%d]: %d\n", rank, i, rdispls[i]);
	}
	/*
	KeyValue *sendbuf = (KeyValue*)malloc(buf_size * sizeof(KeyValue));
	KeyValue *recvbuf = (KeyValue*)malloc(num_ranks * BUCKET_SIZE * sizeof(KeyValue));
	for(int i = 0; i < num_ranks; i++)
	{
		for(int j = 0; j < sdispls[i]; j++)
		{
			sendbuf[j+sdispls[i]] = buckets[i][j];
		}
	}
	*/
}


/*
int Calculate_SendRecv_Count_And_Displs(std::vector<std::map<std::string, uint64_t> > &bucket, int *s_redistr_sizes,
  int *r_redistr_sizes, int *s_redistr_displs,int *r_redistr_displs, uint64_t &num_keyvalues_tot, const int num_ranks){
    for(int i = 0; i < num_ranks; i++) {
      s_redistr_sizes[i] = bucket[i].size();
      s_redistr_displs[i] = num_keyvalues_tot;
      num_keyvalues_tot += bucket[i].size();
    }

    MPI_Alltoall(s_redistr_sizes, 1, MPI_INT, r_redistr_sizes, 1, MPI_INT, MPI_COMM_WORLD);

    r_redistr_displs[0] = 0;
    for(int i = 1; i < num_ranks; i++) {
      r_redistr_displs[i] = r_redistr_sizes[i-1] + r_redistr_displs[i-1];
    }

    return 1;
}
*/