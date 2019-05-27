#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <mpi.h>

#include "word_count.h"

void print_usage();

int main(int argc, char *argv[]) 
{
	int opt, world_rank, num_ranks;
	int repeat = 1;
	char *filename = NULL;

	int hr = MPI_Init(&argc, &argv);
	if (hr != MPI_SUCCESS)
	{
		MPI_Abort(MPI_COMM_WORLD, hr);
		fprintf(stderr, "Error with MPI_Init\n");
		exit(1);
	}

	MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
  	MPI_Comm_size(MPI_COMM_WORLD, &num_ranks);

	while ((opt = getopt(argc, argv, "r:i:")) != -1) {
		switch (opt) {
			case 'r':
				repeat = atoi(optarg);
				break;
			case 'i':
				filename = optarg;
				break;
			default:
				if (world_rank == 0) print_usage(argv[0]);
				MPI_Finalize();
				exit(1);
		}
	}
	/*
	if (argv[optind] == NULL || argv[optind + 1] == NULL || argv[optind + 2] == NULL) {
		if (world_rank == 0) print_usage(argv[0]);
		MPI_Finalize();
		exit(1);
	}
	*/

	/*
	MPI_Finalize();printf("%s\n", text[0]);
	if(world_rank == 0 )
		if(filename == NULL)
			print_usage(argv[0]);
	*/

	int iterations;
  	char** text = readFile(filename, world_rank, num_ranks, &iterations);

	KeyValue** buckets = map(world_rank, num_ranks, iterations, text);

	printf("\n\n\n");

	int l, j;
	int a[2] = {136, 82};
	for(l = 0; l < num_ranks; l++)
	{
		printf("DESTINATION RANK %d\n", l);

		for(j = 0; j < a[l]; j++)
		{
			printf("Position: %d\nKey: %s\nValue: %d\n\n", j, buckets[l][j].key, buckets[l][j].value);
		}
	}

	int b = 0;
  	for(int i = 0; i < 1000000000; i++)
  		b += 2;

  	printf("%d\n", b);

	
	return 0;
}

void print_usage(char *program) 
{
	fprintf(stderr, "Usage: %s [-r repeat] [-i input-file]\n", program);
	exit(1);
}
