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

  	char** text = read_file(filename, world_rank, num_ranks);

  	printf("text: %s\n", text[0]);
	//tokenize(text);
	//free(text);

	/*

	MPI_Finalize();printf("%s\n", text[0]);
	if(world_rank == 0 )
		if(filename == NULL)
			print_usage(argv[0]);

			*/
	int a = 0;
  	for(int i = 0; i < 10000; i++)
  		a += 2;

  	printf("%d\n", a);
	
	return 0;
}

void print_usage(char *program) 
{
	fprintf(stderr, "Usage: %s [-r repeat] [-i input-file]\n", program);
	exit(1);
}
