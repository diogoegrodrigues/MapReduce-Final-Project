#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <mpi.h>

#include "word_count.h"

void print_usage(char*);

int main(int argc, char *argv[])
{
	int opt, repeat = 1;
	char* filename = NULL;
	char* outfile = NULL;

	int hr = MPI_Init(&argc, &argv);
	if (hr != MPI_SUCCESS)
	{
		MPI_Abort(MPI_COMM_WORLD, hr);
		fprintf(stderr, "Error with MPI_Init\n");
		exit(1);
	}

	while ((opt = getopt(argc, argv, "r:i:o:")) != -1) {
		switch (opt) {
			case 'r':
				repeat = atoi(optarg);
				break;
			case 'i':
				filename = optarg;
				break;

			default:
				//if (world_rank == 0) print_usage(argv[0]);
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

	readFile(filename);

	map();

	createKeyValueDatatype();

	MPI_Barrier(MPI_COMM_WORLD);

	redistributeKeyValues();

	reduce();

	writeFile();

	MPI_Finalize();
	return 0;
}

void print_usage(char *program)
{
	fprintf(stderr, "Usage: %s [-r repeat] [-i input-file]\n", program);
	exit(1);
}
