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
	int opt, world_rank, repeat = 1;
	char* filename = NULL;

	double avg_runtime = 0.0, prev_avg_runtime = 0.0, stddev_runtime = 0.0;
	double start_time, end_time;

	int hr = MPI_Init(&argc, &argv);
	if (hr != MPI_SUCCESS)
	{
		MPI_Abort(MPI_COMM_WORLD, hr);
		fprintf(stderr, "Error with MPI_Init\n");
		exit(1);
	}

	MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

	while ((opt = getopt(argc, argv, "r:i:o:")) != -1) 
	{
		switch (opt) 
		{
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

	int i;
	for (i = 0; i < repeat; i++)
	{
		initialization();
				
		MPI_Barrier(MPI_COMM_WORLD);
		start_time = MPI_Wtime();

		readFile(filename);

		createKeyValueDatatype();

		redistributeKeyValues();

		reduce();

		writeFile();

		MPI_Barrier(MPI_COMM_WORLD);
		end_time = MPI_Wtime();

		if (world_rank == 0)
			printf("run %d: %f s\n", i, end_time - start_time);

		prev_avg_runtime = avg_runtime;
		avg_runtime = avg_runtime + ( (end_time - start_time) - avg_runtime ) / (i + 1);
		stddev_runtime = stddev_runtime + ( (end_time - start_time) - avg_runtime) * ( (end_time - start_time) - prev_avg_runtime);

		cleanup();
	}

	if (world_rank == 0) 
	{
		stddev_runtime = sqrt(stddev_runtime / (repeat - 1));
		printf("duration\t= %f±%f\n", avg_runtime, stddev_runtime);
	}

	MPI_Finalize();
	return 0;
}

void print_usage(char *program)
{
	fprintf(stderr, "Usage: %s [-r repeat] [-i input-file]\n", program);
	exit(1);
}