#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <ctype.h>

#define CHUNK_SIZE 64

char** read_file();
void tokenize();
void print_usage();

int main(int argc, char *argv[]) {
	int opt, world_rank, num_ranks;
  int repeat = 1;
	char *filename = NULL;

	MPI_Init(&argc, &argv);
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
		}
	}
  char** text = read_file(filename, world_rank, num_ranks);
	tokenize(text);
	free(text);

	MPI_Finalize();printf("%s\n", text[0]);
  if(world_rank == 0 ) {
    if(filename == NULL) {
      print_usage(argv[0]);
    }
  }
	return 0;
}

char** read_file(char *filename, int rank, int num_ranks) {
  MPI_File fh;
	MPI_Offset filesize;
	MPI_File_open(MPI_COMM_WORLD, filename, MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);
	MPI_File_get_size(fh, &filesize);
  MPI_Aint length = CHUNK_SIZE * sizeof(int);
  MPI_Offset disp = rank * length;

	int nblocks = (int) filesize / num_ranks;
	char **result = (char**)malloc(nblocks*sizeof(char*));
	for(int i = 0; i < nblocks; i++) {
		result[i] = (char*)malloc(CHUNK_SIZE*sizeof(char));
	}

	for(int i = 0; i < nblocks; i++) {
		MPI_File_seek(fh, disp, MPI_SEEK_SET);
  	MPI_File_read(fh, result[i], CHUNK_SIZE, MPI_INT, MPI_STATUS_IGNORE);
		disp += num_ranks * length;

	}
	return result;
}

void tokenize(char **text_array) {
	for(int i = 0; i < sizeof(text_array); i++) {
		for (char *p = text_array[i]; *p; p++)
    	if (!isalpha(*p)) *p = ' ';

		char* token = strtok(text_array[i], " ");
		while (token) {
    	printf("token: %s\n", token);
    	token = strtok(NULL, " ");
		}
	}
}

void print_usage(char *program) {
	fprintf(stderr, "Usage: %s [-i input-file] [-r repeat]\n", program);
	exit(1);
}
