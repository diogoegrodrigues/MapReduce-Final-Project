#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>

#define CHUNK_SIZE 64

void print_usage();
void read_file();

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
  read_file(filename, world_rank, num_ranks);

	MPI_Finalize();
  if(world_rank == 0 ) {
    if(filename != NULL) {
      printf("filepath: %s \nrepeat: %d \n", filename, repeat);
    } else {
      print_usage(argv[0]);
    }
  }
	return 0;
}

void read_file(char *filename, int rank, int num_ranks) {
  MPI_File fh;
  char *buffer = (char*)malloc(CHUNK_SIZE*sizeof(char));
  MPI_Aint length = CHUNK_SIZE * sizeof(int);
  MPI_Aint extent = num_ranks * length;
  MPI_Offset disp = rank * length;
  MPI_Offset filesize;
  MPI_Datatype contig, filetype;

  MPI_Type_contiguous(CHUNK_SIZE, MPI_INT, &contig);
  MPI_Type_create_resized(contig, 0, extent, &filetype);
  MPI_Type_commit(&filetype);

  MPI_File_open(MPI_COMM_WORLD, filename, MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);
  MPI_File_get_size(fh, &filesize);
  MPI_File_set_view(fh, disp, MPI_INT, filetype, "native", MPI_INFO_NULL);
  MPI_File_read_all(fh, buffer, CHUNK_SIZE, MPI_INT, MPI_STATUS_IGNORE);

  printf("rank: %d\nbuffer: %s\n", rank, buffer);
  printf("\n");
  free(buffer);
}

void print_usage(char *program) {
	fprintf(stderr, "Usage: %s [-i input-file] [-r repeat]\n", program);
	exit(1);
}
