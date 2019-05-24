#include "word_count.h"

// 64 MB
#define CHUNK_SIZE 2*1024//64 * 1024 * 1024

char** read_file(char *filename, int rank, int num_ranks)
{
	MPI_File fh;
	MPI_Offset filesize;

	if(rank == 0)
	{
		MPI_File_open(MPI_COMM_SELF, filename, MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);
		MPI_File_get_size(fh, &filesize);
		MPI_File_close(&fh);

		//printf("File Size in bytes: %lu\nFile Size in MB: %f\n\n", filesize, (double)(filesize*0.00000095367432));
	}

	MPI_Bcast(&filesize, 1, MPI_INT, 0, MPI_COMM_WORLD);

	MPI_Aint length = CHUNK_SIZE * sizeof(char);
	MPI_Aint extent = num_ranks * length;
	MPI_Offset disp = rank * length;
	MPI_Datatype contig, filetype;

	int iterations = ceil( (double) (filesize / extent) );
	//int iterations = 3;

	MPI_Type_contiguous(CHUNK_SIZE, MPI_CHAR, &contig);
	MPI_Type_create_resized(contig, 0, extent, &filetype);
	MPI_Type_commit(&filetype);

	char** buffer = (char**) malloc(iterations * sizeof(char*));
	int i;
	for(i = 0; i < iterations; i++)
		buffer[i] = (char*) malloc(CHUNK_SIZE*sizeof(char));

	MPI_File_open(MPI_COMM_WORLD, filename, MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);
	MPI_File_set_view(fh, disp, MPI_CHAR, filetype, "native", MPI_INFO_NULL);

	for(i = 0; i < iterations; i++)
		MPI_File_read_all(fh, buffer[i], CHUNK_SIZE, MPI_CHAR, MPI_STATUS_IGNORE);

	MPI_File_close(&fh);

	for(i = 0; i < iterations; i++)
	{
		if(strlen(buffer[i])==0)
			printf("Rank: %d\nbuffer[%d]: empty!!!\n\n", rank, i);
		else
			printf("Rank: %d\nstrlen: %lu\nbuffer[%d][0]: %c\n\n", rank, strlen(buffer[i]), i, buffer[i][0]);
	}

	return buffer;
}

void tokenize(char **text_array) {
	for(int i = 0; i < sizeof(text_array); i++) {
		if(text_array[i] == NULL) {
			return;
		} else {
			for (char *p = text_array[i]; *p; p++)
    		if (!isalnum(*p)) *p = ' ';

			char* token = strtok(text_array[i], " ");
			while (token) {
    		printf("token: %s\n", token);
    		token = strtok(NULL, " ");
			}
		}
	}
}
