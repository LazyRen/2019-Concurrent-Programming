




#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdbool.h>
#include <assert.h>



#define TUPLE_SIZE	(100)
#define KEY_SIZE	(10)



int compare(const void* p1, const void* p2) {
	return memcmp(p1, p2, KEY_SIZE);
}




int main(int argc, char* argv[]) {

	if (argc < 3) {
		printf("usage: ./run InputFile OutputFile\n");
		return 0;
	}

	// open input file.
	int input_fd;
	input_fd = open(argv[1], O_RDONLY);
	if (input_fd == -1) {
		printf("error: open input file\n");
		return 0;
	}

	// get size of input file.
	size_t file_size;
	file_size = lseek(input_fd, 0, SEEK_END);

	char* buffer;
	buffer = (char*) malloc(sizeof(char) * file_size);

	// read data from input file.
	for (size_t offset = 0; offset < file_size; ) {
		size_t ret = pread(input_fd, buffer + offset, file_size - offset, offset);
		if (ret < 0) {
			printf("error: read input file\n");
		}
		offset = offset + ret;
	}

	// sort.
	qsort(buffer, file_size / TUPLE_SIZE, TUPLE_SIZE, compare);

	// open output file.
	int output_fd;
	output_fd = open(argv[2], O_RDWR | O_CREAT | O_TRUNC, 0777);
	if (output_fd == -1) {
		printf("error: open output file\n");
		return 0;
	}

	// flush to output file.
	for (size_t offset = 0; offset < file_size; ) {
		size_t ret = pwrite(output_fd, buffer + offset, file_size - offset, offset);
		if (ret < 0) {
			printf("error: write output file\n");
		}
		offset = offset + ret;
	}

	// free buffer.
	free(buffer);

	// close input file.
	close(input_fd);

	// close output file.
	close(output_fd);

	return 0;
}







