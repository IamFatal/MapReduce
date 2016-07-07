#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "mapreduce.h"

/*
 * Wrapper function for closing pipes and handling errors.
 */
void close_pipe(int fd);


void map_worker(int outfd, int infd) {
	int n;
	FILE *in_file;
	char filename[MAX_FILENAME];
	char chunk[READSIZE + 1];
	
	while ((n = read(infd, filename, MAX_FILENAME)) != 0) {
		if (n == -1) {
			perror("read");
			exit(1);
		}

		// open given file for reading
		in_file = fopen(filename, "r");
		if (!in_file) {
			perror("fopen");
			exit(1);
		}
		
		// read file contents into chunk, call map on each chunk until EOF
		while((fread(chunk, sizeof(char), READSIZE, in_file)) != 0) {
			chunk[READSIZE] = '\0';
			map(chunk, outfd);
		}
		
		if ((fclose(in_file)) == -1) {
            perror("fclose");
            exit(1);
        }
	}
	
	// no more data is being sent to/from worker
	close_pipe(infd);	// close infd
    close_pipe(outfd);	// close outfd
	exit(0); 			// end process
}
