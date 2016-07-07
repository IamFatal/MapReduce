#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>
#include "mapreduce.h"


/*
 * Executes ls process and rearranges and closes pipes accordingly.
 *		- ls process		stdout -> write end of pipe
 *		- parent process	stdin  -> read end of the pipe
 */
//void setup_ls_process(char *dirname, int ls_fd[2]);


/*
 * Creates specified number of map worker processes with required pipes.
 */
//void create_map_workers(int m_infd[][2], int m_outfd[][2], int m_numprocs);


/*
 * Creates specified number of reduce worker processes with required pipes.
 */
//void create_reduce_workers(int m_infd[][2], int r_numprocs);


/*
 * Wrapper function for closing pipes and handling errors.
 */
void close_pipe(int fd);


/*
 * Print usage message to stderr.
 */
void print_usage_msg(char *prog_name);


int main (int argc, char **argv) {
	int m_numprocs = 0;		// number of map workers to be created
	int r_numprocs = 0;		// number of reduce workers to be created
	char *dirname = NULL;	// directory path
	
	// parse arguments using getopt and handle any errors
	int c;
	opterr = 0;

	while ((c = getopt(argc, argv, "m:r:d:")) != -1) {
		switch (c) {
			case 'm':
				m_numprocs = (int) strtol(optarg, NULL, 10);
				if (m_numprocs <= 0) {
					print_usage_msg(argv[0]);
					exit(1);
				}
				break;
			case 'r':
				r_numprocs = (int) strtol(optarg, NULL, 10);
				if (r_numprocs <= 0) {
					print_usage_msg(argv[0]);
					exit(1);
				}
				break;
			case 'd':
				dirname = optarg;
				break;
			case '?':
				if (optopt == 'm' || optopt == 'r' || optopt == 'd'){
					fprintf(stderr, 
					"Option -%c requires an argument.\n", optopt);
					print_usage_msg(argv[0]);
				} else if (isprint (optopt)) {
					fprintf(stderr, "Unknown option `-%c'.\n", optopt);
					print_usage_msg(argv[0]);
				} else {
					print_usage_msg(argv[0]);
				}
				exit(1);
			default:
				abort();
		}
	}
	
	// set default values for workers if none were specified
	if (m_numprocs <= 0) {
		m_numprocs = 2;
	}
	if (r_numprocs <= 0) {
		r_numprocs = 2;
	}

	// exit and print usage message if directory was not provided
	if (!dirname || strlen(dirname) >= MAX_FILENAME) {
		print_usage_msg(argv[0]);
        exit(1);
	}
	
	// add forward slash to end of dirname if not already present
	if (dirname[strlen(dirname) - 1] != '/') {
		strcat(dirname, "/");
	}
	
	// create a pipe for ls process
	int ls_fd[2];
	if (pipe(ls_fd) == -1) {
		perror("pipe");
		exit(1);
	}
	
	//setup_ls_process(dirname, ls_fd);	// setup child ls process
	int result = fork();				// create child process
	if (result == 0) {	// child process
		// set stdout so that it writes to the pipe
        if ((dup2(ls_fd[1], fileno(stdout))) == -1) {
            perror("dup2");
            exit(1);
        }
		
        // close write end of the pipe
        close_pipe(ls_fd[1]);
		
        // close read end of the pipe
        close_pipe(ls_fd[0]);
		
		// execute ls program with dirname as an arg
        execlp("ls", "ls", "-1", dirname, NULL);
        perror("execlp");
        exit(1);
    } else if (result > 0) { // parent process
		// set stdin so that it reads from the pipe
        if ((dup2(ls_fd[0], fileno(stdin))) == -1) {
            perror("dup2");
            exit(1);
        }
		
        // close write end of the pipe
        close_pipe(ls_fd[1]);
		
        // since stdin reads from pipe, we can close fd[0]
        close_pipe(ls_fd[0]);
	}
	else {
		perror("fork");
		exit(1);
	}
	
	// file descriptor arrays of specified size for map workers
	int m_infd[m_numprocs][2];
	int m_outfd[m_numprocs][2];
    
	//create map workers
	for (int i = 0; i < m_numprocs; i++) {
		// create pipes for this child
		if (pipe(m_infd[i]) == -1 || pipe(m_outfd[i]) == -1) {
			perror("pipe");
			exit(1);
		}
		
		int result = fork();
		if (result < 0) {
			perror("fork");
			exit(1);
		}
		else if (result > 0) {	// parent process
			close_pipe(m_outfd[i][1]);		// close write end of outfd
			close_pipe(m_infd[i][0]);		// close read end of infd
		}
		else if (result == 0) {	// child process
			
			// close write ends for previously forked children
			for (int j = 0; j < i; j++) {
				close_pipe(m_infd[j][1]);
				close_pipe(m_outfd[j][0]);
			}
			
			close_pipe(m_outfd[i][0]);		// close read end of outfd
			close_pipe(m_infd[i][1]);		// close write end of infd
			
			map_worker(m_outfd[i][1], m_infd[i][0]);
		}
	}
	
	int worker = 0;
	char filename[MAX_FILENAME];
	char path[MAX_FILENAME];
	while(scanf("%s", filename) != EOF) {
		if ((strlen(filename) + strlen(dirname) + 1) > MAX_FILENAME) {
			continue;
		}
		strcpy(path, dirname);
		strcat(path, filename);
		
		// write filepath to a map worker's pipe
		if (write(m_infd[worker][1], path, MAX_FILENAME) == -1) {
			perror("write to pipe");
			exit(1);
		}
		
		// increment to next worker
		// loop back to first worker once last worker is reached
		worker += 1;
		if (worker == m_numprocs) {
			worker = 0;
		}
	}
	
	// all filenames have been written
	for (int i = 0; i < m_numprocs; i++) {
		close_pipe(m_infd[i][1]); // close write ends going to map workers
	}
    
    // file descriptor array of specified size for reduce workers
	int r_infd[r_numprocs][2];
    
    // create reduce workers
	for (int i = 0; i < r_numprocs; i++) {
		// create pipe for this child
		if (pipe(r_infd[i]) == -1) {
			perror("pipe");
			exit(1);
		}
		
		int result = fork();
		if (result < 0) {
			perror("fork");
			exit(1);
		}
		else if (result > 0) {	// parent process
			close_pipe(r_infd[i][0]);		// close read end
		}
		else {	// child process
		
			// close write ends for previously forked children
			for (int j = 0; j < i; j++) {
				close_pipe(r_infd[j][1]);
			}

			close_pipe(r_infd[i][1]);		// close write end
			
			reduce_worker(-1, r_infd[i][0]);
		}
	}
    
    // read in pairs and send them to reduce workers
    int n;
	Pair pair;
	for (int i = 0; i < m_numprocs; i++) {
		while ((n = read(m_outfd[i][0], &pair, sizeof(pair))) != 0) {
			if (n == -1) {
				perror("read");
				exit(1);
			}
            if ((write(r_infd[0][1], &pair, sizeof(pair))) == -1) {
                perror("write to pipe");
                exit(1);
            }
		}
	}
    
    // close write ends to reduce workers
	for (int i = 0; i < r_numprocs; i++) {
		close_pipe(r_infd[i][1]);
	}
	
	// number of processes = map processes + reduce processes + ls process
    int num_procs = m_numprocs + r_numprocs + 1;
    
    // wait for children processes to exit before terminating
	for (int i = 0; i < num_procs; i++) {
        pid_t pid;
        int status;
        if((pid = wait(&status)) == -1) {
            perror("wait");
			exit(1);
        } /* else {
            if (WIFEXITED(status)) {
                printf("Child %d terminated with %d\n",
                    pid, WEXITSTATUS(status));
            }
        } */
    } 
	
	return 0;
}


/*
 * Wrapper function for closing pipes.
 */
void close_pipe(int fd) {
	if ((close(fd)) == -1) {
		perror("close");
		exit(1);
	}
}


/*
 * Print usage message to stderr.
 */
void print_usage_msg(char *prog_name) {
	fprintf(stderr, 
		"usage: %s -d dirname [-m numprocs] [-r numprocs]\n\n \
		\t-d is mandatory, directory name must be less than 32 characters\n \
		\t-m numprocs defines the number of map workers (default is 2)\n \
		\t-r numprocs defines the number of reduce workers (default is 2)\n",
		prog_name);
}


