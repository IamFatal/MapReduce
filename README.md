# MapReduce
A simple engine to run programs written using a MapReduce model.

**Master:** creates the processes to perform map and reduce operations, assigns input files to the map processes, and facilitates communication between the different processes using pipes.

**Map Worker:** reads in input filenames sent by the master process. For each input file, it opens the file and reads in chunks into a buffer, calling a map function on each chunk.

**Reduce Worker:** reads a sequence of key/value pairs from a pipe. It builds up a list of pairs for each distinct key, and calls the reduce function once for each key. The reduce worker takes each returned pair and writes it (in binary) to a file named [pid].out, where [pid] is replaced by the PID of the worker.

word_freq.c contains a sample implementation of map and reduce functions. User can provide map and reduce functions.

Running make produces an executable called `mapreduce`, which takes the following command line option arguments:  
* **-m numprocs**, where numprocs is the number of map workers. If not provided, default is 2.
* **-r numprocs**, where numprocs is the number of reduce workers. Default is 2.
* **-d dirname**, where dirname is the absolute or relative path to a directory containing input files. This argument is *required*.

The folder texts contains sample data which can be used for testing.
