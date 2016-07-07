CFLAGS = -Wall -std=c99 -Werror -D_POSIX_SOURCE

mapreduce: master.o mapworker.o reduceworker.o word_freq.o
	gcc $(CFLAGS) -o mapreduce master.o mapworker.o reduceworker.o word_freq.o
	
master.o: master.c mapreduce.h
	gcc $(CFLAGS) -c master.c

word_freq.o: word_freq.c mapreduce.h
	gcc $(CFLAGS) -c word_freq.c
	
mapworker.o: mapworker.c mapreduce.h
	gcc $(CFLAGS) -c mapworker.c
	
reduceworker.o: reduceworker.c mapreduce.h linkedlist.h 
	gcc $(CFLAGS) -c reduceworker.c
	
clean:
	rm mapreduce *.o