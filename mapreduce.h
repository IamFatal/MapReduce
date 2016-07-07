#ifndef MAPREDUCE_H
#define MAPREDUCE_H

#define MAX_KEY 64       // Max size of key, including null-terminator.
#define MAX_VALUE 256    // Max size of value, including null-terminator.
#define MAX_FILENAME 32  // Max length of input file path, including null-terminator.
#define READSIZE 128     // Number of bytes to read per chunk of input file.
                         //   - You should allocate one more byte than this number
                         //     for a final null-terminator after these bytes.

void map_worker(int outfd, int infd);
void reduce_worker(int outfd, int infd);

// A key-value pair emitted by a map function.
// All keys and values must be null-terminated.
typedef struct pair {
    char key[MAX_KEY];
    char value[MAX_VALUE];
} Pair;

// Linked list - each node contains a (string) value.
// value must be null-terminated.
typedef struct valuelist {
    char value[MAX_VALUE];
    struct valuelist *next;
} LLValues;

// Linked list - each node contains a unique key and list of corresponding values.
// key must be null-terminated.
typedef struct keyValues {
    char key[MAX_KEY];
    LLValues *head_value;
    struct keyValues *next;
} LLKeyValues;


/*
 * Takes a chunk of text and generates zero or more
 * Pair values, which it writes to outfd.
 *
 * Precondition: chunk is a null-terminated string.
 */
void map(const char *chunk, int outfd);

/*
 * Takes a key and list of values, and returns a new
 * Pair.
 *
 * Precondition: key and all strings in values are null-terminated.
 */
Pair reduce(const char *key, const LLValues *values);


#endif
