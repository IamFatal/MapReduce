#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "linkedlist.h"
#include "mapreduce.h"

/*
 * Create a new LLKeyValue with the given pair and insert it at the tail of
 * the list of keys whose head is pointed to by *key_list. If pair's key 
 * exists, append pair's value to existing key's values list.
 */
void add_pair(LLKeyValues **key_list, Pair pair);


/*
 * From the list pointed to by key_list, delete the given key
 * and free all memory allocated for the key.
 */
void remove_key(LLKeyValues **key_list, LLKeyValues *key);


/*
 * Wrapper function for closing pipes and handling errors.
 */
void close_pipe(int fd);


void reduce_worker(int outfd, int infd) {
	int n;
	Pair pair;
	LLKeyValues *key_list = NULL;

	// read in and add pairs to list until all pairs have been read
	while ((n = read(infd, &pair, sizeof(Pair))) != 0) {
		if (n == -1) {
			perror("read");
			exit(2);
		}
		add_pair(&key_list, pair);
	}

	// create file [pid].out and open for writing in binary
	char filename[MAX_FILENAME];
	char extension[5] = ".out";
	snprintf(filename, MAX_FILENAME, "%d%s", getpid(), extension);
	FILE *out_file = fopen(filename, "wb");
    if (!out_file) {
        perror("fopen");
        exit(1);
    }

	// reduce each key and write returned pair to output file
    LLKeyValues *temp = NULL;
	LLKeyValues *cur_key = key_list;
	while(cur_key != NULL) {
		pair = reduce(cur_key->key, cur_key->head_value);
		if ((fwrite(&pair, sizeof(Pair), 1, out_file)) == -1) {
			perror("write to pipe");
			exit(1);
		}
        temp = cur_key;
        cur_key = cur_key->next;
        remove_key(&key_list, temp);
	}
	
	close_pipe(infd);
	if ((fclose(out_file)) == -1) {
		perror("fclose");
		exit(1);
	}
	exit(0);
}


/*
 * Create a new LLKeyValue with the given pair and insert it at the tail of
 * the list of keys whose head is pointed to by *key_list. If pair's key 
 * exists, append pair's value to existing key's values list.
 */
void add_pair(LLKeyValues **key_list, Pair pair) {
	// create new LLValue for pair
	LLValues *new_val = malloc(sizeof(LLValues));
	if (new_val == NULL) {
		perror("malloc");
		exit(1);
	}
	strcpy(new_val->value, pair.value);
	new_val->next = NULL;

	// set cur_key to front of linked list of keys
	LLKeyValues *cur_key = *key_list;

	// check for first insertion
	if (cur_key == NULL) {
		LLKeyValues *new_key = malloc(sizeof(LLKeyValues));
		if (new_val == NULL) {
			perror("malloc");
			exit(1);
		}
		strcpy(new_key->key, pair.key);
		new_key->head_value = new_val;
		new_key->next = NULL;
		*key_list = new_key;
		return;
	}

    // iterate through list, if key exists, append value to its values list
	while(cur_key->next != NULL) {
		if (strcmp(pair.key, cur_key->key) == 0) {  // key found
			// append new value to the end of values list
			LLValues *cur_val = cur_key->head_value;
			while(cur_val->next != NULL) {
				cur_val = cur_val->next;
			}
			cur_val->next = new_val;
			return;
		}
		cur_key = cur_key->next;
	}

	// create new key node and append it to the end of key list
	LLKeyValues *new_key = malloc(sizeof(LLKeyValues));
	if (new_val == NULL) {
		perror("malloc");
		exit(1);
	}
	strcpy(new_key->key, pair.key);
	new_key->head_value = new_val;
	new_key->next = NULL;
	cur_key->next = new_key;
}


/*
 * From the list pointed to by key_list, delete the given key
 * and free all memory allocated for the key.
 */
void remove_key(LLKeyValues **key_list, LLKeyValues *key) {
    LLValues *temp;
    LLValues *head_value = key->head_value;
    
    // iterate through values list and free all allocated memory
    while (head_value != NULL) {
        temp = head_value;              // hold head with temp
        head_value = head_value->next;  // set head to next node in list
        free(temp);                     // free allocated memory for LLValue
    }
    
    LLKeyValues *head = key;            // set head to given key
    head = head->next;                  // set head to next node in list
    free(*key_list);                    // free key
    *key_list = head;                   // set head pointer to next node in list
}
