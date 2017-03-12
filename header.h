/*
 * header.h
 *
 *  Created on: Apr 22, 2009
 *      Author: jianbinqin
 */

#ifndef HEADER_H_
#define HEADER_H_

#include "hash_dict.h"


#define RECORD_MAX_LINE_LEN 32*1024    // Line lenght for per line
#define RECORD_MAX_LINE_NUM 10*1024*1024 // max line number per data
#define MAX_CHUNK_NUM 1024*1024*10		// Max CHUNK number.
#define VIRTUAL_BOUND_LEN 20
#define CHUNK_BLOCK_CHUNK_NUM 100000
#define MAX_FILE_SIZE 1024*1024*1024


// Filtering Macro

#define PASSED 0
#define FILTERED_BY_STRING_LENGTH 1
#define FILTERED_BY_CHUNK_NUMBER 2
#define FILTERED_BY_LOCATION_BASED_MISMATCH 3
#define FILTERED_BY_CONTENT_BASED_MISMATCH 4
#define FILTERED_BY_VIRTUAL_CHUNK_NUMBER 5
#define FILTERED_BY_MATCHED_POSITION_INFO 6


typedef struct _idf_node{
  int record_id;
  int pos;
  int order;
  int porder;
} idf_node_t;

// This is the chunk data type.
typedef struct _chunk_t{
  unsigned long long sign;

#ifdef DEBUG
  char *str;
  int len;
  int candn;
#endif

  int frq;
  int pfx_num;
  int head_idf;
  int last_idf;
  idf_node_t *idf_list;
} chunk_t;

typedef struct _chunk_block_t{
  struct _chunk_t chunks[CHUNK_BLOCK_CHUNK_NUM];
  int last_free;
  struct _chunk_block_t *next;
} chunk_block_t;


typedef struct _chunk_slot{
  unsigned int pos;
  unsigned int order;
  unsigned int len;
  struct _chunk_t *chunk;
} chunk_slot_t;

/* This structure restore the content of the documents.*/
typedef struct _record{
  char *str;
  int len;

#ifdef DEBUG
  int candn;
  int resultn;
#endif

  int vcn[VIRTUAL_BOUND_LEN];
  int chunk_len;
  int prefix_len;
  int stat;
  struct _chunk_slot *chunk_slots;
} record_t;


// interface from bound.c
int init_bound_map(char *bound_dict_file, int seed);
int split_string_by_bound(char *str, int str_len, int *idx, int *vcn);
int destroy_bound();


// interface from filter.c
int string_length_filtering(record_t *ra, record_t *rb, int tau);
int position_filtering(record_t *ra, int posa, int ordera, record_t *rb, int posb, int orderb, int tau);
int chunk_number_filtering(record_t *ra, record_t *rb, int tau);
int virtual_chunk_number_filtering( record_t *ra, record_t *rb, int tau);
int content_based_mismatch_filtering(record_t *ra, record_t *rb, int tau);


// interface from index.c
void init_index(int size);
int build_record_chunks(char *str, int* chunk_idx, int chunk_len, chunk_slot_t *chunk_slots);
chunk_t* insert_chunk_node(char *str, int len, int record_id);
int destroy_index();
void dump_chunks();



// interface from joins.c
void sort_all_chunk_list_by_freq(record_t *records, int records_num);
void calculate_all_prefix_length(record_t *records, int records_num, int tau);
int ppjoin_ed(record_t * records,int records_num, int tau);

// interface from record.c
int read_all_documents(FILE* fp, record_t * records);
void dump_records(FILE *stream, int record_num, record_t * records);




#endif /* HEADER_H_ */
