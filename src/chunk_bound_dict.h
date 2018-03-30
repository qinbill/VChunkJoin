#ifndef _CHUNK_BOUND_DICT_H_
#define _CHUNK_BOUND_DICT_H_


#define MAX_CBD_DICT_STRING_NUM 1000000
#define MAX_CBD_STRING_LEN 1024*10


#define MAX_TRIE_CHILDREN 128
#define MAX_CBDS_LEN 1024

typedef struct _trie_t{
  int value;
  struct _trie_t *children[MAX_TRIE_CHILDREN];
} trie_t;


typedef struct _chunk_bound_dict_t{
  int set_one_map[256];
  int set_two_map[256];
  char *cbd_strings[MAX_CBD_DICT_STRING_NUM];
  int cbs_num;
  trie_t *trie;
}chunk_bound_dict_t;










/*
   Read a cbd dict from a config file
*/
chunk_bound_dict_t * create_cbd_from_config_file(char *cbd_file_name);

/*
   Create a new cbd dict from set one and set two strings
*/
chunk_bound_dict_t * create_new_cbd(char *set_one, char *set_two);

/*
   destory a cbd dict from memory 
*/
int destory_cbd(chunk_bound_dict_t *bound_dict);

/*
   Save a cbd dict to a file from memory
*/
int save_cbd_config_file(chunk_bound_dict_t *bound_dict, char *cbd_file_name);

/*
   Add a cbd string into the cbd dict
*/
int cbd_dict_add_cbd_string(chunk_bound_dict_t * bound_dict, char *bound_string);

/*
   Try to match the string out of the cbd dict
*/
int cbd_dict_match_string(chunk_bound_dict_t * bound_dict, char *string, int len);



#endif
