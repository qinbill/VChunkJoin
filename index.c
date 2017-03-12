/*
 * index.c
 *
 *  Created on: Apr 22, 2009
 *      Author: jianbinqin
 */

#include <stdio.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "header.h"
#include "hash_dict.h"



#ifdef MAKE_CHUNKS_AS_CHAIN
chunk_t *g_chunks_head = NULL, *g_chunks_last = NULL;
#endif
int g_chunk_num=0;

hash_dict_t *_index_hash_dict = NULL;
chunk_block_t *_chunk_block_head = NULL;
chunk_block_t *_curr_chunk_block = NULL;

/*
 * Init the data of the inverted index and chunk.
 */
void init_index(int size)
{
  if(_index_hash_dict != NULL){
    hash_dict_destory(_index_hash_dict);
  }
  _index_hash_dict = hash_dict_create(size * 1.5);
  _chunk_block_head = ( chunk_block_t *)malloc(sizeof(chunk_block_t));
  if ( _chunk_block_head == NULL ){
    fprintf  ( stderr, "MALLOC MEMEORY ERROR\n" );
    exit(-1);
  }
  _chunk_block_head->last_free = 0;
  _chunk_block_head->next = NULL;
  _curr_chunk_block = _chunk_block_head;

  g_chunk_num=0;
}


/*
 * get a chunk from a block date s  
 */
chunk_t * new_chunk()
{
  chunk_block_t *cbtp;

  if ( _curr_chunk_block->last_free < CHUNK_BLOCK_CHUNK_NUM ){
    return &_curr_chunk_block->chunks[_curr_chunk_block->last_free++];
  }else{
    cbtp = ( chunk_block_t *)malloc(sizeof(chunk_block_t));            
    if ( cbtp == NULL ){                                               
      fprintf  ( stderr, "MALLOC MEMEORY ERROR\n" );                                
      return NULL;
    }             
    cbtp->last_free = 0;
    cbtp->next = NULL;
    _curr_chunk_block->next = cbtp;
    _curr_chunk_block = cbtp;
    return &_curr_chunk_block->chunks[_curr_chunk_block->last_free++];
  }
}



chunk_t* insert_chunk_node(char *str, int len, int record_id);


/*
  This function use the record string to build a list chunks.
  and those chunks is in the data set.
 */
int build_record_chunks(char *str, int* chunk_idx,
		     int chunk_len, chunk_slot_t *chunk_slots)
{

  int i;
  int len,s,t;


  for(i=0;i < chunk_len;i++){
    t = chunk_idx[i+1];
    s = chunk_idx[i];
    len = t - s;
    chunk_slots[i].pos = s;
    chunk_slots[i].order = i;
    chunk_slots[i].len = len;
    chunk_slots[i].chunk = insert_chunk_node(str + s, len, -1);    
  }
  return 0;
}



chunk_t* insert_chunk_node(char *str, int len, int record_id)
{

  hash_dict_node_t snode;
  int res;
  chunk_t *chunk_p;
  char chunk_buffer[1024*10];
  // idf_node_t *node_p;
  strncpy(chunk_buffer, str, len);
  chunk_buffer[len] = '\0';

  //  if(create_sign_md5(str, &snode.sign1, &snode.sign2)!=0){
  if(create_sign_md5(chunk_buffer, &snode.sign1, &snode.sign2)!=0){
    fprintf(stderr, "Error: Error in create sign of %s\n", str);
    return NULL;
  }
  
  if ( hash_dict_search(_index_hash_dict, &snode) == RT_HASH_DICT_SEARCH_SUCC ){
    chunk_p =(chunk_t *)snode.pointer;
    chunk_p->frq++;
  }else{
    if ((chunk_p=new_chunk())==NULL){
      fprintf(stderr,"ERROR: Out of memory \n");
      exit(-1);
    }

    chunk_p->sign = ((unsigned long long)snode.sign1 << 32) + (unsigned long long)snode.sign2;
    chunk_p->frq=1;
    chunk_p->idf_list=NULL;
    chunk_p->head_idf=0;
    chunk_p->last_idf=0;
    chunk_p->pfx_num=0;
    
#ifdef DEBUG
    chunk_p->str = str;
    chunk_p->len = len;
#endif

    g_chunk_num++;    
    // insert it into hash_dict
    snode.pointer=(void *)chunk_p;
    res = hash_dict_add(_index_hash_dict, &snode, 0);
    if(res != RT_HASH_DICT_ADD_SUCC && res != RT_HASH_DICT_ADD_EXIST){
      fprintf(stderr, "Error, insert a node into hash dict error \n");
      return NULL;
    }
  }
  

  return chunk_p;
}




void dump_chunks(){
  int i; 
  chunk_block_t *chunk_block_p;
  chunk_t *chunk_p;

  fprintf(stderr, "----------- Dump Chunks Start ----------------\n");
  chunk_block_p = _chunk_block_head;
  while (chunk_block_p){
    for ( i = 0; i< chunk_block_p->last_free; i++ ){
      chunk_p = & chunk_block_p->chunks[i];
      /* New output information we needed */
#ifdef DEBUG
      fprintf(stderr, "CHUNK CAND: %d PFX_NUM: %d DF: %d STR %s\n",
              chunk_p->candn, chunk_p->pfx_num, chunk_p->frq, chunk_p->str);
#endif
    }
    chunk_block_p = chunk_block_p->next;
  }
  fprintf(stderr, "----------- Dump Chunks END ----------------\n");
}


int destroy_index()
{
  int i; 
  chunk_block_t *chunk_block_p, *tmp_p;  

  chunk_block_p = _chunk_block_head;
  while (chunk_block_p){
    for ( i = 0; i< chunk_block_p->last_free; i++ ){
      
#ifdef CHUNK_STRING_PRESERVED
      free ( chunk_block_p->chunks[i].str );
#endif
      
      if( chunk_block_p->chunks[i].idf_list != NULL ){
	free ( chunk_block_p->chunks[i].idf_list );
      }
    }
    tmp_p = chunk_block_p->next;
    free (chunk_block_p);
    chunk_block_p = tmp_p;
  }
 
  if(_index_hash_dict != NULL)
    hash_dict_destory(_index_hash_dict);
  return 0;
}

