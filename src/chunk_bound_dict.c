/*
  This is the main part of cbd selection 
*/


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include "hash_dict.h"
#include "chunk_bound_dict.h"



chunk_bound_dict_t *create_cbd_from_config_file(char *cbd_file_name)
{
  FILE *fp;
  int len;
  int i;
  chunk_bound_dict_t *bound_dict;
  char buffer[MAX_CBD_STRING_LEN];
  
  if((fp=fopen(cbd_file_name, "r"))==NULL){
    fprintf(stderr, "Error, File %s can not been open\n", cbd_file_name );
    return NULL;
  }

  bound_dict = (chunk_bound_dict_t *) malloc( sizeof(chunk_bound_dict_t) );
  if ( bound_dict == NULL ){
    fprintf(stderr, "Error, Out of memory when create bound_dict\n" );
    return NULL;
  }
  
  bound_dict->trie = (trie_t *) malloc(sizeof(trie_t));
  memset ( bound_dict->trie, 0, sizeof(trie_t));

  memset ( bound_dict->set_one_map, 0, sizeof(int)*256 );  
  memset ( bound_dict->set_two_map, 0, sizeof(int)*256 );
  
  // Process set one.
  fgets(buffer, MAX_CBD_STRING_LEN, fp);
  len = strlen(buffer);
  for(i=0;i<len;i++) if(buffer[i]!='\n' && buffer[i]!='\r') bound_dict->set_one_map[(int)buffer[i]] = 1;

  // Process set two.
  fgets(buffer, MAX_CBD_STRING_LEN, fp);
  len = strlen(buffer);
  for(i=0;i<len;i++) if(buffer[i]!='\n' && buffer[i]!='\r') bound_dict->set_two_map[(int)buffer[i]] = 1;

  // Process boundary strings
  while ( fgets(buffer, MAX_CBD_STRING_LEN, fp) != NULL ){
    len = strlen(buffer);
    
    // delete the new line characters 
    while ( len > 0  && ( buffer[len-1] == '\n' || buffer[len-1] == '\r')) buffer[--len]='\0';
    if ( len == 0 ) continue;
    
    // add this string into the dict
    cbd_dict_add_cbd_string( bound_dict, buffer );
  } 
  fclose(fp); 
  return bound_dict;
}


chunk_bound_dict_t * create_new_cbd(char *set_one, char *set_two)
{
  int len;
  int i;
  chunk_bound_dict_t *bound_dict;
  char *buffer;
  
  bound_dict = (chunk_bound_dict_t *) malloc( sizeof(chunk_bound_dict_t) );
  if ( bound_dict == NULL ){
    fprintf(stderr, "Error, Out of memory when create bound_dict\n" );
    return NULL;
  }

  bound_dict->trie = (trie_t *) malloc(sizeof(trie_t));
  memset ( bound_dict->trie, 0, sizeof(trie_t));
  
  for(i=0;i<256;i++){
    bound_dict->set_one_map[i] = 0;
    bound_dict->set_two_map[i] = 0;
  }
  
  bound_dict->cbs_num = 0;
  // Process set one.
  buffer = set_one;
  len = strlen(buffer);
  for(i=0;i<len;i++) if(buffer[i]!='\n' && buffer[i]!='\r') bound_dict->set_one_map[(int)buffer[i]] = 1;

  // Process set two.
  buffer = set_two;
  len = strlen(buffer);
  for(i=0;i<len;i++) if(buffer[i]!='\n' && buffer[i]!='\r') bound_dict->set_two_map[(int)buffer[i]] = 1;

  return bound_dict;
}




static void _traval_trie_and_output(int level, char *str, trie_t *tp, FILE *fp)
{
  int i;
  for ( i = 0; i < MAX_TRIE_CHILDREN; i++ ){
    if ( tp->children[i]!=NULL ){
      str[level]=i;
      if (tp->children[i]->value==1){
	fprintf(fp, "%s\n", str + level );
      }else{
	_traval_trie_and_output( level-1, str, tp->children[i], fp);
      }
    }
  }
}



static void _destory_trie(trie_t * tp)
{
  int i = 0; 
  
  for ( i = 0 ;i< MAX_TRIE_CHILDREN; i ++ ){
    if ( tp->children[i] != NULL )
      _destory_trie(tp->children[i]);
  }
  free ( tp );
}


int destory_cbd(chunk_bound_dict_t * bound_dict)
{
  int i;
  for ( i = 0; i< bound_dict->cbs_num; i++ ){
    if (bound_dict->cbd_strings[i]!=NULL){
      free (bound_dict->cbd_strings[i]);
    }
  }
  _destory_trie (bound_dict->trie);
  free (bound_dict);
  return 0;
}



int save_cbd_config_file(chunk_bound_dict_t *bound_dict, char *cbd_file_name)
{
  FILE *fp;
  int i;
  char buffer[MAX_CBDS_LEN+2];
  
  if((fp=fopen(cbd_file_name, "w"))==NULL){
    fprintf(stderr, "Error, File %s can not been open\n", cbd_file_name );
    return -1;
  }
  
  for(i=0;i<128;i++) {if ( bound_dict->set_one_map[i] ) fprintf(fp, "%c", i);}
  fprintf(fp, "\n");
  for(i=0;i<128;i++) {if ( bound_dict->set_two_map[i] ) fprintf(fp, "%c", i);}
  fprintf(fp, "\n");

  buffer[MAX_CBDS_LEN] = '\0';
  _traval_trie_and_output ( MAX_CBDS_LEN - 1, buffer, bound_dict->trie, fp );
  
  fclose(fp);
  return 0;
}



int cbd_dict_add_cbd_string(chunk_bound_dict_t * bound_dict, char *str)
{
  int len;
  int i;
  trie_t *trie, *tp = NULL;


  len = strlen(str);  
  if ( len > MAX_CBDS_LEN ){
    fprintf( stderr, "Error: cbd string '%s' exceed max cbs length aboundand\n", str );
    return -1;  
  }
  
  trie = bound_dict->trie;
  i = len-1;

  while ( i >= 0 ){
    if ( trie->children[(int)str[i]] == NULL ){
      tp = (trie_t *)malloc(sizeof(trie_t));
      memset(tp,0,sizeof(trie_t));
      trie->children[(int)str[i]] = tp;
    }
    trie = trie->children[(int)str[i]];
    if ( trie->value == 1 )
      break;
    i --;
  }
  
  if ( trie->value != 1 ){
    trie->value = 1;
    for ( i = 0; i < MAX_TRIE_CHILDREN; i++ ){
      if ( trie->children[i] != NULL ){
	_destory_trie(trie->children[i]);
	trie->children[i] = NULL;
      }
    }
  }
  return 0;
} 


int cbd_dict_match_string(chunk_bound_dict_t * bound_dict, char *str, int len)
{
  int i;
  trie_t *trie;

  trie = bound_dict->trie;
  i = len - 1;
  while ( i >= 0 ){
    if ( trie->children[(int)str[i]] == NULL ){
      return 0;
    }else if (trie->children[(int)str[i]]->value == 1)
      return 1;
    trie = trie->children[(int)str[i]];
    i--;
  }
  return 0;
}


