
/*
 * bound.c
 *  This module is doing bound chars map and 
 *  some string split opertaion.
 *  Created on: Apr 22, 2009
 *      Author: jianbinqin
 */

#include <stdio.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "header.h"
#include "chunk_bound_dict.h"


typedef int (v_bound_map_t)[256];
typedef int (bound_map_t)[256][256];

v_bound_map_t v_bound_map;
chunk_bound_dict_t *bound_dict;



#ifdef SIMPLE_VCN
char vcn_chars[]="eai";

void init_vcn_map(int seed){
  int len;
  int i;
  int slots[VIRTUAL_BOUND_LEN];
  int avg;
  int ran;
  len = strlen(vcn_chars);
  if ( len % VIRTUAL_BOUND_LEN == 0 )
    avg = len / VIRTUAL_BOUND_LEN;
  else
    avg = len / VIRTUAL_BOUND_LEN + 1;

  // clean up;                                                                                                                                                  
  for ( i = 0; i < 256; i++ ) v_bound_map[i] = -1;
  for ( i = 0; i < VIRTUAL_BOUND_LEN; i++ ) slots[i] = 0;

  srand(seed);

  i = 0;
  while ( i < len ){
    do{
      ran =  rand() % VIRTUAL_BOUND_LEN;
    }while ( slots[ran] >= avg);
    slots[ran] ++;
    v_bound_map [ (int) vcn_chars[i] ] = ran;
    i ++;
  }
}

#elif defined FULL_VCN

char vcn_chars[]="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-,.' ;:\'\"?!";

void init_vcn_map(int seed){
  int len;
  int i; 
  int slots[VIRTUAL_BOUND_LEN];
  int avg;
  int ran;
  //  len = strlen(vcn_chars);
  len = 128 ;
  if ( (len-32)  % VIRTUAL_BOUND_LEN == 0 )
    avg = (len-32) / VIRTUAL_BOUND_LEN;
  else
    avg = (len-32) / VIRTUAL_BOUND_LEN + 1;
  
  // clean up;
  for ( i = 0; i < 256; i++ ) v_bound_map[i] = -1;
  for ( i = 0; i < VIRTUAL_BOUND_LEN; i++ ) slots[i] = 0;

  srand(seed);

  i = 32; 
  while ( i < len ){
    do{
      ran =  rand() % VIRTUAL_BOUND_LEN; 
    }while ( slots[ran] >= avg);
    slots[ran] ++;
    v_bound_map [ i ] = ran;
    i ++;
  }
}

#elif defined WITHOUT_VCN

void init_vcn_map(int seed){
  int i;
  for ( i = 0; i < 256; i++ ) v_bound_map[i] = -1;
}

#endif


/*
 * This function is build a bound map 
 * which is used to split string.
 */
int init_bound_map(char *bound_dict_file, int seed)
{
  
  bound_dict=create_cbd_from_config_file( bound_dict_file );
  if ( bound_dict == NULL ){
    fprintf ( stderr, "Error: bound_dict init err at init_bound_map %s\n", bound_dict_file );
    return -1;
  } 
  init_vcn_map(seed);

  return 0;
}


/*
 * This function split a string by bound chars. 
 * The bound chars need to be mapping to a array first. 
 * Call this function need to call init_bound_map first.
 */
int split_string_by_bound(char *str, int str_len, int *idx, int *vcn)
{
  int count = 0;
  char *p;
  int i, pl;
  for(i=0;i<VIRTUAL_BOUND_LEN;i++)
    vcn[i] = 0;

  p=str;
  i = 0;
  pl = 0;
  idx[count++]=0;  // init the chunk index point.
  while(p[i] != '\0'){
    if(v_bound_map[(int)p[i]]!=-1){
      vcn[v_bound_map[(int)p[i]]]++;
    }
    if ( bound_dict->set_one_map[(int)str[i]] ){
      idx[count++] = i + 1;
      pl = i + 1;
    }else if ( bound_dict->set_two_map[(int)str[i]] ){
      if ( cbd_dict_match_string (bound_dict, str + pl, i - pl + 1) ){
	idx[count++] = i + 1;
      }
      pl = i + 1;
    }
    i ++;
  }
  if (idx[count - 1] == str_len ){
    count --;
  }else{
    idx[count]=str_len;
  }
  return count;
}



int destroy_bound()
{
  return 0;
}






