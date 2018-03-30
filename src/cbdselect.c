/* 
   This version of cbd selection based on the previous
   version of cbd selection. we changed this cbd select
   algorithm from one pass to two pass. 
   
   First pass:
   The first pass is gather information of the 
   dataset. To gather nessesary information for
   the second pass. So in this version, we need 
   to store the dataset in the memory and also 
   index the candidate cbds. There is some infor-
   mation need to gather. 1) DF of a rule. 
   2) How many string underflow before this rule and
   how many string already have enough chunk. 
   3) transfer dataset into another data structure
   which could make access from string to ccbd much
   faster. 
         
   TODO: The ccbd contain is very common. So when 
   we gather information around we could choose some
   useful information to do such thing. 
         
   Second pass:
   We do the actual cbd selection in this pass. 
   We process from the shortest string to longest
   string. Our cost function in this senearo is
   based on how many strings would be affected by
   add one chunk into the dataset. 

   So there are some information that needed to be 
   gathered during the first pass. 
*/


#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <ctype.h>
#include <sys/time.h>
#include "header.h"
#include "hash_dict.h"
#include "chunk_bound_dict.h"


#define SOLID_CUT 1
#define CANDI_CUT 0

#define MAX_CUTS_NUM 32*1024*1024
#define MAX_RCDS_NUM 1024*1024

typedef struct _cut_t{
  char *str;
  int len;
  int selected;
  int pstv;
  int *rcds;
  int rcdsn;
}cut_t;

hash_dict_t *cdict;

typedef struct _rcd_t{
  //char *str;
  //int len;
  int currck;
  int *cuts;
  int *clen;
  int *cklen;
  int *pfx;
  int cutsn;
}rcd_t;

cut_t cuts[MAX_CUTS_NUM];
rcd_t rcds[MAX_RCDS_NUM];
int rcdsn = 0;
int cutsn = 1;
int tau = 0;
int prefix_len = 0;
int prefix_len_bound = 0;


void print_usage(){
  fprintf( stderr, "Usage: Program <-s 'character set one'> \n" );
  fprintf( stderr, "               <-u 'character set two'> \n" );
  fprintf( stderr, "               <-m 'last prefix length min bound'> \n" );
  fprintf( stderr, "               <-d dump final split of string records \n" );
  fprintf( stderr, "               <-t edit distance> \n" );
  fprintf( stderr, "               <-i cbd in file name> \n" );
  fprintf( stderr, "               <-o cbd out put file name > \n" );
  fprintf( stderr, "               <-h help > \n" );
  fprintf( stderr, "               <-v version > \n" );
  exit(-1);
}

void print_version(){
  fprintf( stderr, "Version 1.0.0.1 \n" );
  exit(-1);
}


#define ARRAY_POOL_SIZE 1024*1024
int *ap = NULL;
int aplen = 0;
int appos = 0;

int * getarray(int len){
  int *rt;
  if ( aplen - appos < len ){
    ap = (int *) malloc ( sizeof ( int ) * ARRAY_POOL_SIZE );
    aplen = ARRAY_POOL_SIZE;
    appos = 0;
  }
  rt = &ap[ appos ];
  appos += len;
  return rt;
}

#define STRING_POOL_SIZE 1024*1024*4
char *sp = NULL;
int splen = 0;
int sppos = 0;

char* getstring(int len){
  char *rt;
  len = (( len >> 2 ) + 1) << 2; /* Align */
  if ( splen - sppos < len ){
    sp = (char *) malloc ( sizeof ( char ) * STRING_POOL_SIZE );
    splen = STRING_POOL_SIZE;
    sppos = 0;
  }
  rt = &sp[ sppos ];
  sppos += len;
  return rt;
}

int split_by_cbd_dict ( char *str, chunk_bound_dict_t *bound_dict, 
                        int *spos, int *stype )
{
  int i;
  char *ps;
  int num = 0;
  int ret;
  int pl;
  ps = str;
  pl = 0;
  
  i = 0;
  while ( str[i] != '\0' ){
    if ( bound_dict->set_one_map[(int)str[i]] ){
      stype[num] = SOLID_CUT;
      spos[num] = i;
      num ++;
      pl = i + 1;
    }else if ( bound_dict->set_two_map[(int)str[i]] ){
      ret = cbd_dict_match_string( bound_dict, str + pl, i - pl + 1);
      if ( ret == 1 )
        stype[num] = SOLID_CUT;
      else
        stype[num] = CANDI_CUT;      
      spos[num] = i;
      num ++;
      pl = i + 1;
    }
    i++;
  }
  if ( spos[num - 1] != i - 1 ){
    stype[num] = SOLID_CUT;
    spos[num] = i-1;
    num ++;
  }else{
    stype[num] = SOLID_CUT;
  }
  return num;
}

int compare_int (const void *a, const void *b)
{
  return (*((int*)a) < *((int*)b)) - (*((int*)a) > *((int*)b));
}

#ifndef CUTS_BY_RATE
int compare_cuts (const void *a, const void *b)
{
  int da  = *((const int *) a);
  int db  = *((const int *) b);
  return (cuts[da].pstv > cuts[db].pstv) - (cuts[da].pstv < cuts[db].pstv);
}

#else

int compare_cuts (const void *a, const void *b)
{
  int da  = *((const int *) a);
  int db  = *((const int *) b);
  double ra = cuts[da].pstv / cuts[da].rcdsn;
  double rb = cuts[db].pstv / cuts[db].rcdsn;
  
  if ( ra < rb )
    return 1;
  else if ( ra > rb )
    return -1;
  else
    return (cuts[da].pstv > cuts[db].pstv) - (cuts[da].pstv < cuts[db].pstv);
}
#endif


int update_preifxset(int rid, int alpha){
  int i;
  int cklen[RECORD_MAX_LINE_LEN];
  int *ckcuts = rcds[rid].cuts;
  int *ckclen = rcds[rid].clen;
  int *ckpfx  = rcds[rid].pfx;
  int ckcutsn = rcds[rid].cutsn;
  int ckn = 0;

  /* initialize */
  cklen[0] = 0;
  
  /* First we calculate the original alpha length */
  for ( i = 0; i < ckcutsn; i ++ ){
    cklen[ckn] += ckclen[i];
    if (ckcuts[i] == 0){
      cklen[++ckn] = 0;
    }else if (cuts[abs(ckcuts[i])].selected){
      /* This cuts has already been selected. so */
      /* Ne need to put it as a permanant cuts   */
      ckcuts[i] = 0;
      cklen[++ckn] = 0;
    }
  }
  qsort( cklen, ckn, sizeof(int), compare_int ); 
  /* Store alpha length k len into the set. */
  /* Need to allocate array and do data cp  */
  for ( i = 0; i <= alpha; i ++ ){
    /* copy the prefix length array */
    if ( i < ckn ){
      ckpfx[i] = cklen[i];
    }else{
      ckpfx[i] = 0;
    }
  }
  return ckn;
}


int evaluate_cuts(int rid, int alpha){
  int i;
  int *ckcuts = rcds[rid].cuts;
  int *ckclen = rcds[rid].clen;
  int *ckpfx  = rcds[rid].pfx;
  int ckcutsn = rcds[rid].cutsn;
  int left = -1;
  int right = 0;
  int allen = 0; 
  int leftlen = 0; 
  int rightlen = 0;

  /* We need to calcuate the prefix set */
  update_preifxset( rid, alpha );

  /* Find a rigion bound by left and right */
  while (right < ckcutsn){
    if (ckcuts[right] != 0){
      right ++;
    }else{
      if ( right - left > 1 ){
        /* We finded a region which have some candidate cut. */
        /* Then we might need to the evaluation              */

        allen = leftlen = rightlen = 0; /* init */
        for ( i = left + 1; i <= right; i ++ )
          allen += ckclen[i]; /* calcuate total len */
        for ( i = left + 1; i < right; i ++ ){
          /* index it */
          leftlen += ckclen[i];
          /* Then we decide whether leftlen and right len is both larger  */
          rightlen = allen - leftlen;
          if ( allen < ckpfx[alpha-1] ){
            /* Now we find a case which is not affacted  */
            /* Need to mark this chunk as not affected   */
            if ( ckcuts[i] < 0 ){
              ckcuts[i] = - ckcuts[i];
              cuts[ckcuts[i]].pstv ++;
            }
          }else if ( rightlen >= ckpfx[alpha-1] && leftlen >= ckpfx[alpha-1] ){
            /* Now we find a good case, then mark this cut as good case */
            if ( ckcuts[i] < 0 ){
              ckcuts[i] = - ckcuts[i];
              cuts[ckcuts[i]].pstv ++;
            }
          }else{
            if ( ckcuts[i] > 0 ){
              ckcuts[i] = - ckcuts[i];
              cuts[-ckcuts[i]].pstv --;
            }
          }
        }
      }
      left = right; 
      right ++;
    }
  }
  return 0;
}


/* Do the add operation on the string */
inline void add_cut_to_cbd(chunk_bound_dict_t *cbd, 
                           int cid, int alpha){
  int last = -1;
  int i;

  cuts[cid].selected = 1;
  cbd_dict_add_cbd_string ( cbd, cuts[cid].str );
 
  /* we better put containd cuts into selected too */
  /* update all the string's cuts */
  for ( i = 0; i < cuts[cid].rcdsn; i ++)
    if ( cuts[cid].rcds[i] != last ){
      last = cuts[cid].rcds[i];
      evaluate_cuts( last, alpha );
    }
  return;
}

/* Do the selection process  */
int selection(chunk_bound_dict_t *cbd, int alpha){
  int i, j;
  int cutid;
  int *cutso;
  int round = 0;
  int cutson;
  
  for ( i = 0; i < rcdsn; i ++ ){
    for ( j = 0; j < rcds[i].cutsn; j ++ ){
      cutid = rcds[i].cuts[j];
      if (cutid != 0){
        if ( cuts[cutid].rcds == NULL ){
          cuts[cutid].rcds = getarray(cuts[cutid].rcdsn);
          cuts[cutid].pstv = cuts[cutid].rcdsn;
          cuts[cutid].rcdsn = 0;  
        } 
        cuts[cutid].rcds[cuts[cutid].rcdsn++] = i;        
      }
    }
    evaluate_cuts (i, alpha);
  }

  cutso = (int *) malloc ( sizeof (int) * cutsn );

#ifdef DEBUG
  for ( i = 1; i < cutsn; i ++ ){
    fprintf(stderr, "str: %s selected: %d pstv: %d rcdsn %d\n", 
            cuts[i].str, cuts[i].selected, cuts[i].pstv, cuts[i].rcdsn);
  }
#endif

  /* Running until there is no improvement availabe */
  while (1){
    cutson = 0;
    for ( i = 0; i < cutsn; i ++ ){
      if ( cuts[i].selected == 0 &&
           cuts[i].rcdsn < (cuts[i].pstv << 1) ){
        cutso[cutson++] = i;
      }
    }
    /* Order them by the benifit */
    /* Select until there is no availabe */
    round ++;

#ifdef DEBUG    
    fprintf(stderr, " Round %d \n", round);
#endif

    if ( cutson < 1 )
      break;
    
    /* Quick sort it */
    qsort( cutso, cutson, sizeof(int), compare_cuts );

    for ( i = 0; i < cutson; i ++ ){          
      if ( cuts[cutso[i]].rcdsn < ( cuts[cutso[i]].pstv << 1 )){

#ifdef DEBUG        
        fprintf(stderr, "str: %s selected: %d pstv: %d rcdsn %d\n", 
                cuts[cutso[i]].str, cuts[cutso[i]].selected, 
                cuts[cutso[i]].pstv, cuts[cutso[i]].rcdsn);
#endif        
              
        add_cut_to_cbd(cbd, cutso[i], alpha);
      }
    }
  }
  
  return 0;
}

inline int find_cut_id(char *key, int len){
  hash_dict_node_t snode;

  create_sign_bitwise_len (key, &snode.sign1, &snode.sign2, len);
  if (hash_dict_search(cdict, &snode) == RT_HASH_DICT_SEARCH_SUCC){
    return snode.code;
  }

  cuts[cutsn].str = getstring( len+1 );
  strncpy(cuts[cutsn].str, key, len);
  cuts[cutsn].str[len]='\0';
  snode.code = cutsn;  
  hash_dict_add(cdict, &snode, 0);
  cuts[cutsn].len = len;
  cuts[cutsn].rcds = NULL;
  cuts[cutsn].rcdsn = 0;
  return cutsn++;
}

void parse_records(chunk_bound_dict_t *bound_dict){
  char rcd_buffer[RECORD_MAX_LINE_LEN];
  char *cutstr;
  int spos[RECORD_MAX_LINE_LEN];
  int stype[RECORD_MAX_LINE_LEN];
  int i = 0, len, slen;
  int cutid, cutlen;
  
  while ( fgets(rcd_buffer, RECORD_MAX_LINE_LEN, stdin) != NULL ) {

#ifdef DEBUG    
    if ( rcdsn % 1000 == 0 ){
      fprintf( stderr, "INFO: Parse record number: %d\n", rcdsn );
    }
#endif
    
    len = strlen(rcd_buffer);
    while ( len > 0 && ( rcd_buffer[len-1] == '\n' || 
                         rcd_buffer[len-1] == '\r' ) ) 
      rcd_buffer[--len] = '\0';
    
    // rcds[rcdsn].str = getstring(len+1);
    // Todo  check the malloc 
    
    //strncpy ( rcds[rcdsn].str, rcd_buffer, len );
    //rcds[rcdsn].str[len] = '\0';
    //rcds[rcdsn].len = len;
    
    slen = split_by_cbd_dict ( rcd_buffer, bound_dict, spos, stype );
    rcds[rcdsn].cuts = getarray(slen);
    rcds[rcdsn].clen = getarray(slen);
    rcds[rcdsn].pfx  = getarray(prefix_len + 1);
    rcds[rcdsn].cutsn = 0;

    for( i = 0; i < slen; i++ ){
      cutlen = i == 0 ? spos[i] + 1 : spos[i] - spos[i-1];
      if (stype[i] == CANDI_CUT){
        if ( i == 0 ){
          cutstr = rcd_buffer;
        } else {
          cutstr = rcd_buffer + spos [i-1] + 1;
        }
        //fprintf(stderr, "%s %d\n", cutstr, cutlen);
        cutid = find_cut_id(cutstr, cutlen);
        rcds[rcdsn].cuts[i] = cutid;
        cuts[cutid].rcdsn ++;
      }else{
        rcds[rcdsn].cuts[i] = 0;
      }
      rcds[rcdsn].clen[i] = cutlen;
    }
    rcds[rcdsn].cutsn = slen;
    rcdsn ++;
  }
  return;
}

int main(int argc, char **argv) {
  
  char *set_one = NULL, *set_two= NULL;
  char *cbd_in_file_name= NULL; 
  char *cbd_out_file_name = NULL;
  int dump_flag = 0;
  char c;
  chunk_bound_dict_t *bound_dict = NULL;
  struct timeval start_run;
  
  while ((c = getopt(argc, argv, "hvds:u:t:i:o:m:p:")) != -1)
    switch (c) {
      case 's':
        set_one = optarg;
        break;
      case 'u':
        set_two = optarg;
        break;
      case 'd':
        dump_flag = 1;
      case 't':
        tau = atoi(optarg);
        break;
      case 'p':
        prefix_len = atoi(optarg);
      case 'm':
        prefix_len_bound = atoi(optarg);
      case 'i':
        cbd_in_file_name = optarg;
        break;
      case 'o':
        cbd_out_file_name = optarg;
        break;
      case 'h':
        print_usage();
        break;
      case 'v':
        print_version();
        break;
      case '?':
        if (optopt == 's' || optopt == 't'  || optopt == 'u' || optopt == 'i' || optopt == 'o')
          fprintf(stderr, "Error: Option -%c requires an argument.\n", optopt);
        else if (isprint(optopt))
          fprintf(stderr, "Error: Unknown option `-%c'.\n", optopt);
        else
          fprintf(stderr, "Error: Unknown option character `\\x%x'.\n",
                  optopt);
        return 1;
      default:
        print_usage();
    }

  if ( tau <= 0 ){
    fprintf ( stderr, "Error, tau need to be larger than 0\n" );
    exit (-1);
  }
    
  if ( prefix_len == 0 )
    prefix_len = 2 * tau +1;  
  
  if ( set_one != NULL && set_two != NULL ){
    bound_dict = create_new_cbd( set_one, set_two );
    if ( bound_dict == NULL ){
      fprintf (stderr, "Error: create bound_dict error\n");
      exit(-1);
    }
  }else if ( cbd_in_file_name != NULL ){
    bound_dict = create_cbd_from_config_file( cbd_in_file_name );
    if ( bound_dict == NULL ){
      fprintf (stderr, "Error: create bound_dict error\n");
      exit(-1);
    }
  }else{
    print_usage();
  }

  cdict = hash_dict_create(1000003);
  gettimeofday(&start_run,NULL);

  // test_cbd_dict(bound_dict);
  parse_records (bound_dict);
  selection(bound_dict, prefix_len);
  if ( cbd_out_file_name != NULL && bound_dict != NULL ){
    save_cbd_config_file( bound_dict, cbd_out_file_name );
  }
  
  /* The second parse is to do the greedy cbd selection */  
  destory_cbd(bound_dict);
  exit(0);
}
