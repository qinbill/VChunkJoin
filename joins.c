/*
 * joins.c
 *
 *  Created on: Apr 22, 2009
 *      Author: jianbinqin
 */

#include <stdio.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <math.h>
#include "header.h"



#ifdef GOOGLE_MAP

#include <google/dense_hash_map>
struct Counter
{
 Counter() : count(0){}
  void increment()
  {
    ++count;
  }
  void increment(int inc)
  {
    count += inc;
  }
  int count;
};

typedef google::dense_hash_map<int, Counter> hashmap_t;
typedef google::dense_hash_map<int, Counter>::iterator hashmap_iterator_t;
hashmap_t candidates;

#endif

extern unsigned int cand_num;
extern long long int total_cand_num;
extern long long int total_cand_one;
extern long long int total_underflow_candi;
extern long long int total_underflow_cand_one;

extern unsigned int underflow_records;

extern int result_num;

extern long long int total_index_entry;
extern long long int total_cand_zero;
extern long long int total_prefix_len;


extern int g_chunk_num;
extern char *g_bound;
extern int g_v_seed;
extern int g_dump_all_flag;
extern int g_dump_prefix_flag;
extern int g_print_underflow_flag;
extern int g_without_chunk_number_filter;
extern chunk_t *g_chunks_head;
extern chunk_t *g_chunks_last;
extern int g_cal_edit_distance_result_flag;
extern int g_join_underflows;



#ifdef DEBUG
extern long long int total_prefix_chunk_length;
#endif  


inline static int _min(int a, int b)
{
  return a>b? b:a;
}



inline static int _min3(int a, int b, int c)
{
  if (a <= b && a <= c) return a;
  if (b <= a && b <= c) return b;
  return c;
}

inline static int _edit_distance(char* x, int lx,  char* y, int ly, int D) {
  //!!FIXME - LEN_LIMIT
  //if (lx < LEN_LIMIT || ly < LEN_LIMIT) return D + 1;
  int i, j, ans; int* mat[2]; 
  int row, valid = 0;
  
  for (i = 0; i < 2; i++){
    mat[i] = (int *) malloc( sizeof (int) * (ly +1));;
  }
  for (i = 0; i <= D; i++) 
    mat[0][ly - i] = i;
  
  for (i = 1, row = 1; i <= lx; i++, row = !row) {
    valid = 0;
    if (i <= D) mat[row][ly] = i;
    for (j = (i - D >= 1 ? i - D : 1); j <= (i + D <= ly ? i + D : ly); j++) {
      if (x[lx - i] == y[ly - j])
        mat[row][ly - j] = mat[!row][ly - j + 1];
      else
        mat[row][ly - j] = _min3(j - 1 >= i - D ? mat[row][ly - j + 1] : D, mat[!row][ly - j + 1], j + 1 <= i + D ? mat[!row][ly - j] : D) + 1;
      if (mat[row][ly - j] <= D) valid = 1;
    }
    if (!valid) break;
  }
  ans = valid ? mat[!row][0] : D + 1;
  for (i = 0; i < 2; i++)
    free(mat[i]);
  return ans;
}




static int _compare_chunk_slots (const void *a, const void *b)
{
  const chunk_slot_t *sa = (const chunk_slot_t *)a;
  const chunk_slot_t *sb = (const chunk_slot_t *)b;
  
  const chunk_t *da = sa->chunk;
  const chunk_t *db = sb->chunk;
  int ret;
  
  ret = (da->frq > db->frq) - (da->frq < db->frq);
  if(ret == 0){
    ret = ( da->sign > db->sign ) - ( da->sign < db->sign );
    if(ret == 0){ // if they are the same chunk. then sort them by exist order.
      ret = ( sa->order > sb->order ) - ( sa->order < sb->order);
      
    }
  }
  
  return ret;
}



#ifdef DEBUG

struct _list_node_t{
  int slot_id;
  int pos;
};

static int _compare_chunk_list_node (const void *a, const void *b)
{
  const struct _list_node_t *da = (const struct _list_node_t *)a;
  const struct _list_node_t *db = (const struct _list_node_t *)b;
  return (da->pos > db->pos) - (da->pos < db->pos);
}


static void _output_candidate_records(record_t *record, int rid, int alpha, int pos)
{
  struct _list_node_t *list;
  int i;
  int sid;
  char buff[RECORD_MAX_LINE_LEN];
  int chunk_len = record->chunk_len;
  list =(struct _list_node_t*)  malloc(record->chunk_len * sizeof(struct _list_node_t));
  
  for(i=0;i<chunk_len;i++){
    list[i].slot_id=i;
    list[i].pos=record->chunk_slots[i].pos;
  }
  qsort(list,chunk_len, sizeof(struct _list_node_t), _compare_chunk_list_node);
  fprintf(stderr, "Rid: %8d | ", rid);

  for(i=0;i<chunk_len;i++){
    sid =list[i].slot_id;
    strncpy( buff, record->chunk_slots[sid].chunk->str, record->chunk_slots[sid].chunk->len );
    buff[record->chunk_slots[sid].chunk->len] = '\0';
    
    if(sid < alpha){
      if(list[i].pos == pos){
        fprintf(stderr, "<[%s(%d)(%d)]> ", buff, record->chunk_slots[sid].chunk->frq, record->chunk_slots[sid].chunk->pfx_num);
      }else{
        fprintf(stderr, "[%s(%d)(%d)] ", buff, record->chunk_slots[sid].chunk->frq, record->chunk_slots[sid].chunk->pfx_num);
      }
    }else{
      fprintf(stderr, "'%s(%d)(%d)' ", buff, record->chunk_slots[sid].chunk->frq, record->chunk_slots[sid].chunk->pfx_num);
    }
  }
  fprintf(stderr, "\n");
  free(list);  
}

#endif 


void sort_all_chunk_list_by_freq(record_t *records, int records_num)
{
  int i = 0;
  for(i = 0;i<records_num;i++){
    // process non-underflow records                                            
    qsort ( records[i].chunk_slots, records[i].chunk_len, 
            sizeof (chunk_slot_t), _compare_chunk_slots); 
  }
}


int location_based_mismatch_prefix_length(record_t *ra, int tau)
{
  int list[128*1024];
  int chunk_len = ra->chunk_len;
  int i,left_len, right_len;
  int ed = 0;
  int order;
  int pfx = -1;
  

#ifdef WITHOUT_LOCATION_FILTERING
  int alpha = ( 2 * tau ) + 1;
  if ( chunk_len >= alpha)   pfx = alpha;
#else

  for(i=0;i<chunk_len;i++)
    list[i] = 0;
  
  for(i=0;i<chunk_len;i++){
    order=ra->chunk_slots[i].order;
    left_len=0;    
    while( order - left_len - 1 >= 0 
           && list[ order - left_len - 1] )
      left_len ++;
    right_len = 0;
    while (order + right_len + 1 < chunk_len 
           && list[ order + right_len + 1] )
      right_len ++;
    if ( right_len % 2 == 0 && left_len % 2 == 0 )
      ed ++;
    list[order] = 1;
    if(ed > tau){
      pfx = i+1; // i start from 0
      break;
    }
  }
#endif
  
  if (pfx < 0){ // a underflow string. output it

#ifdef DEBUG
    fprintf(stderr, "UD: %s\n", ra->str);
#endif
    underflow_records ++;
    // if ( chunk_len >= tau + 1 ){
    // This is the trick part.      
    /*       for ( i = 0; i < chunk_len; i++ ){ */
    /*         ra->chunk_slots[i].chunk->pfx_num ++; */
    /* #ifdef DEBUG */
    /*         total_prefix_chunk_length += ra->chunk_slots[i].chunk->len; */
    /* #endif */
    /*       } */
    /*       ra->stat = 0; */
    /*       pfx = chunk_len; */
    /*     }else{ */
    /*       ra->stat = -1; */
    /*     } */
    
    
    if ( chunk_len >= tau + 1 ){
      // This is the trick part.      
      ra->stat = 0;
    }else{
      ra->stat = -1;
    }
    
    pfx = chunk_len;
    for ( i = 0; i < chunk_len; i++ ){
      ra->chunk_slots[i].chunk->pfx_num ++;
#ifdef DEBUG
      total_prefix_chunk_length += ra->chunk_slots[i].chunk->len;
#endif
    }
  }else{
    total_prefix_len += pfx;
    for ( i = 0 ;i < pfx ; i++ ){
      ra->chunk_slots[i].chunk->pfx_num ++;
#ifdef DEBUG
      total_prefix_chunk_length += ra->chunk_slots[i].chunk->len;
#endif
    }
    ra->stat = 1;
  }
  return pfx;
}

int *underflow_list= NULL;
int underflow_num = 0;


void calculate_all_prefix_length(record_t *records, int records_num, int tau)
{
  int i,j;
  chunk_t *chunk_p;
  underflow_records = 0;
    
  for(i = 0;i<records_num;i++){
    records[i].prefix_len = location_based_mismatch_prefix_length(&records[i], tau);
  }
    
  underflow_num = 0;
  if ( g_join_underflows == 1 && underflow_records > 0){
    if ((underflow_list = (int *) malloc(underflow_records * sizeof(int)))==NULL){
      fprintf(stderr, "Memory out\n");
      exit(-1);
    }
  }
    
  for (i = 0; i< records_num; i++ ){
#ifdef DEBUG
    records[i].candn = 0;
    records[i].resultn = 0;
#endif
    if (g_join_underflows && records[i].stat <= 0){
      underflow_list[underflow_num++] = i;
    }
      
    for ( j = 0; j < records[i].prefix_len; j ++ ){
      chunk_p= records[i].chunk_slots[j].chunk;
#ifdef DEBUG
      chunk_p->candn = 0;
#endif
      if ( chunk_p->idf_list == NULL ){
        if ( chunk_p->pfx_num < 2){
          continue;
        }else{
          total_index_entry+=chunk_p->pfx_num;
          if((chunk_p->idf_list=(idf_node_t *)malloc(sizeof(idf_node_t)*chunk_p->pfx_num))==NULL){
            fprintf(stderr, "Error: out of memory when allocat idf_list\n");
            exit(-1);
          }
          chunk_p->head_idf = 0;
          chunk_p->last_idf = 0;
        }
      }
      chunk_p->idf_list[chunk_p->last_idf].record_id = i;
      chunk_p->idf_list[chunk_p->last_idf].pos = records[i].chunk_slots[j].pos;
      chunk_p->idf_list[chunk_p->last_idf].order = records[i].chunk_slots[j].order;
      chunk_p->idf_list[chunk_p->last_idf].porder = j;
      chunk_p->last_idf++;
    }
  }
}
 
inline static void output_candidate( record_t * records, int ix, int iy, int under, int ed , int tau )
{
  if ( under ){
    fprintf(stderr, "CAND2 ");
  }
  if ( ed <= tau ) {
    fprintf ( stderr, "RST %d ", ed );
    
#ifdef DEBUG_RESULT
    if ( strcmp ( records[ix].str, records[iy].str ) < 0 )
      fprintf ( stdout, "%s %s\n", records[ix].str, records[iy].str);
    else
      fprintf ( stdout, "%s %s\n", records[iy].str, records[ix].str);
#endif
    
  }else{
    fprintf ( stderr, "CAN %d ", ed ); 
  }
  
  fprintf(stderr, "[%lld] <%d>[L:%d][T:%d]--<%d>[L:%d][T:%d]  {%s}---{%s}\n",
          total_cand_num, iy, records[iy].len, records[iy].chunk_len, ix,records[ix].len, 
          records[ix].chunk_len, records[iy].str, records[ix].str);
#ifdef DEBUG
  _output_candidate_records(&records[iy], iy, records[iy].prefix_len, -1);
  _output_candidate_records(&records[ix], ix, records[ix].prefix_len, -1);
  fprintf(stderr, "\n");
#endif
  
}


#ifndef GOOGLE_MAP

#define U_INT_LEN 32
#define U_INT_BIT_LEN 5

typedef struct _bit_map_t{
  unsigned int min;
  unsigned int max;
  unsigned int *bitmap;
  unsigned int bitmap_len;
} bit_map_t;


inline static void bit_map_init( bit_map_t *bm, int len , int llen )
{
  bm->min = 0xffffffff;
  bm->max = 0;
  bm->bitmap_len =  len / U_INT_LEN + 2;
  bm->bitmap = (unsigned int * ) malloc ( sizeof ( int ) * bm->bitmap_len );
  memset ( bm->bitmap, 0, sizeof(int) * bm->bitmap_len );
}

inline static void bit_map_clear ( bit_map_t *bm )
{
  unsigned j,m;
  if ( bm->min != 0xffffffff){
    j = bm->min;
    m = bm->max - bm->min + 1;
    memset (&bm->bitmap[j], 0, sizeof(int) * m );	
    bm->min = 0xffffffff;
    bm->max = 0;
  }
}

inline static int bit_map_set ( bit_map_t *bm, unsigned int y )
{
  unsigned j,m;
  j = y >> U_INT_BIT_LEN;
  m = y - ( j << U_INT_BIT_LEN );

  if ( bm->bitmap[j] & (1 << m) ) return 1;

  if ( j < bm->min ) bm->min = j;
  if ( j > bm->max ) bm->max = j;

  bm->bitmap[j] = bm->bitmap[j] | (1 << m) ;
  return 0;
}

inline static int bit_map_check( bit_map_t *bm, int y )
{
  int j,m;
  j = y >> U_INT_BIT_LEN;
  m = y - ( j << U_INT_BIT_LEN );
  if ( bm->bitmap[j] & (1 << m) ) return 1;
  return 0;
}

#endif


int ppjoin_ed(record_t * records,int records_num, int tau)
{
  int x,y,i,j,sc,ret,ed,rret;
  chunk_slot_t *chunk_slots;
  chunk_t *chunk_p;
  int x_prefix_len, y_prefix_len;
  int x_block_start, y_block_start;
  int x_block_start_dup;
  int x_block_end, y_block_end, y_probe_len;
  int min_len_bound;
  int ra_stat;

  // int underflow_num = 0;

#ifdef GOOGLE_MAP
  hashmap_iterator_t it;
  candidates.set_empty_key(0xffffffff);
#else
  bit_map_t bm;
  bit_map_init (&bm, records_num, 100 );
#endif
  
  // For every record do join.
  for(x=0;x<records_num;x++){

    min_len_bound = records[x].len - tau;    
	
    chunk_slots = records[x].chunk_slots;
    x_prefix_len = records[x].prefix_len;
    ra_stat = records[x].stat;
	
    //if(ra_stat >= 0) {

#ifdef GOOGLE_MAP
    candidates.clear_no_resize();
#else
    bit_map_clear(&bm);
#endif

    for( i = 0; i < x_prefix_len;  ){
      chunk_p = chunk_slots[i].chunk;
        
      /* look for a block with the same chunk in x */
      x_block_start = i;
      x_block_end = i + 1;
      while ( x_block_end < x_prefix_len && chunk_slots[x_block_end].chunk == chunk_slots[x_block_start].chunk ) ++x_block_end; 
      x_block_start_dup = x_block_start;
      i = x_block_end;
        
      if ( chunk_p->idf_list == NULL ) continue;		
        
      // move start ptr for a inverted list. 
      for ( j = chunk_p->head_idf; j < chunk_p->last_idf && records[chunk_p->idf_list[j].record_id].len < min_len_bound; j++);
	
      for ( chunk_p->head_idf = j; j < chunk_p->last_idf;  ){
        y = chunk_p->idf_list[j].record_id;
	  
        // reach the end of the list
        if ( y >= x ) 
          break;

        x_block_start = x_block_start_dup;
		  
        // Initilaze the y probe variables. 
        y_block_start = chunk_p->idf_list[j].porder;
        y_block_end = y_block_start + 1;
        y_prefix_len = records[y].prefix_len;

        // Probe find the same block in y.
        while ( y_block_end < y_prefix_len && records[y].chunk_slots[y_block_end].chunk == records[y].chunk_slots[y_block_start].chunk ) ++y_block_end;	  
        y_probe_len = y_block_end - y_block_start;
        // Move j to the next prob window.
        j += y_probe_len;

        // Check if it is two underflow string join, So then do not
        // need to be join here, they will be join in the next
        // round.
        if ( ra_stat <= 0 && records[y].stat <= 0 )
          continue;
		  
        // chunk number filtering
        if ( abs ( records[x].chunk_len - records[y].chunk_len ) > tau ) 
          continue;
	  
        //	  if ( abs ( (records[x].chunk_len - records[y].chunk_len) + (records[y].len - records[x].len) ) > tau )
        //  continue;
   
        // merge too window find their match point.
        for ( sc = 0; x_block_start < x_block_end && y_block_start < y_block_end; ){
          ret = chunk_slots[x_block_start].pos - records[y].chunk_slots[y_block_start].pos;
          rret = chunk_slots[x_block_start].order - records[y].chunk_slots[y_block_start].order;
          if( ret >= -tau && ret <= tau ){
            if ( rret >= -tau && rret  <= tau ){
              sc = 1;
              break;
            }
          }
          if ( ret < 0 ){
            x_block_start ++;
          }else if ( ret > 0 ){
            y_block_start ++;
          }else if ( rret < 0 ){
            x_block_start ++;
          }else{
            y_block_start ++;
          }
        }
	  
        // no match pair
        if (sc == 0) 
          continue;

#ifdef GOOGLE_MAP
        //	  total_cand_zero ++;
        it = candidates.find(y);
        if (it != candidates.end()) 
          continue;
        else 
          candidates[y].increment(sc); // new candidate  
#else
        if (bit_map_set(&bm,y)==1)
          continue;
#endif

#ifdef DEBUG
        records[x].candn ++;
        chunk_p->candn ++;
#endif

        total_cand_zero ++;
#ifndef WITHOUT_VCN
        // virtual chunk nunber filtering 
        if ( virtual_chunk_number_filtering(&records[x], &records[y], tau) != PASSED ) 
          continue;
#endif
        total_cand_one ++;

        if ( content_based_mismatch_filtering ( &records[x], &records[y], tau ) != PASSED ) continue;	
        total_cand_num ++;

        ed = _edit_distance ( records[y].str, records[y].len, records[x].str, records[x].len, tau);
	  
#ifdef DEBUG      
        if ( ed <= tau ){
          result_num ++;
          output_candidate ( records, x, y, 1, ed, tau );
        }else if ( !g_cal_edit_distance_result_flag ) {
          output_candidate ( records, x, y, 1, ed, tau );
        }
#else
        if ( ed <= tau ){
          result_num ++;
          fprintf ( stderr, "%d %d-%d\n", ed, x, y );
        }
#endif	  
      }
    }
  }
  
    

  // do work about underflow
  if ( g_join_underflows ){      
    // for every x in underflow list
    for (i = 0; i < underflow_num; i++) {
      x = underflow_list[i];
      // do a round robin from lower to higher
      for (j = i + 1; j < underflow_num; j ++) {
        y = underflow_list[j];

        if (records[y].len - records[x].len > tau){
          break;
        }
          
        if ( abs ( records[x]. chunk_len - records[y].chunk_len ) > tau ) continue;          
#ifdef WITHOUT_VCN
        int a,b,f=0;
        for ( a = 0 ; a <= tau; a ++ ){
          for ( b = 0 ; b <= tau; b++ ){
            if ( records[x].str[a] == records[y].str[b] ){
              f=1;
              break;
            }
          }
          if ( f ) break;
        }
        if ( !f ) { continue; }
#else
        if ( virtual_chunk_number_filtering ( &records[x], &records[y], tau) != PASSED ){ continue; }
#endif          
        total_underflow_candi ++;
        ed = _edit_distance ( records[x].str, records[x].len, records[y].str, records[y].len, tau);
        if ( ed <= tau ){
          result_num ++;
          fprintf ( stderr, "%d %d-%d\n", ed, y, x );
#ifdef DEBUG
          if (ed <=tau) {
            records[i].resultn ++;
            output_candidate ( records, x, y, 1, ed, tau );
          }else if ( !g_cal_edit_distance_result_flag ) {
            output_candidate ( records, x, y, 1, ed, tau );
          }
#endif
        }
          
      }	
    }
  }
      
  /* Now output some useful infromation for me */
#ifdef DEBUG
  /* Dump the chunks information first */
  
  dump_chunks();
  
  /* Out put all the chunk's prefix length and candidate number */  
  fprintf(stderr, "----------- Start dump records --------");
  for ( i = 0; i < records_num; i ++ ){    
    fprintf(stderr, "RECORD: %d CAND: %d RST: %d CKN: %d PFX: %d --> ",
            i, records[i].candn, records[i].resultn, records[i].chunk_len, 
            records[i].prefix_len);
    for ( j = 0; j < records[i].chunk_len; j ++ ){
      if ( j == records[i].prefix_len ){
        fprintf(stderr, " || ");
      }
        chunk_p = records[i].chunk_slots[j].chunk;
        fprintf(stderr, "[%d|%d|%d|%d|%s] ", chunk_p->candn, chunk_p->len, 
                chunk_p->frq, chunk_p->pfx_num, chunk_p->str); 
    }
    fprintf(stderr, "||\n");
  }
  
  fprintf(stderr, "----------- End dump records --------");
   
#endif
  // FREE:
  return 0;
}

    

