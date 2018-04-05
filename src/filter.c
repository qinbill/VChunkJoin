/**
   This is filter module 

**/

#include <math.h>
// #include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "header.h"




int string_length_filtering(record_t *ra, record_t *rb, int tau)
{
  if ( abs (ra->len - rb->len) <= tau )
    return PASSED;
  else
    return FILTERED_BY_STRING_LENGTH;
}

int position_filtering(record_t *ra, int posa, int ordera, record_t *rb, int posb, int orderb, int tau)
{
  //int ret;
  //int i;
  int la,lb,ca,cb;

  la=ra->len;
  lb=rb->len;
  ca=ra->chunk_len;
  cb=rb->chunk_len;
  
  if (posa < posb ){
    if ( posb - posa > tau )
      return FILTERED_BY_MATCHED_POSITION_INFO;
    if ( lb < la && (posb-posa)+(la-lb) >tau )
      return FILTERED_BY_MATCHED_POSITION_INFO;
  }else{
    if ( posa - posb > tau)
      return FILTERED_BY_MATCHED_POSITION_INFO;
    if ( la < lb && (posa-posb)+(lb-la)>tau )
      return FILTERED_BY_MATCHED_POSITION_INFO;
  }
  
  if (abs(ordera-orderb)>tau )
    return FILTERED_BY_MATCHED_POSITION_INFO;
  return PASSED;
}


int chunk_number_filtering(record_t *ra, record_t *rb, int tau)
{
  if ( abs (ra->chunk_len - rb->chunk_len) <= tau )
    return PASSED;
  else
    return FILTERED_BY_CHUNK_NUMBER;
}

int virtual_chunk_number_filtering( record_t *ra, record_t *rb, int tau)
{
  int *vcna = ra->vcn;
  int *vcnb = rb->vcn;
  int i; 
  int a = 0, b = 0;


  //  return PASSED;
  for ( i = 0; i < VIRTUAL_BOUND_LEN; i ++ ){
    if ( vcna[i] > vcnb[i] ){
      a += (vcna[i] - vcnb[i]);
    }else{
      b += (vcnb[i] - vcna[i]);
    }
    if ( a > tau || b > tau ){
      return FILTERED_BY_VIRTUAL_CHUNK_NUMBER;
    }
  }

  return PASSED;
}


static int _compare_chunks (const chunk_t *da, const chunk_t  *db)
{
  int ret;
   
  ret = (da->frq > db->frq) - (da->frq < db->frq);
  if(ret == 0){
    ret = (da->sign > db->sign) - ( da->sign < db->sign );
  }
  return ret;
}



static int _compare_slots_by_position( const void *da, const void *db)
{

  const chunk_slot_t *csa = *((chunk_slot_t **)da);
  const chunk_slot_t *csb = *((chunk_slot_t **)db);

  return ( csa->pos > csb->pos ) - ( csa->pos < csb->pos );
}


inline int _l_one_distance ( const char *str_a, int len_a, const char *str_b, int len_b, int w_start, int w_end , int tau, int right_err) 
{
  int i = 0;
  int alpha = 2*tau;
  int rerrs = 2*right_err;
  int mapa[128], mapb[128];
  int l_one = 0;

  memset ( mapa, 0, 128 * sizeof ( int ));
  memset ( mapb, 0, 128 * sizeof ( int ));
  
  for( i = w_start; i <= w_end; i++ ){
    mapa[(int)str_a[i]]++;
    if ( i < len_b )
      mapb[(int)str_b[i]]++;
    else
      l_one ++;
  }
  
  for ( i = 127; i >= 0; i-- ){
    l_one += abs ( mapa[i] - mapb[i] );
    if ( l_one + rerrs > alpha )
      return -1;
  }
  return l_one;
}



inline void _sum_right_errs(chunk_slot_t **slots, int *errs, int chunk_len, int slen)
{
  int list[128*1024];
  int i,left_len,right_len;
  int ed = 0;
  int order;
  
  
  for(i=0;i<chunk_len;i++) list[i] = 0;
  
  for(i=slen-1;i>=0;i--){
    order=slots[i]->order;
    left_len=0;
    while( order - left_len - 1 >= 0 && list[ order - left_len - 1] )
      left_len ++;
    right_len = 0;
    while (order + right_len + 1 < chunk_len && list[ order + right_len + 1] )
      right_len ++;

    if( (right_len + left_len) % 2 == 0 )
      ed ++;
    list[order] = 1;
    errs[i]=ed;
  }
}



// This is a very interesting filtering program. 
int content_based_mismatch_filtering(record_t *ra, record_t *rb, int tau)
{
  int i, as, bs;
  int t;
  int alpha = 2 * tau;
  int mlen;
  chunk_slot_t *a_slots, *b_slots;
  chunk_slot_t *a_mis_slots[1024];
  int mis_area_s[1024];
  int mis_area_e[1024];
  int mis_area_len[1024];
  int mis_area_right_err[1024];
  int mis_area_num = 0;
  int a_chunk_len, b_chunk_len;
  int a_mis=0;
  int ws, we, is, ie;
  int l_one;
  int mis_len;
  int right_errs[1024];
  

  mlen = ra->len > rb->len? ra->len: rb->len;
  
  a_slots = ra->chunk_slots;
  b_slots = rb->chunk_slots;
  a_chunk_len = ra->chunk_len;
  b_chunk_len = rb->chunk_len;
  
  as = bs = 0;
  
  // this part is for calculate the mis-match window
  while ( as < a_chunk_len && bs < b_chunk_len) {
    if (as < a_chunk_len && bs < b_chunk_len ){
      if ( a_slots[as].chunk == b_slots[bs].chunk ){
        if ( abs ( int(a_slots[as].pos) - int(b_slots[bs].pos) ) <= tau ){
          // loosely mis-matching.
          as ++;
          bs ++;
        }else if ( a_slots[as].pos < b_slots[bs].pos ){
          if ( as > 0 && a_slots[as].chunk == a_slots[as-1].chunk &&
               bs > 0 && a_slots[as].chunk == b_slots[bs-1].chunk &&
               abs( int(a_slots[as].pos) - int(b_slots[bs-1].pos) ) <= tau ){
	    
          }else{
            a_mis_slots[a_mis++] = &a_slots[as];
          }
          as ++;
        }else{
          bs ++;
        }
      }else{
        t = _compare_chunks( a_slots[as].chunk, b_slots[bs].chunk);
        if ( t < 0 ){
          if ( as > 0 && a_slots[as].chunk == a_slots[as-1].chunk && 
               bs > 0 && a_slots[as].chunk == b_slots[bs-1].chunk &&
               abs ( int(a_slots[as].pos) - int(b_slots[bs-1].pos) ) <= tau ){

          }else{
            a_mis_slots[a_mis++] = &a_slots[as];
          }
          as ++;
        }else{
          bs ++;
        }
      }
    }
    if ( a_mis > alpha )
      return FILTERED_BY_CONTENT_BASED_MISMATCH;
  }

  for ( ; as < a_chunk_len; as ++ ){
    if ( as > 0 && a_slots[as].chunk == a_slots[as-1].chunk && 
         bs > 0 && a_slots[as].chunk == b_slots[bs-1].chunk &&
         abs( int(a_slots[as].pos) - int(b_slots[bs-1].pos) ) <= tau ){
    }else{
      a_mis_slots[a_mis++] = &a_slots[as];
    }
  }
  

#ifdef WITHOUT_CONTENT_MISMATCH_FILTER
  return PASSED;
#endif


  
  //  qsort ( records[i].chunk_slots, records[i].chunk_len, sizeof (chunk_slot_t), _compare_chunk_slots);  
  qsort (a_mis_slots, a_mis, sizeof (chunk_slot_t *), _compare_slots_by_position);
  _sum_right_errs ( a_mis_slots, right_errs, ra->chunk_len, a_mis );
  if (right_errs[0] > tau)
    return FILTERED_BY_CONTENT_BASED_MISMATCH;
  right_errs[a_mis] = 0;


  // make separate mis match chunks into a mis match area list.
  i = 0;
  while ( i < a_mis ){
    mis_area_s[mis_area_num] = a_mis_slots[i]->pos;
    while ( i+1 < a_mis && a_mis_slots[i]->order + 1 == a_mis_slots[i+1]->order )
      i ++;
    mis_area_e[mis_area_num] = a_mis_slots[i]->pos + a_mis_slots[i]->len-1;
    mis_area_len[mis_area_num] = mis_area_e[mis_area_num] - mis_area_s[mis_area_num] + 1;
    mis_area_right_err[mis_area_num] = right_errs[i+1];
    mis_area_num ++;
    i ++;
  }

  
  // from left to right to calculate the l_one distance and filter
  i = 0;
  ws = we = 0;
  is = ie = 0;
  while ( i < mis_area_num ){
    is = i;
    mis_len = mis_area_len[is];
    while ( i + 1 < mis_area_num && mis_len + 2 * mis_area_right_err[i] < alpha ){
      i ++;
      mis_len += mis_area_len[i];
    }
    ie = i;
    while ( is > 0 && mis_len + 2 * mis_area_right_err[ie] < alpha ){
      is --;
      mis_len += mis_area_len[is];
    }
    ws = mis_area_s[is];
    we = mis_area_e[ie];
    
    //    int l_one_distance ( const char *str_a, int len_a, const char *strb, int len_b, int w_start, int w_end , int tau)
    l_one = _l_one_distance ( ra->str, ra->len, rb->str, rb->len, ws, we, tau, mis_area_right_err[ie] );
    if ( l_one == -1 )
      return FILTERED_BY_CONTENT_BASED_MISMATCH;
    
    i ++;
  }
  return PASSED;
}






