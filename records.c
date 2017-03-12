/*
 * records.c
 *
 *  Created on: Apr 22, 2009
 *      Author: jianbinqin
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "header.h"



char *_buffer;
int _buffer_pos = 0;
extern int sort_data_flag;

static int compare(const void *a, const void *b)
{
  const record_t *ap=(record_t *)a;;
  const record_t *bp=(record_t *)b;;

  //  return (*da > *db) - (*da < *db);
  return (ap->len > bp->len ) - (ap->len < bp->len);
}




/*
 * Read all documents from the files and store it in the record
 * data type. This part can be revised for the large memory
 * Consumption.
 */
int read_all_documents(FILE* fp, record_t * records)
{
  int num = 0;
  /* buffer for read in */
  char readbuffer[ RECORD_MAX_LINE_LEN ];
  
  if (fp == NULL){
    fprintf(stderr, "Error: Input error\n");
    return -1;
  }

  _buffer = (char *)malloc(sizeof(char)*MAX_FILE_SIZE);
  if ( _buffer == NULL ){
    fprintf ( stderr, "Error; Memory allocate err in raed_all_document for _buffer ");
    return -1;
  }
  
  while (fgets(readbuffer, RECORD_MAX_LINE_LEN, fp) != NULL){
    int len = strlen(readbuffer);

    // wipe out the new line char
    while(len > 0 && (readbuffer[len - 1] == '\n' || readbuffer[len - 1] == '\r' )){ readbuffer[len - 1] = '\0'; len--;}
    if (len <= 0) continue;

#ifdef CASE_INSENSITIVE
    for(int j=0;j<len;j++) if(readbuffer[j]>='A' && readbuffer[j]<='Z') readbuffer[j]=readbuffer[j]-('A'-'a');
#endif

    strcpy ( _buffer + _buffer_pos, readbuffer );
    
    records[num].str= _buffer + _buffer_pos;
    records[num].len=len;
    records[num].chunk_slots=NULL;
    records[num].chunk_len=0;
	records[num].stat = 1;
    _buffer_pos = _buffer_pos + (len + 1);
    if ( _buffer_pos + RECORD_MAX_LINE_LEN > MAX_FILE_SIZE ){
      fprintf( stderr, "Error input file exist maximal filesize: %d\n", MAX_FILE_SIZE );
      return -1;
    }
    num ++;
  }

  // to sort the whole record list
  if ( sort_data_flag)  
    qsort(records, num, sizeof(record_t), compare);

  /**  
  int i = 0;
  for ( i = 0; i < num ;i ++ ) fprintf ( stderr, "[%d] %s\n", i, records[i].str );
  **/
  
  return num;
}


/*
 * Dump the whole records
 */
void dump_records(FILE *stream, int record_num, record_t * records)
{
  int i,j,t;
  
  for(i=0; i< record_num ; i++)
    {
      fprintf(stream, "DUMP_ALL RD:[%d]:L[%d]:T[%d]:P[%d] ", i,records[i].len,
	      records[i].chunk_len, records[i].prefix_len);
      for(j=0;j<VIRTUAL_BOUND_LEN;j++)
	fprintf(stream, "V%d[%d] ", j, records[i].vcn[j]); 
      fprintf(stream, "'%s' ", records[i].str);
      t = 1;

#ifdef DEBUG
      char buff[RECORD_MAX_LINE_LEN];
      for(j=0;j<records[i].chunk_len;j++){
	strncpy ( buff, records[i].chunk_slots[j].chunk->str, records[i].chunk_slots[j].chunk->len );
	fprintf(stream,"[%s]<%d><%d>, ", buff, records[i].chunk_slots[j].chunk->frq, records[i].chunk_slots[j].chunk->pfx_num);
      }
#endif
      fprintf(stream,"\n");
    }
}










