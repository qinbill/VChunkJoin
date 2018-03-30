/*
  ============================================================================
  Name        : chunk_joins_c.c
  Author      : Jianbin Qin
  Version     : 0.0.1
  Copyright   : CSE
  Description : This is a test program of similarity joins
  ============================================================================
*/

#include <stdio.h>
#include <ctype.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include "header.h"
#include "usage.h"




record_t g_records[RECORD_MAX_LINE_NUM];
int g_curr_record_num;


unsigned int cand_num = 0;
long long int total_cand_num = 0;
long long int total_cand_one = 0;
long long int total_underflow_candi = 0;
long long int total_underflow_cand_one = 0;
long long int total_prefix_len = 0;
long long int total_cand_zero = 0;
long long int total_number_of_chunk = 0;
long long int total_index_entry = 0;


unsigned int underflow_records=0;
int result_num = 0;

extern char __usage_information[1024];


#ifdef DEBUG
long long int total_prefix_chunk_length = 0;
#endif  

int g_dump_all_flag = 0;
int g_dump_prefix_flag = 0;
int g_print_underflow_flag = 0;
int g_print_all_cand_info = 0;
int g_without_chunk_number_filter = 0;
int g_cal_edit_distance_result_flag = 0;
int g_join_underflows = 1;
int g_tau = -1;
int sort_data_flag = 0;
char *g_bound = NULL;
int g_v_seed = 121231;
extern chunk_t *g_chunks_head, *g_chunks_last;
extern int g_chunk_num;

char g_version[]=VERSION;


void print_version(){
  fprintf(stderr, "Version: %s\n", g_version);
  exit(0);
}


void print_usage(){
  fprintf(stderr, "usage: <-b bound dict file>\n");
  fprintf(stderr, "       <-g virtual bound random seed>\n");
  fprintf(stderr, "       <-t edit distance /tau>\n");
  fprintf(stderr, "       [-d dump all the chunks and strings]\n");
  fprintf(stderr, "       [-o not join underflow strings]\n");
  fprintf(stderr, "       [-p dump all the prefix sorted by frequence]\n");
  fprintf(stderr, "       [-c mute the chunk number filtering\n");
  fprintf(stderr, "       [-r calculate the final edit-distance result\n");
  fprintf(stderr, "       [-u print underflow candidates\n");
  fprintf(stderr, "       [-s print ppjoin running time\n");
  fprintf(stderr, "       [-h> for help information]\n");
  fprintf(stderr, "This program output one line in stdout and all the candidates and information in stderr\n");
  fprintf(stderr, "  --Stdout output format  explanation:\n");
  fprintf(stderr, "       RNUM    :  All Input Records Number\n");
  fprintf(stderr, "       CBD     :  The Chunk Boundary chars\n");
  fprintf(stderr, "       TAU     :  Inputededit distence\n");
  fprintf(stderr, "       TCADT   :  Total candidates\n");
  fprintf(stderr, "       RCADT   :  TCADT-UDCDT, it means the candidates exclude underflows\n");
  fprintf(stderr, "       UDCDT   :  Number of candidates create by underflows\n");
  fprintf(stderr, "       UDNUM   :  Number of records is underflow\n");
  fprintf(stderr, "       DCNUM   :  All Distinct Chunk Number\n");
  fprintf(stderr, "       DWCNUM  :  All Distinct Widow Chunk Number\n");
  fprintf(stderr, "       DICNUM  :  All Distinct Indexed Chunk Number\n");
  fprintf(stderr, "       AVGPCL  :  Average Prefix Chunk length\n");
  fprintf(stderr, "   --Stderr output format explanation\n");
  fprintf(stderr, "       Candidate line is begin by CAND or BLCAND format is below\n");
  fprintf(stderr, "       CAND[Candidate id] <Record id of a>[L:record length of a ][T:Chunk number of a]- \\\n");
  fprintf(stderr, "                 -<Record id of b>[L:record lenght of b][T:chunk number of b] <string a>---<string b> \n");
#ifdef Debug
  Fprintf(stderr, "   --Candidate debug information format\n");
  fprintf(stderr, "       'xxxx(i)'    : Represents a useless chunk xxxx and i is it's overall frequence\n");
  fprintf(stderr, "       [xxxx(i)]    : Represents a Prefix chunk xxxx and i is it's overall frequence\n");
  fprintf(stderr, "       <[xxxx(i)]>  : Represents a Matched Prefix chunk xxxx and i is it's overall frequence\n");
  fprintf(stderr, "       Rid          : Is the record id of this record\n");
#endif
  fprintf(stderr, "   --Prefix chunks information format\n");
  fprintf(stderr, "       PREFIX_CHUNKS: PF[the frequence in prefix] TF[the frequence in the overall]  \"prefix string\"\n");
  print_version();
  exit(0);
}

int main(int argc, char **argv) {
  int idx[RECORD_MAX_LINE_LEN];
  int c, i;
  struct timeval start_run, start_joint, time_end;

  while ((c = getopt(argc, argv, "srcuodphvg:b:t:")) != -1)
    switch (c) {
      case 'b':
        g_bound = optarg;
        break;
      case 'g':
        g_v_seed = atoi(optarg);
        break;
      case 'o':
        g_join_underflows = 0;
        break;
      case 't':
        g_tau = atoi(optarg);
        break;
      case 's':
        sort_data_flag = 1;
        break;
      case 'd':
        g_dump_all_flag = 1;
        break;
      case 'r':
        g_cal_edit_distance_result_flag = 1;
        break;
      case 'c':
        g_without_chunk_number_filter = 1;
      case 'p':
        g_dump_prefix_flag = 1;
        break;
      case 'u':
        g_print_underflow_flag = 1;
        break;
      case 'h':
        print_usage();
        break;
      case 'v':
        print_version();
        break;
      case '?':
        if (optopt == 'b'  || optopt == 't')
          fprintf(stderr, "Error: Option -%c requires an argument.\n",
                  optopt);
        else if (isprint(optopt))
          fprintf(stderr, "Error: Unknown option `-%c'.\n", optopt);
        else
          fprintf(stderr, "Error: Unknown option character `\\x%x'.\n",
                  optopt);
        return 1;
      default:
        print_usage();
    }
  // debug
  if (g_bound == NULL || g_tau < 0) {
    fprintf(stderr, "Error: Missing arguments. check again\n");
    print_usage();
  }

#ifdef DEBUG
  fprintf(stderr, "INFO: Tau = [%d], Bound =[%s]\n", g_tau, g_bound);
#endif


  gettimeofday(&start_run,NULL);
  if (init_bound_map(g_bound, g_v_seed)==-1){
    fprintf(stderr, "Error: Bound char list has problem, check it\n");
  }

  g_curr_record_num = read_all_documents(stdin, g_records);
  if (g_curr_record_num < 0) {
    fprintf(stderr, "Error: Read data error\n");
    return EXIT_FAILURE;
  }

  // init the hashtable 
  init_index(MAX_CHUNK_NUM);

  for(i=0;i<g_curr_record_num;i++){
    // The part is to split string into bound
    g_records[i].chunk_len=split_string_by_bound(g_records[i].str, g_records[i].len, idx, g_records[i].vcn);
    total_number_of_chunk += g_records[i].chunk_len;
    if ((g_records[i].chunk_slots=(chunk_slot_t *)malloc(sizeof(chunk_slot_t)*g_records[i].chunk_len))==NULL){
      fprintf(stderr, "ERROR: Out of memory\n");
      exit(-1);
    }
    // build the chunk list by using former splition.
    build_record_chunks(g_records[i].str, idx, g_records[i].chunk_len,g_records[i].chunk_slots);
  }
  
  // sort all chunks by overall frequence
  sort_all_chunk_list_by_freq( g_records, g_curr_record_num);
  
  // The first steps for ppjoin_ed;
  // use location based mismatch to calculate the prefix length
  calculate_all_prefix_length( g_records, g_curr_record_num, g_tau);
  
  gettimeofday(&start_joint, NULL);

  ResetUsage();

  // This is the part to do ppjoin_ed
  ppjoin_ed(g_records, g_curr_record_num, g_tau);

  ShowUsage();

  gettimeofday(&time_end, NULL);
  
  
  if(g_dump_all_flag)
    dump_records(stderr, g_curr_record_num, g_records);



  fprintf(stdout, "RNUM= %d  TAU= %d CAND0= %lld   CAND1= %lld  CAND2= %lld  UDCDT= %lld  UDNUM= %d  RST_NUM= %d  TCN= %lld  TPL= %lld  APL= %.3f  TIDX= %lld  VCN_SEED= %d  CBD= \"%s\"  ",
	  g_curr_record_num, g_tau, total_cand_zero, total_cand_one, total_cand_num, total_underflow_candi, underflow_records,
	  result_num, total_number_of_chunk, total_prefix_len, (float)total_prefix_len / ( g_curr_record_num - underflow_records ), total_index_entry,  g_v_seed, g_bound);
  
#ifdef DEBUG
  fprintf(stdout, "PCL= %.3f  ", total_prefix_chunk_length / (float)total_prefix_len);
#endif

  fprintf(stdout, "TOTAL= %.3f  PRE_PRC= %.3f  JOINT= %.3f  Usage: %s\n", 
          time_end.tv_sec - start_run.tv_sec + (time_end.tv_usec - start_run.tv_usec) / (double)1e6,
          start_joint.tv_sec - start_run.tv_sec + (start_joint.tv_usec - start_run.tv_usec) /(double)1e6,
          time_end.tv_sec - start_joint.tv_sec + (time_end.tv_usec - start_joint.tv_usec) /(double)1e6,
          __usage_information);


  //FREE

  
  return EXIT_SUCCESS;
}
