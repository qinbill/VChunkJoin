#include <stdio.h>
#include <string.h>
#include <math.h>
#include <stdlib.h>

int num[256];

struct charstat {
  char c;
  char belong;
  int freq;  
};

struct charstat stats[256];
int numstats = 0;

int main(int argc, char ** argv)
{
  char c;
  int i;
  int scale = 5;
  int tau = 1;

  if (argc <= 1){
    fprintf(stderr, "Usage: %s <tau> [scale]\t  (By default scale = 5. You could tune this one within 1~10 to find the best one)\n", argv[0]);
    exit(-1);
  } else {
    tau = atoi(argv[1]);
    if (argc > 2){
      scale = atoi(argv[2]);
    }
  }
  
  while((c=getchar())!=EOF){
    num[(int)c] ++;
  }
  int numRecords = num['\n'];

  for(i='a';i<='z';i++){
    if(num[i] > numRecords/4){
      stats[numstats].c = i;
      stats[numstats].freq = num[i];
      stats[numstats].belong = 0;
      numstats ++;
#ifdef DEBUG
      fprintf(stderr, "%c= %d\n", i, num[i]);
#endif
    }      
  }

  for(i='A';i<='Z';i++){
    if(num[i] > numRecords/2){
      stats[numstats].c = i;
      stats[numstats].freq = num[i];
      stats[numstats].belong = 0;
      numstats ++;
#ifdef DEBUG            
      fprintf(stderr, "%c= %d\n", i, num[i]);
#endif
    }      
  }

  int DBoxsize = numRecords * (scale * tau + 1);
  int RBoxsize = numRecords * (scale * tau + 1);
  
  while (DBoxsize > 0 || RBoxsize > 0) {
    if (DBoxsize > 0) {
      int nextAdd = -1;
      for (i=0;i<numstats;i++) {     
        if (stats[i].belong == 0){
          if (nextAdd == -1 ||
              abs(DBoxsize-stats[nextAdd].freq) < abs(DBoxsize-stats[i].freq)){
            nextAdd = i;
          }
        }      
      }
      if (nextAdd == -1) break;    
      DBoxsize = DBoxsize-stats[nextAdd].freq;
      stats[nextAdd].belong = 'D';
    }
    
    if (RBoxsize > 0) {
      int nextAdd = -1;
      for (i=0;i<numstats;i++) {
        if (stats[i].belong == 0){
          if (nextAdd == -1 ||
              abs(RBoxsize-stats[nextAdd].freq) < abs(RBoxsize-stats[i].freq)) {
            nextAdd = i;
          }
        }      
      }
      if (nextAdd == -1) break;    
      RBoxsize = RBoxsize-stats[nextAdd].freq;
      stats[nextAdd].belong = 'R';
    }
  }

  fprintf(stdout, "-s \"");
  for (i=0;i<numstats;i++) {
    if (stats[i].belong == 'D') {
      fprintf(stdout, "%c", stats[i].c);
    }
  }
  fprintf(stdout,"\"");

  fprintf(stdout, " -u \"");
  for (i=0;i<numstats;i++) {
    if (stats[i].belong == 'R') {
      fprintf(stdout, "%c", stats[i].c);
    }
  }
  fprintf(stdout, "\"\n");
}

