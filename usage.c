#include <stdio.h>
#include <sys/resource.h>
#include <sys/time.h>
#include "usage.h"


static struct rusage Save_r;
static struct timeval Save_t;

long __user_time_sec;
long __user_time_usec;

long __sys_time_sec;
long __sys_time_usec;

long __elapse_time_sec;
long __elapse_time_usec;

char __usage_information[1024] = "";


void ResetUsage(void)
{
  getrusage(RUSAGE_SELF, &Save_r);
  gettimeofday(&Save_t, NULL);
}

void ShowUsage(void)
{
  struct timeval elapse_t;
  struct rusage r;
  
  getrusage(RUSAGE_SELF, &r);
  gettimeofday(&elapse_t, NULL);
  
  if (elapse_t.tv_usec < Save_t.tv_usec){
	elapse_t.tv_sec--;
	elapse_t.tv_usec += 1000000;
  }
  if (r.ru_utime.tv_usec < Save_r.ru_utime.tv_usec){
	r.ru_utime.tv_sec--;
	r.ru_utime.tv_usec += 1000000;
  }
  if (r.ru_stime.tv_usec < Save_r.ru_stime.tv_usec){
	r.ru_stime.tv_sec--;
	r.ru_stime.tv_usec += 1000000;
  }
  
  __elapse_time_sec = (long) (elapse_t.tv_sec - Save_t.tv_sec);
  __elapse_time_usec = (long) (elapse_t.tv_usec - Save_t.tv_usec);
  
  __user_time_sec = (long) (r.ru_utime.tv_sec - Save_r.ru_utime.tv_sec);
  __user_time_usec = (long) (r.ru_utime.tv_usec - Save_r.ru_utime.tv_usec);
  
  __sys_time_sec = (long) (r.ru_stime.tv_sec - Save_r.ru_stime.tv_sec);
  __sys_time_usec = (long) (r.ru_stime.tv_usec - Save_r.ru_stime.tv_usec);
  
  
  sprintf(__usage_information,
		  "System usage stats %ld.%06ld elapsed %ld.%06ld user %ld.%06ld system sec",
		  (long) (elapse_t.tv_sec - Save_t.tv_sec),
		  (long) (elapse_t.tv_usec - Save_t.tv_usec),
		  (long) (r.ru_utime.tv_sec - Save_r.ru_utime.tv_sec),
		  (long) (r.ru_utime.tv_usec - Save_r.ru_utime.tv_usec),
		  (long) (r.ru_stime.tv_sec - Save_r.ru_stime.tv_sec),
		  (long) (r.ru_stime.tv_usec - Save_r.ru_stime.tv_usec));
  
}
