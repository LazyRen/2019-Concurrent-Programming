#include <cstdio>
#include <pthread.h>

#define NUM_THREADS   10
#define NUM_INCREMENT 1000000

long cnt_global;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

void* thread_func(void* arg)
{
  long cnt_local = 0;

  for (int i = 0; i < NUM_INCREMENT; i++) {
    pthread_mutex_lock(&mutex);
    cnt_global++;
    pthread_mutex_unlock(&mutex);
    cnt_local++;
  }

  return (void*)cnt_local;
}

int main()
{
  pthread_t threads[NUM_THREADS];

  for (int i = 0; i < NUM_THREADS; i++) {
    if (pthread_create(&threads[i], 0, thread_func, NULL) < 0) {
      printf("error: pthread_create failed!\n");
      return 0;
    }
  }

  long ret;
  for (int i = 0; i < NUM_THREADS; i++) {
    pthread_join(threads[i], (void**)&ret);
    printf("thread %ld: local count -> %ld\n", threads[i], ret);
  }
  printf("global count -> %ld\n", cnt_global);

  return 0;
}
