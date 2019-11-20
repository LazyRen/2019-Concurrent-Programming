#include <cstdio>
#include <pthread.h>

#define NUM_THREAD    16
#define NUM_WORK      1000000

int cnt_global;
/* to allocate cnt_global & tas_object in different cache lines */
int gap[128];
int tas_object;

void lock(int* lock_object);
void unlock(int* lock_object);
void* thread_work(void* args);

int main()
{
  pthread_t threads[NUM_THREAD];

  for (int i = 0; i < NUM_THREAD; i++)
    pthread_create(&threads[i], NULL, thread_work, NULL);

  for (int i = 0; i < NUM_THREAD; i++)
    pthread_join(threads[i], NULL);

  printf("cnt_global: %d\n", cnt_global);

  return 0;
}

void lock(int* lock_object)
{
  while (__sync_lock_test_and_set(lock_object, 1) == 1) {}
}

void unlock(int* lock_object)
{
  __sync_synchronize();
  *lock_object = 0;
}

void* thread_work(void* args)
{
  for (int i = 0; i < NUM_WORK; i++) {
    lock(&tas_object);
    cnt_global++;
    unlock(&tas_object);
  }

  return NULL;
}
