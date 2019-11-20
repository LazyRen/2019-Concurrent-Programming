#include <cstdio>
#include <pthread.h>

#define NUM_THREAD    16
#define NUM_WORK      1000000

int cnt_global;
/* to allocate cnt_global & clh_object in different cache lines */
int gap[128];
bool *clh_object;

bool clh_init(bool* clh_object);
void lock(bool* lock_object);
void unlock(bool* lock_object);
void* thread_work(void* args);

int main()
{
  pthread_t threads[NUM_THREAD];

  clh_init(clh_object);

  /*
  for (int i = 0; i < NUM_THREAD; i++)
    pthread_create(&threads[i], NULL, thread_work, NULL);

  for (int i = 0; i < NUM_THREAD; i++)
    pthread_join(threads[i], NULL);

  printf("cnt_global: %d\n", cnt_global);
  */
  return 0;
}

bool clh_init(bool* clh_object)
{
  if (clh_object != NULL)
    return false;

  clh_object = new bool;
  *clh_object = false;

  return true;
}

void lock(bool** lock_object)
{
  while (__sync_lock_test_and_set(lock_object, 1) == 1) {}
}

void unlock(bool** lock_object)
{
  __sync_synchronize();
  *lock_object = 0;
}

void* thread_work(void* args)
{
  for (int i = 0; i < NUM_WORK; i++) {
    lock(&clh_object);
    cnt_global++;
    unlock(&clh_object);
  }

  return NULL;
}
