//
// Created by 안재찬 on 2019/11/20.
//

#include <stdio.h>
#include <pthread.h>

#define NUM_THREAD 16
#define NUM_WORK 1000000

int cnt_global;

class QNode {
public:
  bool locked;

  explicit QNode(bool _locked) : locked(_locked) {
  }
};

// Global tail
QNode *tail;

class Lock {
public:
  virtual void lock() = 0;
  virtual void unlock() = 0;
};

class CLHLock : public Lock {
public:
  QNode *my_node;
  QNode *pred_node;

public:
  void lock() override {
    my_node = new QNode(true);
    pred_node = __sync_lock_test_and_set(&tail, my_node);

    while (pred_node->locked)
      pthread_yield();

    delete pred_node;
  }

  void unlock() override {
    my_node->locked = false;
  }
};

void *thread_work(void *args) {
  CLHLock lk;
  for (int i = 0; i < NUM_WORK; i++) {
    lk.lock();
    cnt_global++;
    lk.unlock();
  }
}

int main() {
  tail = new QNode(false);

  pthread_t threads[NUM_THREAD];

  for (int i = 0; i < NUM_THREAD; i++) {
    pthread_create(&threads[i], NULL, thread_work, NULL);
  }
  for (int i = 0; i < NUM_THREAD; i++) {
    pthread_join(threads[i], NULL);
  }

  printf("cnt_global: %d\n", cnt_global);

  delete tail;

  return 0;
}
