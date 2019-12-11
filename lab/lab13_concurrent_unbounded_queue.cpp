#include <atomic>
#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <thread>
#include <iostream>
using namespace std;

#define NUM_PRODUCER                4
#define NUM_CONSUMER                NUM_PRODUCER
#define NUM_THREADS                 (NUM_PRODUCER + NUM_CONSUMER)
#define NUM_ENQUEUE_PER_PRODUCER    10000000
#define NUM_DEQUEUE_PER_CONSUMER    NUM_ENQUEUE_PER_PRODUCER

typedef struct QueueNode {
    int key;
    atomic<QueueNode*> next;

    QueueNode(int _key) : key(_key), next(NULL) {}
} QueueNode;

class Queue {
private:
  atomic<QueueNode*> head;
  atomic<QueueNode*> tail;

public:
  Queue() {
    QueueNode *obj = new QueueNode(-1);
    head.store(obj);
    tail.store(obj);
  }

  void enqueue(int key) {
    QueueNode *obj         = new QueueNode(key);
    bool cas_succeed       = false;

    while (!cas_succeed) {
      QueueNode *loaded_tail = tail.load();
      QueueNode *tmp         = loaded_tail->next.load();

      if (tmp != NULL) {
        tail.compare_exchange_weak(loaded_tail, tmp);
      } else {
        cas_succeed = loaded_tail->next.compare_exchange_weak(tmp, obj);
        if (cas_succeed) {
          tail.compare_exchange_weak(loaded_tail, obj);
        }
      }
    }
  }

  int dequeue() {
    bool cas_succeed = false;
    int ret;

    while (!cas_succeed) {
      QueueNode *loaded_head = head.load();
      QueueNode *next_head = loaded_head->next.load();

      if (next_head)
        cas_succeed = head.compare_exchange_weak(loaded_head, next_head);
      if (cas_succeed)
        ret = next_head->key;
    }
    return ret;
  }

  ~Queue() {
    QueueNode *cur = head;
    while (cur) {
      QueueNode *next = cur->next.load();
      delete cur;
      cur = next;
    }
  }
};

bool flag_verification[NUM_PRODUCER * NUM_ENQUEUE_PER_PRODUCER];
Queue queue;

void ProducerFunc(int tid) {
    int key_enqueue = NUM_ENQUEUE_PER_PRODUCER * tid;
    for (int i = 0; i < NUM_ENQUEUE_PER_PRODUCER; i++) {
        queue.enqueue(key_enqueue);
        // printf("%d enqueued\n", key_enqueue);
        key_enqueue++;
    }

    return;
}

void ConsumerFunc() {
    for (int i = 0; i < NUM_DEQUEUE_PER_CONSUMER; i++) {
        int key_dequeue = queue.dequeue();
        // printf("%d dequeued\n", key_dequeue);
        flag_verification[key_dequeue] = true;
    }

    return;
}

int main(void) {
    std::thread threads[NUM_THREADS];

    for (int i = 0; i < NUM_THREADS; i++) {
        if (i < NUM_PRODUCER) {
            threads[i] = std::thread(ProducerFunc, i);
        } else {
            threads[i] = std::thread(ConsumerFunc);
        }
    }

    for (int i = 0; i < NUM_THREADS; i++) {
        threads[i].join();
    }

    // verify
    for (int i = 0; i < NUM_PRODUCER * NUM_ENQUEUE_PER_PRODUCER; i++) {
        if (flag_verification[i] == false) {
            printf("INCORRECT!\n");
            return 0;
        }
    }
    printf("CORRECT!\n");

    return 0;
}
