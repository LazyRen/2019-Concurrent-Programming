#include <atomic>
#include <stdio.h>
#include <limits.h>
#include <pthread.h>
#include <stdlib.h>
#include <time.h>
using namespace std;

#define MAX_NUM_THREAD  16
#define MAX_NUM_WORK    100000

template<class T>
class MarkableReference {
// https://stackoverflow.com/questions/40247249/what-is-the-c-11-atomic-library-equivalent-of-javas-atomicmarkablereferencet
private:
    uintptr_t val;
    static const uintptr_t mask = 1;
public:
    MarkableReference(T* ref = NULL, bool mark = false) : val(((uintptr_t)ref & ~mask) | (mark ? 1 : 0)) {}

    T* getRef() {
        return (T*)(val & ~mask);
    }

    bool getMark() {
        return (val & mask);
    }

    T* get(bool* is_marked) {
      *is_marked = getMark();
      return getRef();
    }
};

// list node structure
struct Node {
  int key;
  atomic<MarkableReference<Node>> next;

  Node(int _key) : key(_key) {}
};

class Window {
public:
  Node *pred;
  Node *curr;

  Window(Node* _pred, Node* _curr) : pred(_pred), curr(_curr) {}
};

// workload data
struct Work {
  char type;
  int value;
};

Node *head;
Node *tail;
Work thread_work[MAX_NUM_THREAD][MAX_NUM_WORK];
int num_work;

void list_init(Node** head, Node** tail);
Window find(int key);
bool list_add(int key);
bool list_remove(int key);
bool list_contains(int key);
void* ThreadFunc(void* args);

int main(void) {
  int num_thread;

  // input workloads
  FILE* fp = fopen("lab11_workload.txt", "r");

  fscanf(fp, "%d %d", &num_thread, &num_work);

  int thread_num;
  char work_type;
  int value;
  for (int i = 0; i < num_thread; i++) {
    fscanf(fp, "%d ", &thread_num);

    for (int j = 0; j < num_work; j++) {
      fscanf(fp, "%c %d ", &work_type, &value);

      thread_work[i][j].type = work_type;
      thread_work[i][j].value = value;
    }
  }

  fclose(fp);

  // set random seed
  srand(time(NULL));

  // initialize list
  list_init(&head, &tail);

  pthread_t threads[MAX_NUM_THREAD];

  // create threads
  for (long i = 0; i < num_thread; i++) {
    if (pthread_create(&threads[i], NULL, ThreadFunc, (void*)i) < 0) {
      exit(0);
    }
  }

  // wait threads end
  for (long i = 0; i < num_thread; i++) {
    pthread_join(threads[i], NULL);
  }

  // validate list
  FILE* fp_result = fopen("lab11_result.txt", "r");
  int size_result;
  int value_result;

  fscanf(fp_result, "%d", &size_result);

  bool is_valid = true;
  Node* it = head->next.load().getRef();
  for (int i = 0; i < size_result; i++) {
    if (it == tail) {
      is_valid = false;
      break;
    }
    fscanf(fp_result, "%d", &value_result);
    if (it->key != value_result) {
      is_valid = false;
      break;
    }
    it = it->next.load().getRef();
  }
  if (it != tail) {
    is_valid = false;
  }

  fclose(fp_result);

  if (is_valid) {
    printf("correct!\n");
  } else {
    printf("incorrect!\n");
  }

  return 0;
}

// initialize linked list
void list_init(Node** head, Node** tail)
{
  *head = (Node*)malloc(sizeof(Node));
  (*head)->key = INT_MIN;
  *tail = (Node*)malloc(sizeof(Node));
  (*tail)->key = INT_MAX;

  (*head)->next.store(MarkableReference<Node>(*tail));
  (*tail)->next.store(MarkableReference<Node>(NULL));
}

// find pred & curr node with key
Window find(int key) {
  Node *pred, *curr, *succ;
  bool marked, snip;

  retry:
  while (true) {
    pred = head;
    curr = pred->next.load().getRef();
    while (true) {
      succ = curr->next.load().get(&marked);
      while (marked) {
        MarkableReference<Node> tmp =  MarkableReference<Node>(curr);
        snip = pred->next.compare_exchange_weak(tmp, MarkableReference<Node>(succ));
        if (!snip)
          goto retry;
        curr = succ;
        succ = curr->next.load().get(&marked);
      }
      if (curr->key >= key)
        return Window(pred, curr);
      pred = curr;
      curr = succ;
    }
  }
}

// add key into the list
bool list_add(int key) {
  Node *node = new Node(key);
  Node *pred, *curr;
  bool splice;

  while (true) {
    Window window = find(key);
    pred = window.pred; curr = window.curr;
    if (curr->key == key) {
      free(node);
      return true;
    } else {
      node->next.store(MarkableReference<Node>(curr));
      MarkableReference<Node> tmp = MarkableReference<Node>(curr);
      if (pred->next.compare_exchange_weak(tmp, MarkableReference<Node>(node)))
        return true;
    }
  }
}

// remove key from the list
bool list_remove(int key) {
  Node *pred, *curr;
  bool snip;

  while (true) {
    Window window = find(key);
    pred = window.pred; curr = window.curr;
    if (curr->key != key) {
      return false;
    } else {
      MarkableReference<Node> succ = curr->next.load();
      snip = curr->next.compare_exchange_weak(succ, MarkableReference<Node>(succ.getRef(), true));

      if (!snip)
        continue;
      MarkableReference<Node> tmp = MarkableReference<Node> (curr);
      pred->next.compare_exchange_weak(tmp, succ);
      return true;
    }
  }
}

// check whether a key is in the list
bool list_contains(int key) {
  bool marked;
  Node *curr = head;

  while (curr->key < key)
    curr = curr->next.load().getRef();

  Node *succ = curr->next.load().get(&marked);

  return (curr->key == key && !marked);
}

// do workloads for each threads
void* ThreadFunc(void* args) {
  long tid = (long)args;

  for (int i = 0; i < num_work; i++) {
    switch (thread_work[tid][i].type) {
      case 'A':
        list_add(thread_work[tid][i].value);
        break;
      case 'R':
        list_remove(thread_work[tid][i].value);
        break;
      case 'C':
        list_contains(thread_work[tid][i].value);
        break;
      default:
        break;
    }
  }

  return NULL;
}
