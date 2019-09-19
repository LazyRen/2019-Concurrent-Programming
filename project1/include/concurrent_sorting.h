#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <future>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>
#ifdef VERBOSE
#include <chrono>
#include <iostream>
using namespace std::chrono;
#endif
using namespace std;

#define KEY_SIZE        (10)
#define TUPLE_SIZE      (100)
#define MAX_THREADS     (32)
#define FILE_THRESHOLD  (1<<19)

class KEYTYPE {
public:
  unsigned char binary[KEY_SIZE];
  unsigned int index;

  KEYTYPE() {};

  KEYTYPE(unsigned char* binary, int index) {
    memcpy(this->binary, binary, KEY_SIZE);
    this->index = index;
  }

  void assign(unsigned char* binary, int index) {
    memcpy(this->binary, binary, KEY_SIZE);
    this->index = index;
  }
};

KEYTYPE *key;
KEYTYPE *tmp_key;
thread th[MAX_THREADS];
int total_tuples;
int key_per_thread;

void merge(int left, int mid, int right);
void mergeSort(int pid, int l, int r);
void parallelWrite(int input_fd, int output_fd, int start, int end);
void printKeys(int left, int right);

bool operator< (const KEYTYPE &k1,const KEYTYPE &k2)
{
  int cmp = memcmp(k1.binary, k2.binary, KEY_SIZE);
  if (cmp < 0)
    return true;
  return false;
}

bool operator<= (const KEYTYPE &k1,const KEYTYPE &k2) {
  int cmp = memcmp(k1.binary, k2.binary, KEY_SIZE);
  if (cmp <= 0)
    return true;
  return false;
}

bool operator> (const KEYTYPE &k1,const KEYTYPE &k2)
{
  int cmp = memcmp(k1.binary, k2.binary, KEY_SIZE);
  if (cmp > 0)
    return true;
  return false;
}

bool operator>= (const KEYTYPE &k1,const KEYTYPE &k2) {
  int cmp = memcmp(k1.binary, k2.binary, KEY_SIZE);
  if (cmp >= 0)
    return true;
  return false;
}
