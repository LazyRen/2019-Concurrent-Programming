#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>
#include <chrono>
using namespace std;

#define KEY_SIZE        (10)
#define TUPLE_SIZE      (100)
#define MAX_THREADS     (32)

class KEYTYPE {
public:
  unsigned char binary[KEY_SIZE];
  int index;

  KEYTYPE(unsigned char* binary, int index) {
    memcpy(this->binary, binary, KEY_SIZE);
    this->index = index;
  }

  KEYTYPE& operator= (const KEYTYPE &k2) {
  memcpy(this->binary, k2.binary, KEY_SIZE);
  this->index = k2.index;
  return *this;
}
};

vector<KEYTYPE> key;
vector<KEYTYPE> tmp_key;
thread th[MAX_THREADS];
int total_tuples;
int key_per_thread;

void merge(int left, int mid, int right);
void mergeSort(int pid, int l, int r);
bool isOrdered(int left, int right);
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
