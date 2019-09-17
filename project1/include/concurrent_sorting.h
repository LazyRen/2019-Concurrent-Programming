#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>
using namespace std;

#define KEY_SIZE        (10)
#define TUPLE_SIZE      (100)
#define SPLIT_THRESHOLD (1<<10)
#define MAX_THREADS     (100)

class KEYTYPE {
public:
  unsigned char binary[KEY_SIZE];
  int index;

  KEYTYPE(unsigned char* binary, int index) {
    memcpy(this->binary, binary, KEY_SIZE);
    this->index = index;
  }
};

vector<KEYTYPE> key;
vector<KEYTYPE> tmp_key;
thread th[MAX_THREADS];

void merge(int left, int mid, int right);
void mergeSort(int l, int r);

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
