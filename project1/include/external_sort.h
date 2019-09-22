#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <future>
#include <queue>
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

#define KEY_SIZE        (10UL)
#define TUPLE_SIZE      (100UL)
#define MAX_THREADS     (32)
#define FILE_THRESHOLD  (500000000UL)
#define BUFFER_SIZE     (50000000UL)
#define W_BUFFER_SIZE   (500000000UL)

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

class TUPLETYPE {
public:
  unsigned char binary[TUPLE_SIZE];

  TUPLETYPE() {};

  TUPLETYPE(unsigned char* binary) {
    memcpy(this->binary, binary, TUPLE_SIZE);
  }

  void assign(unsigned char* binary) {
    memcpy(this->binary, binary, TUPLE_SIZE);
  }
};

class FILEINFO {
  public:
    string file_name;
    size_t cur_offset;
    size_t size;

    FILEINFO() {};

    FILEINFO(string file, size_t offset, size_t size) : file_name(file), cur_offset(offset), size(size) {};
};

TUPLETYPE *tuples;
thread th[MAX_THREADS];
vector<FILEINFO> tmp_files;
int total_file;
size_t total_tuples;
size_t chunk_per_file;
size_t chunk_per_thread;

void mergeSort(int pid, int l, int r);
void parallelRead(int pid, int input_fd, size_t start, size_t end);
void externalSort(int output_fd);
size_t readFromFile(int fd, void *buf, size_t nbyte, size_t offset);
size_t writeToFile(int fd, const void *buf, size_t nbyte, size_t offset);
bool isSorted(unsigned char *buf, size_t nbyte);
void printKey(TUPLETYPE tuple);
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

bool operator< (const TUPLETYPE &k1,const TUPLETYPE &k2)
{
  int cmp = memcmp(k1.binary, k2.binary, KEY_SIZE);
  if (cmp < 0)
    return true;
  return false;
}

bool operator<= (const TUPLETYPE &k1,const TUPLETYPE &k2) {
  int cmp = memcmp(k1.binary, k2.binary, KEY_SIZE);
  if (cmp <= 0)
    return true;
  return false;
}

bool operator> (const TUPLETYPE &k1,const TUPLETYPE &k2)
{
  int cmp = memcmp(k1.binary, k2.binary, KEY_SIZE);
  if (cmp > 0)
    return true;
  return false;
}

bool operator>= (const TUPLETYPE &k1,const TUPLETYPE &k2) {
  int cmp = memcmp(k1.binary, k2.binary, KEY_SIZE);
  if (cmp >= 0)
    return true;
  return false;
}
