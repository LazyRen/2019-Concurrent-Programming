#ifndef EXTERNAL_SORT_H
#define EXTERNAL_SORT_H

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <future>
#include <omp.h>
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
#define MAX_THREADS     (16)
#define FILE_THRESHOLD  (1000000000UL)
#define BUFFER_SIZE     (100000000UL)
#define W_BUFFER_SIZE   (200000000UL)

class TUPLETYPE {
public:
  unsigned char binary[TUPLE_SIZE];

  TUPLETYPE() {};

  TUPLETYPE(unsigned char* binary) {
    memcpy(this->binary, binary, TUPLE_SIZE);
  }

  bool operator> (const TUPLETYPE &other)
  {
    if (memcmp(this->binary, other.binary, KEY_SIZE) > 0)
      return true;
    return false;
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

struct RadixTraits
{
    static const int nBytes = KEY_SIZE - 1;

    int kth_byte(const TUPLETYPE& x, int k) {
        return x.binary[KEY_SIZE - 2 - k] & ((unsigned char) 0xFF);
    }
    bool compare(const TUPLETYPE& k1, const TUPLETYPE& k2) {
        return (memcmp(k1.binary, k2.binary, KEY_SIZE) < 0);
    }
};

struct OneByteRadixTraits
{
    static const int nBytes = 1;

    int kth_byte(const TUPLETYPE& x, int k) {
        return x.binary[k] & ((unsigned char) 0xFF);
    }
    bool compare(const TUPLETYPE& k1, const TUPLETYPE& k2) {
        return (memcmp(k1.binary, k2.binary, KEY_SIZE) < 0);
    }
};

TUPLETYPE *tuples;
thread th[MAX_THREADS];
vector<FILEINFO> tmp_files;
int total_file;
size_t total_tuples;
size_t chunk_per_file;
size_t chunk_per_thread;

void parallelSort(TUPLETYPE* tuples, size_t count);
void externalSort(int output_fd, TUPLETYPE* read_buf[][2]);
size_t readFromFile(int fd, void *buf, size_t nbyte, size_t offset);
size_t writeToFile(int fd, const void *buf, size_t nbyte, size_t offset);
void printKey(TUPLETYPE tuple);

struct pq_cmp {
    bool operator()(pair<TUPLETYPE*, pair<int, size_t> > &t1, pair<TUPLETYPE*, pair<int, size_t> > &t2)
    {
      return *t1.first > *t2.first;
    }
};

bool operator< (const TUPLETYPE &k1,const TUPLETYPE &k2)
{
  if (memcmp(k1.binary, k2.binary, KEY_SIZE) < 0)
    return true;
  return false;
}

bool operator<= (const TUPLETYPE &k1,const TUPLETYPE &k2) {
  if (memcmp(k1.binary, k2.binary, KEY_SIZE) <= 0)
    return true;
  return false;
}

bool operator> (const TUPLETYPE &k1,const TUPLETYPE &k2)
{
  if (memcmp(k1.binary, k2.binary, KEY_SIZE) > 0)
    return true;
  return false;
}

bool operator>= (const TUPLETYPE &k1,const TUPLETYPE &k2) {
  if (memcmp(k1.binary, k2.binary, KEY_SIZE) >= 0)
    return true;
  return false;
}

#endif
