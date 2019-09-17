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

  bool operator< (const KEYTYPE &other) {
    int cmp = memcmp(this->binary, other.binary, KEY_SIZE);
    if (cmp < 0)
      return true;
    return false;
  }

  bool operator<= (const KEYTYPE &other) {
    int cmp = memcmp(this->binary, other.binary, KEY_SIZE);
    if (cmp <= 0)
      return true;
    return false;
  }

  bool operator> (const KEYTYPE &other) {
    int cmp = memcmp(this->binary, other.binary, KEY_SIZE);
    if (cmp > 0)
      return true;
    return false;
  }

  bool operator>= (const KEYTYPE &other) {
    int cmp = memcmp(this->binary, other.binary, KEY_SIZE);
    if (cmp >= 0)
      return true;
    return false;
  }
};

vector<KEYTYPE> key;
vector<KEYTYPE> tmp_key;
thread th[MAX_THREADS];

// inline bool cmp(const string &a, const string &b);
void merge(int left, int mid, int right);
void mergeSort(int l, int r);

int main(int argc, char* argv[])
{
  if (argc < 3) {
    printf("usage: ./run InputFile OutputFile\n");
    return 0;
  }

  // open input file.
  int input_fd;
  input_fd = open(argv[1], O_RDONLY);
  if (input_fd == -1) {
    printf("error: open input file\n");
    return 0;
  }

  // get size of input file.
  size_t file_size;
  file_size = lseek(input_fd, 0, SEEK_END);
  int total_tuples = file_size / TUPLE_SIZE;
  key.reserve(total_tuples);
  unsigned char *buffer = new unsigned char[KEY_SIZE];

  // read data from input file
  for (int cur_tuple = 0; cur_tuple < total_tuples; cur_tuple++) {
    size_t offset = cur_tuple * TUPLE_SIZE;
    size_t ret = pread(input_fd, buffer, KEY_SIZE, offset);
    if (ret != KEY_SIZE) {
      printf("error: failed to read key%d\n", cur_tuple);
    }
    key.push_back(KEYTYPE(buffer, cur_tuple));
    // for (int i = 0; i < KEY_SIZE; i++) {
    //   printf("%x", buffer[i]);
    // }
    // printf(" vs ");
    // for (int i = 0; i < KEY_SIZE; i++) {
    //   printf("%x", key[cur_tuple].binary[i]);
    // }
    // printf("\n");
  }
  tmp_key = key;
  delete[] buffer;

  // sort.
  mergeSort(0, total_tuples-1);

  // open output file.
  int output_fd;
  output_fd = open(argv[2], O_RDWR | O_CREAT | O_TRUNC, 0777);
  if (output_fd == -1) {
    printf("error: open output file\n");
    return 0;
  }

  // flush to output file.
  buffer = new unsigned char[TUPLE_SIZE];
  for (int cur_tuple = 0; cur_tuple < total_tuples; cur_tuple++) {
    size_t offset_r = key[cur_tuple].index * TUPLE_SIZE;
    size_t offset_w = cur_tuple * TUPLE_SIZE;
    size_t ret = pread(input_fd, buffer, TUPLE_SIZE, offset_r);
    if (ret != TUPLE_SIZE) {
      printf("error: failed to read tuple%d\n", key[cur_tuple].index);
    }
    ret = pwrite(output_fd, buffer, TUPLE_SIZE, offset_w);
    if (ret != TUPLE_SIZE) {
      printf("error: write output file at tuple%d\n", key[cur_tuple].index);
    }
  }
  delete[] buffer;

  // close input file.
  close(input_fd);
  // close output file.
  close(output_fd);

  return 0;
}

// inline bool cmp(const string &a, const string &b)
// {

// }

void merge(int left, int mid, int right)
{
  int l = left;
  int r = mid + 1;
  int s = left;

  while (l <= mid && r <= right)
  {
    if (key[l] <= key[r])
      tmp_key[s++] = key[l++];
    else
      tmp_key[s++] = key[r++];
  }

  int tmp = l > mid ? r : l;

  while(s <= right)
    tmp_key[s++] = key[tmp++];

  for (int i = left; i <= right; i++)
    key[i] = tmp_key[i];
}

void mergeSort(int l, int r)
{
  if (l < r) {

    int m = l + (r - l) / 2;

    mergeSort(l, m);
    mergeSort(m + 1, r);

    merge(l, m, r);
    // if (r - l < SPLIT_THRESHOLD) {
    //   sort(key.begin() + l, key.begin() + r + 1);
    // } else {
    //   int m = l + (r - l) / 2;

    //   mergeSort(l, m);
    //   mergeSort(m + 1, r);

    //   merge(l, m, r);
    // }
  }
}
