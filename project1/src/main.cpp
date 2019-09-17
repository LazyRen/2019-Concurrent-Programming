#include "concurrent_sorting.h"

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
  const size_t file_size   = lseek(input_fd, 0, SEEK_END);
  total_tuples   = file_size / TUPLE_SIZE;
  key_per_thread = total_tuples / MAX_THREADS;
  int last_thread = 0;
  key.reserve(total_tuples);
  unsigned char *buffer = new unsigned char[KEY_SIZE];
#ifdef DEBUG
  printf("file_size: %zu total_tuples: %d key_per thread: %d\n", file_size, total_tuples, key_per_thread);
#endif
  // read data from input file
  for (int cur_tuple = 0; cur_tuple < total_tuples; cur_tuple++) {
    size_t offset = cur_tuple * TUPLE_SIZE;
    size_t ret = pread(input_fd, buffer, KEY_SIZE, offset);
    if (ret != KEY_SIZE) {
      printf("error: failed to read key%d\n", cur_tuple);
    }
    key.push_back(KEYTYPE(buffer, cur_tuple));
    tmp_key.push_back(KEYTYPE(buffer, cur_tuple));
    if (cur_tuple+1 >= (last_thread+1) * key_per_thread && last_thread < MAX_THREADS - 1) {
      th[last_thread] = thread(mergeSort, last_thread, last_thread * key_per_thread, cur_tuple);
      // printf("%d: %d ~ %d\n", last_thread, last_thread * key_per_thread, cur_tuple);
      last_thread++;
    }
  }
  delete[] buffer;
  th[last_thread] = thread(mergeSort, last_thread, last_thread * key_per_thread, total_tuples-1);
  last_thread++;
  // sort.
  th[MAX_THREADS-1].join();

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
#ifdef DEBUG
  printKeys(left, right);
#endif
}

void mergeSort(int pid, int l, int r)
{
  if (l < r) {
    sort(key.begin() + l, key.begin() + r + 1);
#ifdef DEBUG
    printf("%d: %d ~ %d\n\n", pid, l, r);
#endif
  }

  for (int div = 2; div <= MAX_THREADS; div *= 2) {
    if (pid % div == div - 1) {
      for (int i = 1; i < div; i++) {
        if (th[pid-i].joinable()) {
          try {
            th[pid-i].join();
          } catch (const std::exception& ex) {
            printf("pid%d catched error doing %d merging\n", pid, div);
          }
        }
      }
      int m;
      if (pid != MAX_THREADS - 1) {
        l = key_per_thread * (pid+1 - div);
        m = l + (r - l) / 2;
      } else {
        m = l-1;
        l = key_per_thread * (pid+1 - div);
      }
      merge(l, m, r);
#ifdef DEBUG
      printf("%d merging! %d ~ %d\n", pid, l, r);
#endif
    };
  }
}

void printKeys(int left, int right)
{
  for (int i = left; i <= right; i++) {
    for (int j = 0; j < 3; j++)
      printf("%d", (int)key[i].binary[j] - '0');
    printf("\t");
  }
  printf("\n");
}
