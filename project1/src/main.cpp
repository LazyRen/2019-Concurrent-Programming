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
  const size_t file_size = lseek(input_fd, 0, SEEK_END);
  total_tuples    = file_size / TUPLE_SIZE;
  key_per_thread  = total_tuples / MAX_THREADS;
  unsigned int last_thread = 0;
  key = new KEYTYPE[total_tuples];
  tmp_key = new KEYTYPE[total_tuples];
#ifdef VERBOSE
  printf("file_size: %zu total_tuples: %d key_per thread: %d\n", file_size, total_tuples, key_per_thread);
  auto start = high_resolution_clock::now();
#endif

  // read data from input file & start sorting
  unsigned char *buffer = new unsigned char[TUPLE_SIZE * FILE_THRESHOLD];
  for (size_t total_read = 0; total_read < file_size;) {
    unsigned int last_inserted = total_read/TUPLE_SIZE;
    size_t ret = pread(input_fd, buffer, TUPLE_SIZE * FILE_THRESHOLD, total_read);
    for (unsigned int i = 0; i < ret / TUPLE_SIZE; i++) {
      key[last_inserted + i].assign(buffer + i*TUPLE_SIZE, last_inserted + i);
      tmp_key[last_inserted + i].assign(buffer + i*TUPLE_SIZE, last_inserted + i);
#ifdef DEBUG
      printf("key%u: ", last_inserted+i);
      printKeys(last_inserted+i, last_inserted+i);
#endif
    }
    total_read += ret;
#ifdef DEBUG
    printf("total_read: %lu\n", total_read);
#endif
    for (; last_thread < ((total_read/100)/key_per_thread) && last_thread < MAX_THREADS - 1; last_thread++) {
      th[last_thread] = thread(mergeSort, last_thread, last_thread * key_per_thread,
                               (last_thread+1) * key_per_thread - 1);
#ifdef DEBUG
      printf("%u: %u ~ %u\n", last_thread, last_thread*key_per_thread, (last_thread+1)*key_per_thread-1);
#endif
    }
  }
  delete[] buffer;

#ifdef VERBOSE
  auto stop = high_resolution_clock::now();
  auto duration = duration_cast<milliseconds>(stop - start);
  cout << "read & sort took " << duration.count() << "ms\n";
#endif
  mergeSort(last_thread, last_thread * key_per_thread, total_tuples-1);
  last_thread++;
  delete[] tmp_key;
#ifdef VERBOSE
  auto stop1 = high_resolution_clock::now();
  duration = duration_cast<milliseconds>(stop1 - stop);
  cout << "last thread sorting took " << duration.count() << "ms\n";
#endif

#ifdef DEBUG
  for (int i = 0; i < total_tuples; i++) {
    printf("%d: %u ", i, key[i].index);
    printKeys(i, i);
  }
#endif

  // open output file.
  int output_fd;
  output_fd = open(argv[2], O_RDWR | O_CREAT | O_TRUNC, 0777);
  if (output_fd == -1) {
    printf("error: open output file\n");
    return 0;
  }

  // flush to output file.
  buffer = new unsigned char[TUPLE_SIZE * FILE_THRESHOLD];
  for (int i = 0, cur = 0, last_inserted = 0; i < total_tuples; i++, cur++) {
    pread(input_fd, buffer + cur * TUPLE_SIZE, TUPLE_SIZE, key[i].index * TUPLE_SIZE);
#ifdef DEBUG
    printf("%d: %u ", cur, key[i].index);
    printKeys(i, i);
#endif
    if (cur == FILE_THRESHOLD - 1 || i == total_tuples - 1) {
#ifdef DEBUG
      printf("last_inserted: %d\n", last_inserted);
#endif
      size_t ret = pwrite(output_fd, buffer, (cur+1) * TUPLE_SIZE, last_inserted);
      last_inserted += ret;
      cur = -1;
    }
  }
  delete[] buffer;

#ifdef VERBOSE
  auto stop2 = high_resolution_clock::now();
  duration = duration_cast<milliseconds>(stop2 - stop1);
  cout << "file writing took " << duration.count() << "ms\n";
#endif

  //free
  delete[] key;

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

  memcpy(tmp_key+s, key+tmp, (right-s+1)*sizeof(KEYTYPE));

  copy(tmp_key+left, tmp_key+right+1, key+left);
}

void mergeSort(int pid, int l, int r)
{
  if (l < r) {
    sort(key + l, key + (r+1));
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
      printf("%d: %d ~ %d\n", pid, l, r);
      printKeys(l, r);
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
