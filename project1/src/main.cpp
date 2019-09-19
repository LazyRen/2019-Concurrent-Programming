#include "concurrent_sorting.h"

int main(int argc, char* argv[])
{
  if (argc < 3) {
    printf("usage: ./run InputFile OutputFile\n");
    return 0;
  }

  // open input file.
  int input_fd = open(argv[1], O_RDONLY);
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
    }
    total_read += ret;
    for (; last_thread < ((total_read/100)/key_per_thread) && last_thread < MAX_THREADS - 1; last_thread++) {
      th[last_thread] = thread(mergeSort, last_thread, last_thread * key_per_thread,
                               (last_thread+1) * key_per_thread);
    }
  }
  delete[] buffer;

#ifdef VERBOSE
  auto stop = high_resolution_clock::now();
  auto duration = duration_cast<milliseconds>(stop - start);
  cout << "read & sort took " << duration.count() << "ms\n";
#endif
  mergeSort(last_thread, last_thread * key_per_thread, total_tuples);
  last_thread++;
  delete[] tmp_key;
#ifdef VERBOSE
  auto stop1 = high_resolution_clock::now();
  duration = duration_cast<milliseconds>(stop1 - stop);
  cout << "last thread sorting took " << duration.count() << "ms\n";
#endif

  // open output file.
  int output_fd;
  output_fd = open(argv[2], O_WRONLY | O_CREAT | O_TRUNC, 0777);
  if (output_fd == -1) {
    printf("error: open output file\n");
    return 0;
  }
  ftruncate(output_fd, file_size);

  // flush to output file.
  for (int i = 0; i < MAX_THREADS - 1; i++)
    th[i] = thread(parallelWrite, input_fd, output_fd, i * key_per_thread, (i+1) * key_per_thread);
  parallelWrite(input_fd, output_fd, (MAX_THREADS-1) * key_per_thread, total_tuples);

  for (int i = 0; i < MAX_THREADS - 1; i++) {
    if (th[i].joinable()) {
      try {
        th[i].join();
      } catch (const exception& ex) {
        printf("pid%d catched error while writing\n", i);
      }
    }
  }

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

  while (l <= mid && r < right)
  {
    if (key[l] <= key[r])
      tmp_key[s++] = key[l++];
    else
      tmp_key[s++] = key[r++];
  }

  int tmp = l > mid ? r : l;

  memcpy(tmp_key+s, key+tmp, (right-s)*sizeof(KEYTYPE));

  copy(tmp_key+left, tmp_key+right, key+left);
}

void mergeSort(int pid, int l, int r)
{
  if (l < r) {
    sort(key + l, key + r);
  }

  for (int div = 2; div <= MAX_THREADS; div *= 2) {
    if (pid % div == div - 1) {
      for (int i = 1; i < div; i++) {
        if (th[pid-i].joinable()) {
          try {
            th[pid-i].join();
          } catch (const exception& ex) {
            printf("pid%d catched error doing %d merging\n", pid, div);
          }
        }
      }
      int m;
      if (pid != MAX_THREADS - 1) {
        l = key_per_thread * (pid+1 - div);
        m = l + (r - 1 - l) / 2;
      } else {
        m = l-1;
        l = key_per_thread * (pid+1 - div);
      }
      merge(l, m, r);
    };
  }
}

void parallelWrite(int input_fd, int output_fd, int start, int end)
{
  int buf_size = min(FILE_THRESHOLD, end - start);
  unsigned char *buffer = new unsigned char[TUPLE_SIZE * buf_size];
  for (int i = start, cur = 0, last_inserted = 0; i < end; i++, cur++) {
    pread(input_fd, buffer + cur * TUPLE_SIZE, TUPLE_SIZE, key[i].index * TUPLE_SIZE);
    if (cur == buf_size - 1 || i == end - 1) {
      size_t ret = pwrite(output_fd, buffer, (cur+1) * TUPLE_SIZE, start * TUPLE_SIZE + last_inserted);
      last_inserted += ret;
      cur = -1;
    }
  }
  delete[] buffer;
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
