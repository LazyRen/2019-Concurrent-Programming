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
  printf("file_size: %zu total_tuples: %d key_per_thread: %d\n", file_size, total_tuples, key_per_thread);
  auto start = high_resolution_clock::now();
#endif

  // read data from input file & start sorting
  for (int i = 0; i < MAX_THREADS - 1; i++) {
    th[i] = thread(parallelRead, i, input_fd,
                   i * key_per_thread * TUPLE_SIZE, (i+1) * key_per_thread * TUPLE_SIZE);
  }
  parallelRead(MAX_THREADS-1, input_fd,
               (MAX_THREADS-1) * key_per_thread * TUPLE_SIZE, total_tuples * TUPLE_SIZE);
  delete[] tmp_key;

#ifdef VERBOSE
  auto stop = high_resolution_clock::now();
  auto duration = duration_cast<milliseconds>(stop - start);
  cout << "read & sort took " << duration.count() << "ms\n";
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
    th[i] = thread(parallelWrite, i, input_fd, output_fd, i * key_per_thread, (i+1) * key_per_thread);
  parallelWrite(MAX_THREADS-1, input_fd, output_fd, (MAX_THREADS-1) * key_per_thread, total_tuples);

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
  auto stop1 = high_resolution_clock::now();
  duration = duration_cast<milliseconds>(stop1 - stop);
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
  if (l < r)
    sort(key + l, key + r);

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

void parallelRead(int pid, int input_fd, size_t start, size_t end)
{
  int buf_size = min(FILE_THRESHOLD, static_cast<int>(end - start)) * TUPLE_SIZE;
  unsigned char *buffer = new unsigned char[buf_size];
  for (size_t cur_offset = start; cur_offset < end;) {
    if (cur_offset + buf_size > end)
      buf_size = (end - cur_offset);
    size_t ret = pread(input_fd, buffer, buf_size, cur_offset);
    for (unsigned int i = 0; i < ret / TUPLE_SIZE; i++) {
      key[(cur_offset)/TUPLE_SIZE+i].assign(buffer + i*TUPLE_SIZE, (cur_offset)/TUPLE_SIZE + i);
    }
    cur_offset += ret;
  }
  delete[] buffer;

  copy(key + start/TUPLE_SIZE, key + end/TUPLE_SIZE, tmp_key + start/TUPLE_SIZE);

  mergeSort(pid, start/TUPLE_SIZE, end/TUPLE_SIZE);
}

void parallelWrite(int pid, int input_fd, int output_fd, int start, int end)
{
  int buf_size = min(FILE_THRESHOLD, end - start);
  unsigned char *buffer = new unsigned char[buf_size * TUPLE_SIZE];
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
