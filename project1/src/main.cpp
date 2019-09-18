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
  int last_thread = 0;
  key = new KEYTYPE[total_tuples];
  tmp_key = new KEYTYPE[total_tuples];
  unsigned char *buffer = new unsigned char[KEY_SIZE];
#ifdef VERBOSE
  printf("file_size: %zu total_tuples: %d key_per thread: %d\n", file_size, total_tuples, key_per_thread);
  auto start = high_resolution_clock::now();
#endif

  // read data from input file & start sorting
  for (int cur_tuple = 0; cur_tuple < total_tuples; cur_tuple++) {
    size_t offset = cur_tuple * TUPLE_SIZE;
    pread(input_fd, buffer, KEY_SIZE, offset);
    key[cur_tuple].assign(buffer, cur_tuple);
    tmp_key[cur_tuple].assign(buffer, cur_tuple);
    if (cur_tuple+1 >= (last_thread+1) * key_per_thread && last_thread < MAX_THREADS - 1) {
      th[last_thread] = thread(mergeSort, last_thread, last_thread * key_per_thread, cur_tuple);
      last_thread++;
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
    if (cur == FILE_THRESHOLD - 1 || i == total_tuples - 1) {
      last_inserted = pwrite(output_fd, buffer, (cur+1) * TUPLE_SIZE, last_inserted);
      cur = 0;
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
