#include "external_sort.h"

int main(int argc, char* argv[])
{
  if (argc < 3) {
    printf("usage: ./run InputFile OutputFile\n");
    exit(0);
  }

  // open input file.
  int input_fd = open(argv[1], O_RDONLY);
  if (input_fd == -1) {
    printf("error: open input file\n");
    exit(0);
  }

  // get size of input file.
  const size_t file_size = lseek(input_fd, 0, SEEK_END);
  total_file       = (file_size / FILE_THRESHOLD)
                   + (file_size % FILE_THRESHOLD == 0 ? 0 : 1);
  total_tuples     = (file_size / TUPLE_SIZE);
  tuples_per_file  = (file_size / TUPLE_SIZE) / total_file;
  chunk_per_thread = (file_size / total_file) / MAX_THREADS;
  tuples           = new TUPLETYPE[tuples_per_file];
#ifdef VERBOSE
  printf("file_size: %zu total_tuples: %zu key_per_thread: %zu\n", file_size, total_tuples, chunk_per_thread / TUPLE_SIZE);
  auto start = high_resolution_clock::now();
#endif

  // read data from input file & start sorting
  for (int cur_file = 0; cur_file < total_file; cur_file++) {
    for (size_t i = 0; i < MAX_THREADS - 1; i++) {
      th[i] = thread(parallelRead, i, input_fd, i * chunk_per_thread, (i+1) * chunk_per_thread);
    }
    parallelRead(MAX_THREADS-1, input_fd, (MAX_THREADS-1) * chunk_per_thread, tuples_per_file * TUPLE_SIZE);

    if (total_file != 1) {//do external sort with tmp_files
      string outfile = to_string(cur_file) + ".tmp";
      int tmp_fd = open(outfile.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0777);
      if (tmp_fd == -1) {
        printf("error: open %s file\n", outfile.c_str());
        exit(0);
      }
      size_t to_write = file_size / total_file;
      tmp_files.push_back(FILEINFO(outfile, 0, to_write));
      th[cur_file] = thread(writeToFile, tmp_fd, tuples, to_write, 0);
      close(tmp_fd);
    }
  }

  if (total_file != 1) {
    delete [] tuples;
    for (int i = 0; i < total_file; i++) {
      if (th[i].joinable()) {
        try {
          th[i].join();
        } catch (const exception& ex) {
          printf("pid%d catched error while writing\n", i);
        }
      }
    }
  }

#ifdef VERBOSE
  auto stop = high_resolution_clock::now();
  auto duration = duration_cast<milliseconds>(stop - start);
  cout << "read & sort took " << duration.count() << "ms\n";
#endif

  // open output file.
  int output_fd = open(argv[2], O_WRONLY | O_CREAT | O_TRUNC, 0777);
  if (output_fd == -1) {
    printf("error: open output file\n");
    exit(0);
  }

  // flush to output file.
  if (total_file == 1) {
    writeToFile(output_fd, tuples, file_size, 0);
    delete[] tuples;
  } else {
    externalSort(output_fd);
  }
#ifdef VERBOSE
  auto stop1 = high_resolution_clock::now();
  duration = duration_cast<milliseconds>(stop1 - stop);
  cout << "file writing took " << duration.count() << "ms\n";
#endif

  // close input file.
  close(input_fd);
  // close output file.
  close(output_fd);

  return 0;
}

void mergeSort(int pid, int l, int r)
{
  if (l < r)
    sort(tuples + l, tuples + r);

  for (int div = 2; div <= MAX_THREADS; div *= 2) {
    if (pid % div == div - 1) {
      for (int i = 1; i < div; i++) {
        if (th[pid-i].joinable()) {
          try {
            th[pid-i].join();
          } catch (const exception& ex) {
            printf("pid%d catched error while %d merging\n", pid, div);
          }
        }
      }
      int m;
      if (pid != MAX_THREADS - 1) {
        l = (chunk_per_thread / TUPLE_SIZE) * (pid+1 - div);
        m = l + (r - 1 - l) / 2;
      } else {
        m = l-1;
        l = (chunk_per_thread / TUPLE_SIZE) * (pid+1 - div);
      }
      inplace_merge(tuples+l, tuples+m+1, tuples+r);
    };
  }
}

void parallelRead(int pid, int input_fd, size_t start, size_t end)
{
  size_t buf_size = min(FILE_THRESHOLD, end - start);
  unsigned char *buffer = new unsigned char[buf_size];
  for (size_t cur_offset = start; cur_offset < end;) {
    if (cur_offset + buf_size > end)
      buf_size = (end - cur_offset);
    size_t ret = pread(input_fd, buffer, buf_size, cur_offset);
    for (unsigned int i = 0; i < ret / TUPLE_SIZE; i++) {
      tuples[(cur_offset)/TUPLE_SIZE+i].assign(buffer + i*TUPLE_SIZE);
    }
    cur_offset += ret;
  }
  delete[] buffer;

  mergeSort(pid, start/TUPLE_SIZE, end/TUPLE_SIZE);
}

void externalSort(int output_fd)
{
  unsigned char *tmp_tuples[total_file][2][BUFFER_SIZE];
  unsigned char *write_buf[2][BUFFER_SIZE * 5];
  bool is_done[total_file];
  int tmp_fd[total_file], tmp_swi[total_file], write_swi = 0;
  size_t tmp_idx[total_file], write_idx = 0;
  size_t last_write_buf = (BUFFER_SIZE * 5), total_write = 0;
  future<size_t> read[total_file], write;
  priority_queue<pair<TUPLETYPE, pair<int, size_t> > > queue;// {TUPLE, {file_idx, buf_idx}}

  for (int i = 0; i < total_file; i++) {
    is_done[i] = false;
    tmp_idx[i] = 0;
    tmp_swi[i] = 0;
    tmp_fd[i]  = open(tmp_files[i].file_name.c_str(), O_RDONLY);
    if (tmp_fd[i] == -1) {
      printf("error: open tmp%d file\n", i);
      exit(0);
    }
    read[i] = async(readFromFile, tmp_fd[i], tmp_tuples[i][0], BUFFER_SIZE, 0);
  }

  for (int i = 0; i < total_file; i++) {
    tmp_files[i].cur_offset += read[i].get();
    queue.push({TUPLETYPE(tmp_tuples[i][0][0]), {i, 0}});
    read[i] = async(readFromFile, tmp_fd[i], tmp_tuples[i][1], BUFFER_SIZE, tmp_files[i].cur_offset);
  }

  write = async(writeToFile, output_fd, (void*)0, 0, 0);

  while (!queue.empty()) {
    auto min_tuple = queue.top(); queue.pop();
    memcpy(write_buf[write_idx], min_tuple.first.binary, TUPLE_SIZE);
    write_idx += TUPLE_SIZE;

    if (write_idx >= last_write_buf) {
      total_write += write.get();
      write = async(writeToFile, output_fd, write_buf[write_swi][0], last_write_buf, total_write);
    }
    int f_idx = min_tuple.second.first; size_t offset = min_tuple.second.second;
    if (offset == BUFFER_SIZE - TUPLE_SIZE) {
      if (!is_done[f_idx]) {
        tmp_files[f_idx].cur_offset += read[f_idx].get();
        read[f_idx] = async(readFromFile, tmp_fd[f_idx], tmp_tuples[f_idx][tmp_swi[f_idx]],
                            BUFFER_SIZE, tmp_files[f_idx].cur_offset);
        tmp_swi[f_idx] = (tmp_swi[f_idx]+1) % 2;
        queue.push({tmp_tuples[f_idx][tmp_swi[f_idx]][0] ,{f_idx, 0}});

        if (tmp_files[f_idx].cur_offset + BUFFER_SIZE == tuples_per_file * TUPLE_SIZE)
          is_done[f_idx] = true;
      }
    } else {
      queue.push({tmp_tuples[f_idx][tmp_swi[f_idx]][offset+TUPLE_SIZE], {f_idx, offset+TUPLE_SIZE}});
    }
  }
}


size_t readFromFile(int fd, void *buf, size_t nbyte, size_t offset)
{
  size_t total_read = 0;
  while (nbyte) {
    size_t ret = pread(fd, buf, nbyte, offset);
    nbyte      -= ret;
    offset     += ret;
    total_read += ret;
  }

  return total_read;
}

size_t writeToFile(int fd, const void *buf, size_t nbyte, size_t offset)
{
  size_t total_write = 0;
  while (nbyte) {
    size_t ret = pwrite(fd, buf, nbyte, offset);
    nbyte  -= ret;
    offset += ret;
    total_write += ret;
  }

  return total_write;
}

void printKeys(int left, int right)
{
  for (int i = left; i < right; i++) {
    for (int j = 0; j < 3; j++)
      printf("%d", (int)tuples[i].binary[j] - '0');
    printf("\n");
  }
  printf("\n");
}
