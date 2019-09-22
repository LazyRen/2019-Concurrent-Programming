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
  chunk_per_file  =  (file_size / total_file);
  chunk_per_thread = chunk_per_file / MAX_THREADS;
  tuples           = new TUPLETYPE[chunk_per_file/TUPLE_SIZE];
  int tmp_fd[MAX_THREADS];
#ifdef VERBOSE
  printf("file_size: %zu total_tuples: %zu key_per_thread: %zu\n", file_size, total_tuples, chunk_per_thread / TUPLE_SIZE);
  printf("total_tmp_files: %d, chunk_per_file: %zu\n", total_file, chunk_per_file);
  printf("tuple array contains %zu keys\n", chunk_per_file/TUPLE_SIZE);
  auto start = high_resolution_clock::now();
#endif

  // read data from input file & start sorting
  for (int cur_file = 0; cur_file < total_file; cur_file++) {
    for (size_t i = 0; i < MAX_THREADS - 1; i++) {
      th[i] = thread(parallelRead, i, input_fd,
                     (chunk_per_file*cur_file) + (i*chunk_per_thread),
                     (chunk_per_file*cur_file) + ((i+1)*chunk_per_thread));
    }
    parallelRead(MAX_THREADS-1, input_fd,
                 (chunk_per_file*cur_file) + ((MAX_THREADS-1)*chunk_per_thread),
                 (chunk_per_file*(cur_file+1)));
    if (total_file != 1) {//do external sort with tmp_files
      string outfile = to_string(cur_file) + ".tmp";
      tmp_fd[cur_file] = open(outfile.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0777);
      if (tmp_fd[cur_file] == -1) {
        printf("error: open %s file\n", outfile.c_str());
        exit(0);
      }
      size_t to_write = file_size / total_file;
      tmp_files.push_back(FILEINFO(outfile, 0, to_write));
#ifdef VERBOSE
      printf("%s: %zu Bytes\n", outfile.c_str(), to_write);
#endif
      writeToFile(tmp_fd[cur_file], tuples, to_write, 0);
    }
  }

  // printf("wait for io to jon\n");
  // if (total_file != 1) {
  //   for (int i = 0; i < total_file; i++) {
  //     if (th_io[i].joinable()) {
  //       try {
  //         th_io[i].join();
  //       } catch (const exception& ex) {
  //         printf("pid%d catched error while writing\n", i);
  //       }
  //     }
  //     close(tmp_fd[i]);
  //   }
  // }

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
    delete [] tuples;
  } else {
    delete [] tuples;
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
      tuples[((cur_offset)/TUPLE_SIZE+i) % (chunk_per_file/TUPLE_SIZE)].assign(buffer + i*TUPLE_SIZE);
    }
    cur_offset += ret;
  }
  delete[] buffer;

  while (start >= chunk_per_file)
    start -= chunk_per_file;
  while (end > chunk_per_file)
    end -= chunk_per_file;

  mergeSort(pid, start/TUPLE_SIZE, end/TUPLE_SIZE);
}

void externalSort(int output_fd)
{
  bool is_done[total_file];
  int tmp_fd[total_file], tmp_swi[total_file];
  size_t last_write_buf = (BUFFER_SIZE * 5), total_write = 0, write_idx = 0;
  future<size_t> read[total_file];
  priority_queue<pair<TUPLETYPE, pair<int, size_t> >, vector<pair<TUPLETYPE, pair<int, size_t> > >, greater<pair<TUPLETYPE, pair<int, size_t> > > > queue;// {TUPLE, {file_idx, buf_idx}}
  unsigned char ***tmp_tuples = new unsigned char**[total_file];
  unsigned char *write_buf = new unsigned char[last_write_buf];

  for (int i = 0; i < total_file; i++) {
    tmp_tuples[i] = new unsigned char*[2];
    for(int j = 0; j < 2; j++)
      tmp_tuples[i][j] = new unsigned char[BUFFER_SIZE];
  }

  for (int i = 0; i < total_file; i++) {
    is_done[i] = false;
    tmp_swi[i] = 0;
    tmp_fd[i]  = open(tmp_files[i].file_name.c_str(), O_RDONLY);
    if (tmp_fd[i] == -1) {
      printf("error: open %s file\n", tmp_files[i].file_name.c_str());
      exit(0);
    }
    read[i] = async(readFromFile, tmp_fd[i], tmp_tuples[i][0], BUFFER_SIZE, 0);
  }

  for (int i = 0; i < total_file; i++) {
    tmp_files[i].cur_offset += read[i].get();
#ifdef VERBOSE
    printf("writing to disk %zu Bytes done\n", total_write);
    if (!isSorted(tmp_tuples[i][0], BUFFER_SIZE))
      printf("error: tmp buffer %d is not sorted!\n", i);
#endif
    queue.push({TUPLETYPE(tmp_tuples[i][0]), {i, 0}});
    read[i] = async(readFromFile, tmp_fd[i], tmp_tuples[i][1], BUFFER_SIZE, tmp_files[i].cur_offset);
  }

  while (!queue.empty()) {
    auto min_tuple = queue.top(); queue.pop();
    int f_idx = min_tuple.second.first; size_t offset = min_tuple.second.second;
    memcpy(write_buf + write_idx, min_tuple.first.binary, TUPLE_SIZE);
    write_idx += TUPLE_SIZE;

    // write buffer is full. Flush to disk.
    if (write_idx >= last_write_buf) {
#ifdef VERBOSE
      printf("writing to disk %zu Bytes done\n", total_write + write_idx);
      if (!isSorted(write_buf, write_idx))
        printf("error: write buffer is not sorted!\n");
      printf("%d: offset: %zu nbyte:%zu\n", output_fd, total_write, write_idx);
#endif
      total_write += writeToFile(output_fd, write_buf, write_idx, total_write);
      write_idx = 0;
    }

    if (offset + TUPLE_SIZE >= BUFFER_SIZE) {
      if (!is_done[f_idx]) {
        tmp_files[f_idx].cur_offset += read[f_idx].get();
#ifdef VERBOSE
        printf("%s is reading next chunk. %zu done\n", tmp_files[f_idx].file_name.c_str(), tmp_files[f_idx].cur_offset);
#endif
        read[f_idx] = async(readFromFile, tmp_fd[f_idx], tmp_tuples[f_idx][tmp_swi[f_idx]],
                            BUFFER_SIZE, tmp_files[f_idx].cur_offset);
        tmp_swi[f_idx] = (tmp_swi[f_idx]+1) % 2;
        queue.push({tmp_tuples[f_idx][tmp_swi[f_idx]], {f_idx, 0}});

        if (tmp_files[f_idx].cur_offset + BUFFER_SIZE >= tmp_files[f_idx].size)
          is_done[f_idx] = true;
      } else {
        if (tmp_files[f_idx].cur_offset < tmp_files[f_idx].size) {
          tmp_files[f_idx].cur_offset += read[f_idx].get();
          tmp_swi[f_idx] = (tmp_swi[f_idx]+1) % 2;
          queue.push({tmp_tuples[f_idx][tmp_swi[f_idx]], {f_idx, 0}});
        }
      }
    } else {
      queue.push({tmp_tuples[f_idx][tmp_swi[f_idx]] + (offset+TUPLE_SIZE), {f_idx, offset+TUPLE_SIZE}});
    }
  }

  for (int i = 0; i < total_file; i++) {
    for (int j = 0; j < 2; j++)
      delete[] tmp_tuples[i][j];
    delete[] tmp_tuples[i];
  }
  delete[] write_buf;
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
    if (ret == static_cast<size_t>(-1)) {
      printf("error: writing %zu to %d's %zu offset FAILED\n", nbyte, fd, offset);
      exit(0);
    }
    nbyte  -= ret;
    offset += ret;
    total_write += ret;
  }

  return total_write;
}

bool isSorted(unsigned char *buf, size_t nbyte)
{
  TUPLETYPE prev = TUPLETYPE(buf);
  for (size_t offset = TUPLE_SIZE; offset < nbyte; offset += TUPLE_SIZE) {
    TUPLETYPE cur = TUPLETYPE(buf+offset);
    if (prev > cur)
      return false;
    prev = cur;
  }

  return true;
}

bool queue_comp(const pair<TUPLETYPE, pair<int, size_t> > &a, const pair<TUPLETYPE, pair<int, size_t> > &b)
{
  return a.first < b.first;
}

void printKey(TUPLETYPE tuple)
{
  for (int j = 0; j < 10; j++)
    printf("%x ", tuple.binary[j]);
  printf("\n");
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
