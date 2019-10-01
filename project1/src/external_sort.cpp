#include "external_sort.h"
#include "kxsort.h"

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
  chunk_per_file   = (file_size / total_file);
  chunk_per_thread = chunk_per_file / MAX_THREADS;
  int tmp_fd[total_file - 1];
  future<size_t> tmp_write[total_file - 1];
  TUPLETYPE *read_buf[total_file - 1][2];

#ifdef VERBOSE
  printf("file_size: %zu total_tuples: %zu key_per_thread: %zu\n", file_size, total_tuples, chunk_per_thread / TUPLE_SIZE);
  auto startTime = high_resolution_clock::now();
#endif

  // read data from input file & start sorting
  if (total_file == 1) {// input <= 1GB : inmemory sorting & direct writing
    tuples = new TUPLETYPE[chunk_per_file/TUPLE_SIZE];

    #pragma omp parallel for num_threads(MAX_THREADS)
    for (size_t i = 0; i < MAX_THREADS; i++)
      readFromFile(input_fd, &tuples[i*chunk_per_thread/TUPLE_SIZE], chunk_per_thread, i*chunk_per_thread);

    parallelSort(tuples, total_tuples);
  } else if (total_file == 2) {
    tuples = new TUPLETYPE[file_size/TUPLE_SIZE];
    chunk_per_thread = file_size / MAX_THREADS;

    #pragma omp parallel for num_threads(MAX_THREADS)
    for (size_t i = 0; i < MAX_THREADS; i++)
      readFromFile(input_fd, &tuples[i*chunk_per_thread/TUPLE_SIZE], chunk_per_thread, i*chunk_per_thread);

    parallelSort(tuples, total_tuples);
  } else {// total_file > 2 : create .tmp files
    tuples = new TUPLETYPE[chunk_per_file/TUPLE_SIZE];
    for (int i = 0; i < total_file - 1; i++)
      for (int j = 0; j < 2; j++)
        read_buf[i][j] = new TUPLETYPE[BUFFER_SIZE/TUPLE_SIZE];

    for (int cur_file = 0; cur_file < total_file; cur_file++) {
      #pragma omp parallel for num_threads(MAX_THREADS)
      for (size_t i = 0; i < MAX_THREADS; i++)
        readFromFile(input_fd, &tuples[i*chunk_per_thread/TUPLE_SIZE], chunk_per_thread, (chunk_per_file*cur_file) + (i*chunk_per_thread));

      if (cur_file != total_file - 1) {// create total_file - 1 .tmp files. leave 1 in memory.
        string outfile = to_string(cur_file) + ".tmp";
        tmp_fd[cur_file] = open(outfile.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0777);
        if (tmp_fd[cur_file] == -1) {
          printf("error: open %s file\n", outfile.c_str());
          exit(0);
        }

        tmp_files.push_back(FILEINFO(outfile, 0, chunk_per_file - 2*BUFFER_SIZE));

        parallelSort(tuples, chunk_per_file/TUPLE_SIZE);

        memcpy(read_buf[cur_file][0], tuples, BUFFER_SIZE);
        memcpy(read_buf[cur_file][1], tuples + BUFFER_SIZE/TUPLE_SIZE, BUFFER_SIZE);
        posix_fallocate(tmp_fd[cur_file], 0, chunk_per_file);
        writeToFile(tmp_fd[cur_file], tuples + 2*BUFFER_SIZE/TUPLE_SIZE, chunk_per_file - 2*BUFFER_SIZE, 0);
        close(tmp_fd[cur_file]);
      }
      parallelSort(tuples, chunk_per_file/TUPLE_SIZE);
    }
  }

#ifdef VERBOSE
  auto stopTime = high_resolution_clock::now();
  auto duration = duration_cast<milliseconds>(stopTime - startTime);
  cout << "read & sort took " << duration.count() << "ms\n";
#endif

  // open output file.
  int output_fd = open(argv[2], O_WRONLY | O_CREAT | O_TRUNC, 0777);
  if (output_fd == -1) {
    printf("error: open output file\n");
    exit(0);
  }
  posix_fallocate(output_fd, 0, file_size);

  // flush to output file.
  if (total_file <= 2) {
    writeToFile(output_fd, tuples, file_size, 0);
  } else {
    externalSort(output_fd, read_buf);
    for (int i = 0; i < total_file - 1; i++) {
      for (int j = 0; j < 2; j++)
        delete[] read_buf[i][j];
    }
  }

#ifdef VERBOSE
  auto stopTime2 = high_resolution_clock::now();
  duration = duration_cast<milliseconds>(stopTime2 - stopTime);
  cout << "file writing took " << duration.count() << "ms\n";
#endif

  // free
  delete [] tuples;
  // close input file.
  close(input_fd);
  // close output file.
  close(output_fd);

  return 0;
}

void parallelSort(TUPLETYPE* tuples, size_t count)
{
  kx::radix_sort(tuples, tuples+count, OneByteRadixTraits());
  size_t partition[MAX_THREADS];

  #pragma omp parallel for num_threads(MAX_THREADS)
  for (int i = 1; i < MAX_THREADS; i++) {
    unsigned char key[100];
    memset(key, 0, sizeof(key));
    key[0] = i * 256/MAX_THREADS;
    TUPLETYPE tmp(key);
    auto idx = lower_bound(tuples, tuples + count, tmp);
    partition[i-1] = idx - tuples;
  }
  partition[MAX_THREADS-1] = count;

  #pragma omp parallel for num_threads(MAX_THREADS)
  for (int i = 0; i < MAX_THREADS; i++) {
    size_t prev = i == 0 ? 0 : partition[i-1];
    kx::radix_sort(tuples+prev, tuples+partition[i], RadixTraits());
  }
}

void externalSort(int output_fd, TUPLETYPE* read_buf[][2])
{
  int written_files = total_file - 1;
  bool is_done[written_files];
  int tmp_fd[written_files], tmp_swi[written_files];
  size_t total_write = 0, write_idx = 0, write_swi = 0;
  future<size_t> read[written_files], write;
  priority_queue<pair<TUPLETYPE*, pair<int, size_t> >, vector<pair<TUPLETYPE*, pair<int, size_t> > >, pq_cmp > queue;// {TUPLE, {file_idx, buf_idx}}
  bool is_first_writing = true, is_first_reading[written_files];
  TUPLETYPE *write_buf[2];

  for (int i = 0; i < 2; i++)
    write_buf[i] = new TUPLETYPE[W_BUFFER_SIZE/TUPLE_SIZE];

  // open created .tmp files
  for (int i = 0; i < written_files; i++) {
    is_done[i] = false;
    is_first_reading[i] = true;
    tmp_swi[i] = 0;
    tmp_fd[i]  = open(tmp_files[i].file_name.c_str(), O_RDONLY);
    if (tmp_fd[i] == -1) {
      printf("error: open %s file\n", tmp_files[i].file_name.c_str());
      exit(0);
    }
  }

  for (int i = 0; i < written_files; i++)
    queue.push({&read_buf[i][0][0], {i, 0}});
  queue.push({&tuples[0], {written_files, 0}});

  // sort external files by popping least key TUPLE from queue & write to output file
  while (!queue.empty()) {
    auto min_tuple = queue.top(); queue.pop();
    int f_idx = min_tuple.second.first; size_t offset = min_tuple.second.second;
    write_buf[write_swi][write_idx/TUPLE_SIZE] = *min_tuple.first;
    write_idx += TUPLE_SIZE;

    // write buffer is full. Flush to disk.
    if (write_idx >= W_BUFFER_SIZE/TUPLE_SIZE) {
      if (is_first_writing)
        is_first_writing = false;
      else
        total_write += write.get();

      if (total_write + write_idx < total_tuples * TUPLE_SIZE)
        write = async(writeToFile, output_fd, write_buf[write_swi], write_idx, total_write);
      else
        total_write += writeToFile(output_fd, write_buf[write_swi], write_idx, total_write);
      write_idx = 0;
      write_swi = (write_swi+1) % 2;
    }

    // push next tuple of current poped one.
    if (f_idx == written_files) {
      if (offset + TUPLE_SIZE < chunk_per_file) {
        queue.push({&tuples[offset/TUPLE_SIZE + 1], {f_idx, offset + TUPLE_SIZE}});
      }
    } else {
      if (offset + TUPLE_SIZE >= BUFFER_SIZE) {
        if (!is_done[f_idx]) {
          if (is_first_reading[f_idx])
            is_first_reading[f_idx] = false;
          else
            tmp_files[f_idx].cur_offset += read[f_idx].get();

          read[f_idx] = async(readFromFile, tmp_fd[f_idx], read_buf[f_idx][tmp_swi[f_idx]],
                              BUFFER_SIZE, tmp_files[f_idx].cur_offset);
          tmp_swi[f_idx] = (tmp_swi[f_idx]+1) % 2;
          queue.push({&read_buf[f_idx][tmp_swi[f_idx]][0], {f_idx, 0}});

          if (tmp_files[f_idx].cur_offset + BUFFER_SIZE >= tmp_files[f_idx].size)
            is_done[f_idx] = true;
        } else {
          if (tmp_files[f_idx].cur_offset < tmp_files[f_idx].size) {
            tmp_files[f_idx].cur_offset += read[f_idx].get();
            tmp_swi[f_idx] = (tmp_swi[f_idx]+1) % 2;
            queue.push({&read_buf[f_idx][tmp_swi[f_idx]][0], {f_idx, 0}});
          }
        }
      } else {
        queue.push({&read_buf[f_idx][tmp_swi[f_idx]][offset/TUPLE_SIZE+1], {f_idx, offset+TUPLE_SIZE}});
      }
    }
  }

  for (int i = 0; i < 2; i++)
    delete[] write_buf[i];

  for (int i = 0; i < written_files; i++)
    close(tmp_fd[i]);
}

size_t readFromFile(int fd, void *buf, size_t nbyte, size_t offset)
{
  posix_fadvise(fd, offset, nbyte, POSIX_FADV_WILLNEED);
  posix_fadvise(fd, offset, nbyte, POSIX_FADV_SEQUENTIAL);
  size_t total_read = 0;

  while (nbyte) {
    size_t ret  = pread(fd, buf, nbyte, offset);
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
    size_t ret   = pwrite(fd, buf, nbyte, offset);
    nbyte       -= ret;
    offset      += ret;
    total_write += ret;
  }

  return total_write;
}

int keycmp(const void* ptr1, const void* ptr2, size_t count)
{
  register const unsigned char *s1 = (const unsigned char*)ptr1;
  register const unsigned char *s2 = (const unsigned char*)ptr2;

  while (count-- > 0)
    {
      if (*s1++ != *s2++)
	  return s1[-1] < s2[-1] ? -1 : 1;
    }
  return 0;
}

void printKey(TUPLETYPE tuple)
{
  for (int j = 0; j < 10; j++)
    printf("%x ", tuple.binary[j]);
  printf("\n");
}
