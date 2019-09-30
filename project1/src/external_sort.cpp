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
  tuples           = new TUPLETYPE[chunk_per_file/TUPLE_SIZE];
  int tmp_fd[total_file - 1];

#ifdef VERBOSE
  printf("file_size: %zu total_tuples: %zu key_per_thread: %zu\n", file_size, total_tuples, chunk_per_thread / TUPLE_SIZE);
  printf("total_tmp_files: %d, chunk_per_file: %zu\n", total_file, chunk_per_file);
  printf("tuple array contains %zu keys\n", chunk_per_file/TUPLE_SIZE);
  auto startTime = high_resolution_clock::now();
#endif

  // read data from input file & start sorting
  if (total_file == 1) {// input <= 1GB : inmemory sorting & direct writing
    #pragma omp parallel for num_threads(MAX_THREADS)
    for (size_t i = 0; i < MAX_THREADS; i++)
      readFromFile(input_fd, &tuples[i*chunk_per_thread/TUPLE_SIZE], chunk_per_thread, i*chunk_per_thread);

    parallelSort(tuples, total_tuples);
  } else {// total_file > 1 : create .tmp files
    for (int cur_file = 0; cur_file < total_file; cur_file++) {
      #pragma omp parallel for num_threads(MAX_THREADS)
      for (size_t i = 0; i < MAX_THREADS; i++)
        readFromFile(input_fd, &tuples[i*chunk_per_thread/TUPLE_SIZE], chunk_per_thread, (chunk_per_file*cur_file) + (i*chunk_per_thread));

      parallelSort(tuples, chunk_per_file/TUPLE_SIZE);
      if (cur_file != total_file - 1) {// create total_file - 1 .tmp files. leave 1 in memory.
        string outfile = to_string(cur_file) + ".tmp";
        tmp_fd[cur_file] = open(outfile.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0777);
        if (tmp_fd[cur_file] == -1) {
          printf("error: open %s file\n", outfile.c_str());
          exit(0);
        }

        size_t to_write = file_size / total_file;
        tmp_files.push_back(FILEINFO(outfile, 0, to_write));
        writeToFile(tmp_fd[cur_file], tuples, to_write, 0);
      }
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
  size_t ret = ftruncate(output_fd, file_size);
  if (ret == static_cast<size_t>(-1)) {
    printf("error: resizing output file to %zu failed\n", file_size);
    exit(0);
  }

  // flush to output file.
  if (total_file == 1) {
    writeToFile(output_fd, tuples, file_size, 0);
  } else {
    externalSort(output_fd);
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
    auto idx = lower_bound(tuples, tuples+count, tmp);
    partition[i-1] = idx - tuples;
  }
  partition[MAX_THREADS-1] = count;

  #pragma omp parallel for num_threads(MAX_THREADS)
  for (int i = 0; i < MAX_THREADS; i++) {
    size_t prev = i == 0 ? 0 : partition[i-1];

    kx::radix_sort(tuples+prev, tuples+partition[i], RadixTraits());
  }

}

void externalSort(int output_fd)
{
  int written_files = total_file - 1;
  bool is_done[written_files];
  int tmp_fd[written_files], tmp_swi[written_files];
  size_t total_write = 0, write_idx = 0, write_swi = 0;
  future<size_t> read[written_files], write;
  priority_queue<pair<TUPLETYPE*, pair<int, size_t> >, vector<pair<TUPLETYPE*, pair<int, size_t> > >, pq_cmp > queue;// {TUPLE, {file_idx, buf_idx}}
  TUPLETYPE *tmp_tuples[written_files][2];
  TUPLETYPE *write_buf[2];
  bool is_first_writing = true;

  for (int i = 0; i < written_files; i++) {
    for (int j = 0; j < 2; j++)
      tmp_tuples[i][j] = new TUPLETYPE[BUFFER_SIZE/TUPLE_SIZE];
  }

  for (int i = 0; i < 2; i++)
    write_buf[i] = new TUPLETYPE[W_BUFFER_SIZE/TUPLE_SIZE];

  // open created .tmp files
  for (int i = 0; i < written_files; i++) {
    is_done[i] = false;
    tmp_swi[i] = 0;
    tmp_fd[i]  = open(tmp_files[i].file_name.c_str(), O_RDONLY);
    if (tmp_fd[i] == -1) {
      printf("error: open %s file\n", tmp_files[i].file_name.c_str());
      exit(0);
    }
    read[i] = async(readFromFile, tmp_fd[i], tmp_tuples[i][0], BUFFER_SIZE, 0);
  }

  // fill double buffer for .tmp files
  for (int i = 0; i < written_files; i++) {
    tmp_files[i].cur_offset += read[i].get();
    queue.push({&tmp_tuples[i][0][0], {i, 0}});
    read[i] = async(readFromFile, tmp_fd[i], tmp_tuples[i][1], BUFFER_SIZE, tmp_files[i].cur_offset);
  }
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
          tmp_files[f_idx].cur_offset += read[f_idx].get();

          read[f_idx] = async(readFromFile, tmp_fd[f_idx], tmp_tuples[f_idx][tmp_swi[f_idx]],
                              BUFFER_SIZE, tmp_files[f_idx].cur_offset);
          tmp_swi[f_idx] = (tmp_swi[f_idx]+1) % 2;
          queue.push({&tmp_tuples[f_idx][tmp_swi[f_idx]][0], {f_idx, 0}});

          if (tmp_files[f_idx].cur_offset + BUFFER_SIZE >= tmp_files[f_idx].size)
            is_done[f_idx] = true;
        } else {
          if (tmp_files[f_idx].cur_offset < tmp_files[f_idx].size) {
            tmp_files[f_idx].cur_offset += read[f_idx].get();
            tmp_swi[f_idx] = (tmp_swi[f_idx]+1) % 2;
            queue.push({&tmp_tuples[f_idx][tmp_swi[f_idx]][0], {f_idx, 0}});
          }
        }
      } else {
        queue.push({&tmp_tuples[f_idx][tmp_swi[f_idx]][offset/TUPLE_SIZE+1], {f_idx, offset+TUPLE_SIZE}});
      }
    }
  }

  for (int i = 0; i < written_files; i++) {
    for (int j = 0; j < 2; j++)
      delete[] tmp_tuples[i][j];
  }
  for (int i = 0; i < 2; i++)
    delete[] write_buf[i];
}


size_t readFromFile(int fd, void *buf, size_t nbyte, size_t offset)
{
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

void printKey(TUPLETYPE tuple)
{
  for (int j = 0; j < 10; j++)
    printf("%x ", tuple.binary[j]);
  printf("\n");
}
