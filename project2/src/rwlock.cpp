#include "rwlock.h"

int main(int argc, char* argv[])
{
  ios::sync_with_stdio(false);

  if (argc != 4) {
    cout << argv[0] << " requires 3 arguments" << endl;
    cout << "USAGE: ./" << argv[0] << " N R E" << endl;
    cout << "N: Number of worker threads" << endl;
    cout << "R: Number of records" << endl;
    cout << "E: Last global execution order which threads will be used to decide termination" << endl;
    exit(0);
  }


  total_worker_threads = stoi(argv[1]);
  total_records        = stoi(argv[2]);
  max_execution_order  = stoi(argv[3]);
#ifdef VERBOSE
  cout << "total_worker_threads: " << total_worker_threads << endl;
  cout << "total_records: " << total_records << endl;
  cout << "max_execution_order: " << max_execution_order << endl;
#endif
  records = new Record[total_records + 1];
  thread_infos.emplace_back(-1);
  for (int i = 1; i <= total_worker_threads; i++) {
    thread_infos.emplace_back(i);
    threads.push_back(thread(ThreadFunc, i));
  }

  for (int i = 0; i < total_worker_threads; i++)
    threads[i].join();
}

int GetRandomNumber(int maxi)
{
  random_device rd;
  mt19937_64 rng(rd());
  uniform_int_distribution<int> dist(1, maxi);

  return dist(rng);
}

bool CanWakeUp(int tid, int rid, LockType lock_type)
{
  switch (lock_type) {
    case READER_LOCK:
      for (auto it : records[rid].lock_deque) {
        if (it.tid == tid)
          return true;
        if (it.lock_type == WRITER_LOCK)
          return false;
      }
      break;
    case WRITER_LOCK:
      if (records[rid].lock_deque.front().tid == tid)
        return true;
      break;
    default:
      cout << "ERROR: CanWakeUp() - Failed to identify lock_type\n";
      exit(0);
      break;
  }

  return false;
}

void AcquireReadLock(int tid, int rid)
{
#ifdef VERBOSE
  cout << tid << " : trying to acquire " << rid << "'s READ lock\n";
#endif
  records[rid].lock_deque.emplace_back(READER_LOCK, tid, rid);
  auto cur_obj = records[rid].lock_deque.back();
  thread_infos[tid].locks[rid] = &cur_obj;

  bool should_wait = false;
  for (auto it : records[rid].lock_deque) {
    if (it.lock_type == WRITER_LOCK) {
      should_wait = true;
      break;
    }
  }

  if (should_wait) {
#ifdef VERBOSE
    cout << tid << " : waiting for " << rid << "'s READ lock\n";
#endif
    // TODO: DEADLOCK CHECK
    while (!CanWakeUp(tid, rid, READER_LOCK))
      records[rid].cv.wait(global_mutex);
  }

#ifdef VERBOSE
  cout << tid << " : acquired " << rid << "'s READ lock\n";
#endif
  cur_obj.is_acquired = true;
  records[rid].cur_readers++;
}

void ReleaseReadLock(int tid, int rid)
{
#ifdef VERBOSE
  cout << tid << " : releasing " << rid << "'s READ lock\n";
#endif
  for (auto it = records[rid].lock_deque.begin(); it != records[rid].lock_deque.end(); it++) {
    if (it->tid == tid) {
      records[rid].lock_deque.erase(it);
      break;
    }
  }
  thread_infos[tid].locks.erase(rid);

  records[rid].cur_readers--;
  if (records[rid].cur_readers == 0)
    records[rid].cv.notify_all();
}

void AcquireWriteLock(int tid, int rid)
{
#ifdef VERBOSE
  cout << tid << " : trying to acquire " << rid << "'s WRITE lock\n";
#endif
  records[rid].lock_deque.emplace_back(WRITER_LOCK, tid, rid);
  auto cur_obj = records[rid].lock_deque.back();
  thread_infos[tid].locks[rid] = &cur_obj;

  if (records[rid].lock_deque.size() != 1) {// wait to acquire the write lock
#ifdef VERBOSE
    cout << tid << " : waiting for " << rid << "'s WRITE lock\n";
#endif
    // TODO: DEADLOCK CHECK
    while (!CanWakeUp(tid, rid, WRITER_LOCK))
      records[rid].cv.wait(global_mutex);
  }

#ifdef VERBOSE
  cout << tid << " : acquired " << rid << "'s WRITE lock\n";
#endif
  cur_obj.is_acquired = true;
  records[rid].cur_readers = -1;
}

void ReleaseWriteLock(int tid, int rid)
{
#ifdef VERBOSE
  cout << tid << " : releasing " << rid << "'s WRITE lock\n";
#endif
  for (auto it = records[rid].lock_deque.begin(); it != records[rid].lock_deque.end(); it++) {
    if (it->tid == tid) {
      records[rid].lock_deque.erase(it);
      break;
    }
  }
  thread_infos[tid].locks.erase(rid);

  records[rid].cur_readers = 0;
  records[rid].cv.notify_all();
}

void ThreadFunc(int tid)
{
  int commit_id = 0;
  string out_file_name = "thread" + to_string(tid) + ".txt";
  fstream out_file (out_file_name, fstream::out | fstream::trunc);
  if (!out_file.is_open()) {
    cout << "ERROR: failed to open " << out_file_name << endl;
    exit(0);
  }
  while (commit_id <= max_execution_order) {
    int i = GetRandomNumber(total_records);
    int j = GetRandomNumber(total_records);
    int k = GetRandomNumber(total_records);

#ifdef VERBOSE
    cout << tid << " : i(" << i << ") j(" << j << ") k(" << k << ")\n";
#endif

    // Task 1 ~ 5
    global_mutex.lock();
    AcquireReadLock(tid, i);
    global_mutex.unlock();
    int64_t record_i = records[i].data;

    // Task 6 ~ 9
    global_mutex.lock();
    AcquireWriteLock(tid, j);
    global_mutex.unlock();
    records[j].data += record_i + 1;
    int64_t record_j = records[j].data;

    // Task 10 ~ 13
    global_mutex.lock();
    AcquireWriteLock(tid, k);
    global_mutex.unlock();
    records[k].data -= record_i;
    int64_t record_k = records[k].data;

    // Task 14 ~ 17
    global_mutex.lock();
    ReleaseReadLock(tid, i);
    ReleaseWriteLock(tid, j);
    ReleaseWriteLock(tid, k);
    global_execution_order += 1;
    commit_id = global_execution_order;
#ifdef VERBOSE
      cout << tid << " : commit_id(" << commit_id << ")\n";
#endif
    if (commit_id > max_execution_order) {
#ifdef VERBOSE
      cout << tid << " : undo transaction\n";
#endif
      // TODO: UNDO transaction
    } else {
      out_file << commit_id << " " << i << " " << j << " " << k << " ";
      out_file << record_i << " " << record_j << " " << record_k << "\n";
    }
    global_mutex.unlock();
  }

}
