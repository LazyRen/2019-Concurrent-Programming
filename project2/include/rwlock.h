#ifndef _RWLOCK_H_
#define _RWLOCK_H_

#include <algorithm>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <fstream>
#include <iostream>
#include <mutex>
#include <queue>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
using namespace std;

#define IS_WRITING -1

enum LockType {
  WRITER_LOCK,
  READER_LOCK,
  NIL
};

class Lock {
public:
  LockType lock_type;
  int tid;
  int rid;
  bool is_acquired;

  Lock(LockType lock, int thread, int record) :
    lock_type(lock), tid(thread), rid(record), is_acquired(false) {}
};

class Record {
public:
  int64_t data;
  condition_variable_any cv;
  int cur_readers;
  deque<Lock> lock_deque;
  Record() :
    data(100), cur_readers(0) {}
  Record(int64_t data) :
    data(data), cur_readers(0) {}
};

class ThreadInfo {
  public:
  int tid;
  unordered_map<int, Lock*> locks;
  ThreadInfo(int tid) : tid(tid) {}
};

int total_worker_threads, total_records, max_execution_order, global_execution_order = 0;
mutex global_mutex;
Record* records;
vector<thread> threads;
vector<ThreadInfo> thread_infos;

int GetRandomNumber(int maxi);
bool CanWakeUp(int tid, int rid, LockType lock_type);
vector<int> GetWaitingList(int tid, int rid);
bool DeadlockCheck(int tid);
bool AcquireReadLock(int tid, int rid);
void ReleaseReadLock(int tid, int rid);
bool AcquireWriteLock(int tid, int rid);
void ReleaseWriteLock(int tid, int rid);
void ThreadFunc(int tid);
#endif
