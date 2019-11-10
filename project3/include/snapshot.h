#ifndef _SNAPSHOT_H_
#define _SNAPSHOT_H_

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <vector>
using namespace std;

typedef long long int ll;

template <typename T>
class GCObject {
private:
  ll epoch;
  T *snapshot;

public:
  GCObject<T> *next;

  GCObject(ll _epoch = 0, T* _snapshot = nullptr)
    : epoch(_epoch), snapshot(_snapshot), next(nullptr) {}

  ~GCObject()
  {
    delete[] snapshot;
  }

  ll GetEpoch() const
  {
    return epoch;
  }
};

template <typename T>
class GarbageCollector {
public:
  ll global_epoch_counter;
  vector<ll> thread_epoch;
  ll salvaged;
  atomic<GCObject<T>*> garbage_list;
  bool is_time_exceeded;

  GarbageCollector(int thread_num)
    : thread_epoch(thread_num), salvaged(0), garbage_list(nullptr), is_time_exceeded(false) {}

  void AddGarbage(ll epoch, T* snapshot)
  {
    GCObject<T> *garbage = new GCObject<T> (epoch, snapshot);
    garbage->next = garbage_list.load();

    while (true) {
      bool ret = garbage_list.compare_exchange_strong(garbage->next, garbage);

      if (ret)
        break;
    }
  }

  void SalvageGarbage(bool force_salvage = false)
  {
    GCObject<T> *prev, *cur, *next;
    while (!is_time_exceeded) {
      prev = garbage_list.load();
      if (prev) {
        cur = prev->next;
      } else {
        continue;
      }
      while (cur) {
        next = cur->next;
        if (cur->GetEpoch() < *min_element(thread_epoch.begin(), thread_epoch.end())) {
          prev->next = next;
          delete cur;
          salvaged++;
        } else {
          prev = cur;
        }
        cur = next;
      }
      this_thread::sleep_for(chrono::milliseconds(100));
    }

    cur = garbage_list.load();
    while (cur) {
      next = cur->next;
      delete cur;
      cur = next;
      salvaged++;
    }
    return;
  }
};

template <typename T>
class StampedData {
private:
  T value;
  T *snapshot;
  ll stamp;

public:
  StampedData(T _value = 0, ll _stamp = 0, T* _snap = nullptr)
    : value(_value), snapshot(_snap), stamp(_stamp) {}

  StampedData(const StampedData& other)
    : value(other.value), snapshot(other.snapshot), stamp(other.stamp) {}

  T Read() const
  {
    return value;
  }

  void update(T _value, T* _snap, ll _stamp)
  {
    snapshot = _snap;
    stamp    = _stamp;
    value    = _value;
  }

  T* GetSnapshot() const
  {
    return snapshot;
  }

  ll GetStamp() const
  {
    return stamp;
  }
};

template <typename T>
class WFSnapshot {
private:
  vector<StampedData<T>> registers;

  void collect(vector<StampedData<T>> &snapshot)
  {
    for (int i = 0; i < registers.size(); i++)
      snapshot[i] = registers[i];
  }

public:
  GarbageCollector<T> garbage_collector;

  WFSnapshot(int size)
    : registers(size), garbage_collector(size) {}

  void update(T value, int tid)
  {
    // set to true iff taken snapshot from scan() is from other thread's update.
    bool stolen_snapshot = false;
    T *taken_snapshot = scan(stolen_snapshot);
    if (stolen_snapshot)
      taken_snapshot[registers.size()] = -1;
    else
      taken_snapshot[registers.size()] = tid;

    StampedData<T> old_reg = registers[tid];

    garbage_collector.thread_epoch[tid] = garbage_collector.global_epoch_counter;

    if (old_reg.GetSnapshot() && old_reg.GetSnapshot()[registers.size()] == tid)
      garbage_collector.AddGarbage(garbage_collector.thread_epoch[tid], old_reg.GetSnapshot());

    registers[tid].update(value, taken_snapshot, old_reg.GetStamp() + 1);
  }

  T* scan(bool &stolen_snapshot)
  {
    vector<StampedData<T>> old_snapshot (registers.size());
    vector<StampedData<T>> new_snapshot (registers.size());
    vector<bool> is_modified(registers.size(), false);

    collect(old_snapshot);

    while (true) {
      bool is_equal = true;
      collect(new_snapshot);

      for (int i = 0; i < registers.size(); i++) {
        if (old_snapshot[i].GetStamp() != new_snapshot[i].GetStamp()) {
          if (is_modified[i]) {
            stolen_snapshot = true;
            return old_snapshot[i].GetSnapshot();
          } else {
            is_equal = false;
            is_modified[i] = true;
            old_snapshot = new_snapshot;
            break;
          }
        }
      }

      if (is_equal)
        break;
    }

    T *result = new T[registers.size() + 1];
    for (int i = 0; i < registers.size(); i++)
      result[i] = new_snapshot[i].Read();

    return result;
  }
};

#endif
