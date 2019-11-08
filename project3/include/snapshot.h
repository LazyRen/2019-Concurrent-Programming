#ifndef _SNAPSHOT_H_
#define _SNAPSHOT_H_

#include <memory>
#include <vector>
using namespace std;

typedef long long int ll;

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
    : value(other.value), snapshot(nullptr), stamp(0) {}

  T Read() const
  {
    return value;
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

  StampedData<T>* collect()
  {
    StampedData<T> *copy = new StampedData<T>[registers.size()];

    for (int i = 0; i < registers.size(); i++)
      copy[i] = registers[i];

    return copy;
  }

public:
  WFSnapshot(int n)
    : registers(n) {}

  void update(T value, int tid)
  {
    T *prev_snapshot = scan();

    StampedData<T> old_value = registers[tid];
    registers[tid] = StampedData<T>(value, old_value.GetStamp() + 1, prev_snapshot);
  }

  T* scan()
  {
    StampedData<T> *old_snapshot;
    StampedData<T> *new_snapshot;
    vector<bool> is_modified(registers.size(), false);

    old_snapshot = collect();

    while (true) {
      bool is_equal = true;
      new_snapshot = collect();

      for (int i = 0; i < registers.size(); i++) {
        if (old_snapshot[i].GetStamp() != new_snapshot[i].GetStamp()) {
            if (is_modified[i]) {
              return old_snapshot[i].GetSnapshot();
            } else {
              is_equal = false;
              is_modified[i] = true;
              delete old_snapshot;
              old_snapshot = new_snapshot;
              break;
          }
        }
      }

      if (is_equal)
        break;
    }

    T *result = new T[registers.size()];

    for (int i = 0; i < registers.size(); i++)
      result[i] = new_snapshot[i].Read();

    // delete new_snapshot;

    return result;
  }
};

#endif
