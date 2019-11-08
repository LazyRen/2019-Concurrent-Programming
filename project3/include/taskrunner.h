#ifndef _TASKRUNNER_H_
#define _TASKRUNNER_H_

#include <chrono>
#include <climits>
#include <random>
#include <thread>
#include "snapshot.h"
using namespace std;

template <typename T>
class TaskRunner {
private:
  vector<thread> thread_pool;
  WFSnapshot<T> *snapshot;
  bool is_time_exceeded;
  int *update_counter;
  int execution_time;

public:
  TaskRunner(int thread_num, int time)
    : thread_pool(thread_num), snapshot(new WFSnapshot<T>(thread_num)),
      is_time_exceeded(false), update_counter(new int[thread_num]), execution_time(time) {};

  ~TaskRunner()
  {
    delete snapshot;
    delete[] update_counter;
  }

  int GetExecutionTime()
  {
    return execution_time;
  }

  T GetRandomNumber()
  {
    random_device rd;
    mt19937_64 rng(rd());
    uniform_int_distribution<T> dist(numeric_limits<T>::min(), numeric_limits<T>::max());

    return dist(rng);
  }

  int Run()
  {
    for (int i = 0; i < thread_pool.size(); i++)
      thread_pool[i] = thread(&TaskRunner::SpinUpdate, this, i);

    this_thread::sleep_for(chrono::seconds(execution_time));
    is_time_exceeded = true;

    for (int i = 0; i < thread_pool.size(); i++)
      thread_pool[i].join();

    int total_updates = 0;
    for (int i = 0; i < thread_pool.size(); i++)
      total_updates += update_counter[i];

    return total_updates;
  }

  void SpinUpdate(int tid)
  {
    int update_cnt = 0;

    while (!is_time_exceeded) {
      T new_val = GetRandomNumber();
      snapshot->update(new_val, tid);
      update_cnt++;
    }

    update_counter[tid] = update_cnt;
  }
};

#endif
