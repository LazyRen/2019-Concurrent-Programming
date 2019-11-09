#include <iostream>
#include <string>
#include "taskrunner.h"
using namespace std;

int main(int argc, char* argv[])
{
  if (argc < 2) {
    cout << argv[0] << " requires 1 argument" << endl;
    cout << "USAGE: ./" << argv[0] << " N" << endl;
    cout << "N: Number of worker threads" << endl;
    exit(0);
  }

  int total_worker_threads = stoi(argv[1]);

#ifdef VERBOSE
  cout << "Total Worker Threads: " << total_worker_threads << endl;
#endif

  if (total_worker_threads <= 0) {
    cout << "ERROR: program must have positive number of thread\n";
    exit(0);
  }

  TaskRunner<int> runner(total_worker_threads, 60);

  cout << "Total " << runner.Run() << " updates done during " << runner.GetExecutionTime() << " seconds\n";
}
