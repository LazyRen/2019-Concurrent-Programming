#include <iostream>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/thread.hpp>
using namespace std;

#define NUM_THREAD_IN_POOL  10

boost::asio::io_service print_io;
boost::thread_group print_thread;
int tried;

bool IsPrime(int n)
{
    if (n < 2) {
        return false;
    }

    for (int i = 3; i <= sqrt(n); i += 2) {
        if (n % i == 0) {
            return false;
        }
    }
    return true;
}

void PrintResult(int sequence_number, int start, int end, int primes)
{
  cout << "(" << sequence_number << ")number of primes in " << start << " ~ " << end << " is " << primes << endl;
  tried++;
}

void ThreadFunc(int sequence_number, int start, int end)
{

  long cnt_prime = 0;
  for (int i = start; i < end; i++) {
    if (IsPrime(i)) {
      cnt_prime++;
    }
  }

  print_io.post(boost::bind(PrintResult, sequence_number, start, end, cnt_prime));
  return;
}

int main()
{
  boost::asio::io_service io;
  boost::thread_group threadpool;
  boost::asio::io_service::work* work = new boost::asio::io_service::work(io);
  boost::asio::io_service::work* print_work = new boost::asio::io_service::work(print_io);

  int range_start;
  int range_end;
  int sequence_number = 0;

  for (int i = 0; i < NUM_THREAD_IN_POOL; i++) {
    threadpool.create_thread(boost::bind(&boost::asio::io_service::run, &io));
  }
  print_thread.create_thread(boost::bind(&boost::asio::io_service::run, &print_io));

  while (1) {
    cin >> range_start;
    if (range_start == -1)
      break;
    cin >> range_end;

    io.post(boost::bind(ThreadFunc, sequence_number, range_start, range_end));
    sequence_number++;
  }

  // cout << sequence_number << "/" << tried << endl;
  delete work;
  threadpool.join_all();
  delete print_work;
  print_thread.join_all();
  io.stop();
  print_io.stop();
}
