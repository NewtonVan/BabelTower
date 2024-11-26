#include <thread>

#include <common/utils.hpp>
#include <tpcc/bench.hpp>

// handle exception for exmap
[[maybe_unused]] void handleSEGFAULT(int signo, siginfo_t *info, void *extra) {
  void *page = info->si_addr;
  if (ExecContext::getGlobalContext().bm_.isValidPtr(page)) {
    cerr << "segfault restart "
         << ExecContext::getGlobalContext().bm_.toPID(page) << endl;
    throw OLCRestartException();
  } else {
    cerr << "segfault " << page << endl;
    _exit(1);
  }
}

template <class Fn>
void parallel_for(uint64_t begin, uint64_t end, uint64_t nthreads, Fn fn) {
  std::vector<std::thread> threads;
  uint64_t n = end - begin;
  if (n < nthreads)
    nthreads = n;
  uint64_t perThread = n / nthreads;
  for (unsigned i = 0; i < nthreads; i++) {
    threads.emplace_back([&, i]() {
      uint64_t b = (perThread * i) + begin;
      uint64_t e = (i == (nthreads - 1)) ? end : ((b + perThread) + begin);
      fn(i, b, e);
    });
  }
  for (auto &t : threads)
    t.join();
}

int main(int argc, char **argv) {
  unsigned nthreads = envOr("THREADS", 1);
  u64 n = envOr("DATASIZE", 10);
  u64 runForSec = envOr("RUNFOR", 30);
  bool isRndread = envOr("RNDREAD", 0);

  u64 statDiff = 1e8;
  atomic<u64> txProgress(0);
  atomic<bool> keepRunning(true);
  auto systemName = "vmcache";

  auto statFn = [&]() {
    cout << "ts,tx,rmb,wmb,system,threads,datasize,workload,batch" << endl;
    u64 cnt = 0;
    for (uint64_t i = 0; i < runForSec; i++) {
      sleep(1);
      float rmb = (ExecContext::getGlobalContext().bm_.readCount.exchange(0) *
                   pageSize) /
                  (1024.0 * 1024);
      float wmb = (ExecContext::getGlobalContext().bm_.writeCount.exchange(0) *
                   pageSize) /
                  (1024.0 * 1024);
      u64 prog = txProgress.exchange(0);
      cout << cnt++ << "," << prog << "," << rmb << "," << wmb << ","
           << systemName << "," << nthreads << "," << n << ","
           << (isRndread ? "rndread" : "tpcc") << ","
           << ExecContext::getGlobalContext().bm_.batch << endl;
    }
    keepRunning = false;
  };

  if (isRndread) {
    BTree bt;
    bt.splitOrdered = true;

    {
      // insert
      parallel_for(0, n, nthreads,
                   [&](uint64_t worker, uint64_t begin, uint64_t end) {
                     workerThreadId = worker;
                     array<u8, 120> payload;
                     for (u64 i = begin; i < end; i++) {
                       union {
                         u64 v1;
                         u8 k1[sizeof(u64)];
                       };
                       v1 = __builtin_bswap64(i);
                       memcpy(payload.data(), k1, sizeof(u64));
                       bt.insert({k1, sizeof(KeyType)}, payload);
                     }
                   });
    }
    cerr << "space: "
         << (ExecContext::getGlobalContext().bm_.allocCount.load() * pageSize) /
                (float)ExecContext::getGlobalContext().bm_.gb
         << " GB " << endl;

    ExecContext::getGlobalContext().bm_.readCount = 0;
    ExecContext::getGlobalContext().bm_.writeCount = 0;
    thread statThread(statFn);

    parallel_for(0, nthreads, nthreads,
                 [&](uint64_t worker, uint64_t begin, uint64_t end) {
                   workerThreadId = worker;
                   u64 cnt = 0;
                   u64 start = rdtsc();
                   while (keepRunning.load()) {
                     union {
                       u64 v1;
                       u8 k1[sizeof(u64)];
                     };
                     v1 =
                         __builtin_bswap64(RandomGenerator::getRand<u64>(0, n));

                     array<u8, 120> payload;
                     bool succ = bt.lookup({k1, sizeof(u64)}, [&](span<u8> p) {
                       memcpy(payload.data(), p.data(), p.size());
                     });
                     assert(succ);
                     assert(memcmp(k1, payload.data(), sizeof(u64)) == 0);

                     cnt++;
                     u64 stop = rdtsc();
                     if ((stop - start) > statDiff) {
                       txProgress += cnt;
                       start = stop;
                       cnt = 0;
                     }
                   }
                   txProgress += cnt;
                 });

    statThread.join();
    return 0;
  }

  // TPC-C
  Integer warehouseCount = n;

  vmcacheAdapter<warehouse_t> warehouse;
  vmcacheAdapter<district_t> district;
  vmcacheAdapter<customer_t> customer;
  vmcacheAdapter<customer_wdl_t> customerwdl;
  vmcacheAdapter<history_t> history;
  vmcacheAdapter<neworder_t> neworder;
  vmcacheAdapter<order_t> order;
  vmcacheAdapter<order_wdc_t> order_wdc;
  vmcacheAdapter<orderline_t> orderline;
  vmcacheAdapter<item_t> item;
  vmcacheAdapter<stock_t> stock;

  TPCCWorkload<vmcacheAdapter> tpcc(
      warehouse, district, customer, customerwdl, history, neworder, order,
      order_wdc, orderline, item, stock, true, warehouseCount, true);

  {
    tpcc.loadItem();
    tpcc.loadWarehouse();

    parallel_for(1, warehouseCount + 1, nthreads,
                 [&](uint64_t worker, uint64_t begin, uint64_t end) {
                   workerThreadId = worker;
                   for (Integer w_id = begin; w_id < end; w_id++) {
                     tpcc.loadStock(w_id);
                     tpcc.loadDistrinct(w_id);
                     for (Integer d_id = 1; d_id <= 10; d_id++) {
                       tpcc.loadCustomer(w_id, d_id);
                       tpcc.loadOrders(w_id, d_id);
                     }
                   }
                 });
  }
  cerr << "space: "
       << (ExecContext::getGlobalContext().bm_.allocCount.load() * pageSize) /
              (float)ExecContext::getGlobalContext().bm_.gb
       << " GB " << endl;

  ExecContext::getGlobalContext().bm_.readCount = 0;
  ExecContext::getGlobalContext().bm_.writeCount = 0;
  thread statThread(statFn);

  parallel_for(0, nthreads, nthreads,
               [&](uint64_t worker, uint64_t begin, uint64_t end) {
                 workerThreadId = worker;
                 u64 cnt = 0;
                 u64 start = rdtsc();
                 while (keepRunning.load()) {
                   int w_id = tpcc.urand(1, warehouseCount); // wh crossing
                   tpcc.tx(w_id);
                   cnt++;
                   u64 stop = rdtsc();
                   if ((stop - start) > statDiff) {
                     txProgress += cnt;
                     start = stop;
                     cnt = 0;
                   }
                 }
                 txProgress += cnt;
               });

  statThread.join();
  cerr << "space: "
       << (ExecContext::getGlobalContext().bm_.allocCount.load() * pageSize) /
              (float)ExecContext::getGlobalContext().bm_.gb
       << " GB " << endl;

  return 0;
}