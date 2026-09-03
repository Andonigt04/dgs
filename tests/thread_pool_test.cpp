// ─────────────────────────────────────────────────────────────────────────────────────────────────
// ⚠️ This used to not be a test: it enqueued 10 tasks that printed, slept 100 ms and did `return 0`.
// If the pool had dropped EVERY task it would still have come out green. Which is why in CI it lived
// as `timeout 5 ./thread_pool_test || true` — a verdict nobody looks at.
//
// What is checked now, and why it matters:
//   · that the DESTRUCTOR DRAINS the queue. `Logger` enqueues every line into a 1-thread pool and never
//     flushes explicitly: if destroying the pool dropped what was pending, the server would lose the
//     last log entries exactly as it shuts down, which is when they matter most.
//   · that the work is SPREAD. With a single thread doing everything this is not a pool, and a test
//     that only counted finished tasks would not notice the difference.
// No guessed sleeps: everything is measured after destroying the pool, a real synchronisation point.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/thread_pool.h"

#include <atomic>
#include <cstdio>
#include <mutex>
#include <set>
#include <thread>

static int g_pass = 0, g_fail = 0;
static void check(bool ok, const char* msg)
{
    if (ok) { ++g_pass; std::printf("  [ok]   %s\n", msg); }
    else    { ++g_fail; std::printf("  [FAIL] %s\n", msg); }
}

int main()
{
    constexpr int kWorkers = 4;
    constexpr int kTasks   = 500;      // enough that the spread does not depend on luck

    std::atomic<int> done{0};
    std::mutex m;
    std::set<std::thread::id> threads;

    {
        DGS::ThreadPool pool(kWorkers);
        for (int i = 0; i < kTasks; ++i) {
            pool.enqueue([&]() {
                {
                    std::lock_guard<std::mutex> g(m);
                    threads.insert(std::this_thread::get_id());
                }
                done.fetch_add(1, std::memory_order_relaxed);
            });
        }
    }   // <- the destructor is the synchronisation point: nothing can still be pending here

    check(done.load() == kTasks,
          "the destructor DRAINS the queue: all 500 enqueued tasks run");
    if (done.load() != kTasks)
        std::printf("         (%d of %d ran)\n", done.load(), kTasks);

    check(threads.size() > 1,
          "the work is spread across several threads (with only one it would not be a pool)");
    check((int)threads.size() <= kWorkers,
          "no more threads work than were requested at construction");
    std::printf("  threads that took part: %zu of %d\n", threads.size(), kWorkers);

    std::printf("\n== thread_pool: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
