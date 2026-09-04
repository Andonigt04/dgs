// ─────────────────────────────────────────────────────────────────────────────────────────────────
// ⚠️ This used to not be a test: it enqueued 10 tasks that printed, slept 100 ms and did `return 0`.
// If the pool had dropped EVERY task it would still have come out green. Which is why in CI it lived
// as `timeout 5 ./thread_pool_test || true` — a verdict nobody looks at.
//
// What is checked now, and why it matters:
//   · that the DESTRUCTOR DRAINS the queue. `Logger` enqueues every line into a 1-thread pool and never
//     flushes explicitly: if destroying the pool dropped what was pending, the server would lose the
//     last log entries exactly as it shuts down, which is when they matter most.
//   · that the workers run CONCURRENTLY. With a single thread doing everything this is not a pool, and
//     a test that only counted finished tasks would not notice the difference.
// No guessed sleeps: everything is measured after destroying the pool, a real synchronisation point.
//
// ⚠️ THE CONCURRENCY CHECK USED TO BE A COIN FLIP. It ran 500 trivial tasks and asserted that more
// than one thread id had appeared. That asserts a SCHEDULING OUTCOME, not a property of the pool: the
// tasks take under a microsecond, so one worker draining the whole queue while its colleagues never
// wake is perfectly correct behaviour. It produced an unexplained red under load — and pinning the
// process to a single core reproduced it **40 times out of 40**, which is what identified it: the
// pool was never at fault, the test was measuring the machine.
//
// The barrier below makes the requirement ENFORCEABLE instead of lucky: each task blocks until
// `kWorkers` distinct threads have arrived. If the pool really can run four tasks at once they all
// arrive and it is deterministic even on one core (a blocked worker occupies no CPU); if it cannot,
// the deadline expires and it goes red for the right reason. It is also a STRONGER claim than the
// old one — thread ids differing over time does not prove anything ran at the same time.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/thread_pool.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <array>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <thread>

static int g_pass = 0, g_fail = 0;
static void check(bool ok, const char* msg)
{
    if (ok) { ++g_pass; std::printf("  [ok]   %s\n", msg); }
    else    { ++g_fail; std::printf("  [FAIL] %s\n", msg); }
}

/// Enqueues `tasks` tasks into a pool of `workers` and returns how many distinct threads managed to be
/// inside a task AT THE SAME TIME, waiting for each other. Each task parks on the barrier until either
/// everyone expected has arrived or `deadlineMs` runs out, so the answer is a fact about the pool and
/// not about how the scheduler felt.
// ⚠️ THE BARRIER USES NO MUTEX, and that is the second thing ThreadSanitizer taught this file. The
// first version kept the shared state in three stack locals captured BY REFERENCE into tasks running
// on other threads — correct only because the pool's destructor joins, which is a fact about a
// different file — and TSan reported a double lock and a race on the set. Moving the state to the heap
// removed the address reuse and TSan STILL reported the double lock, so the construct itself was the
// problem, not where it lived. Rather than argue with the tool about a mutex the barrier never needed,
// the barrier is now atomics: each task takes a slot, records its thread, and waits for the count.
// Same property, nothing to get wrong, and it is shorter.
struct Barrier
{
    std::atomic<int>                      count{0};
    std::array<std::atomic<size_t>, 64>   ids{};   // hashed thread ids, one slot per arrival
};

static size_t concurrentArrivals(int workers, int tasks, int expected, int deadlineMs)
{
    auto b = std::make_shared<Barrier>();

    {
        DGS::ThreadPool pool(workers);
        for (int i = 0; i < tasks; ++i)
        {
            pool.enqueue([b, expected, deadlineMs]() {
                const int slot = b->count.fetch_add(1, std::memory_order_acq_rel);
                if (slot < (int)b->ids.size())
                    b->ids[slot].store(std::hash<std::thread::id>{}(std::this_thread::get_id()),
                                       std::memory_order_release);
                // Holding the task open is the whole point: a worker that has returned frees its
                // colleague from ever having to exist.
                const auto until = std::chrono::steady_clock::now()
                                 + std::chrono::milliseconds(deadlineMs);
                while (b->count.load(std::memory_order_acquire) < expected &&
                       std::chrono::steady_clock::now() < until)
                    std::this_thread::sleep_for(std::chrono::milliseconds(2));
            });
        }
    }   // the destructor joins: by here nobody is still inside a task

    std::set<size_t> distinct;
    const int n = b->count.load(std::memory_order_acquire);
    for (int i = 0; i < n && i < (int)b->ids.size(); ++i)
        distinct.insert(b->ids[i].load(std::memory_order_acquire));
    return distinct.size();
}

int main()
{
    constexpr int kWorkers = 4;
    constexpr int kTasks   = 500;      // far more than the workers: the queue must outlive them

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

    check((int)threads.size() <= kWorkers,
          "no more threads work than were requested at construction");

    // ── Concurrency, made enforceable ───────────────────────────────────────────────────────────
    const size_t together = concurrentArrivals(kWorkers, kWorkers, kWorkers, 4000);
    std::printf("    threads inside a task AT THE SAME TIME: %zu of %d\n", together, kWorkers);
    check((int)together == kWorkers,
          "the pool runs as many tasks CONCURRENTLY as it has workers");

    // ── Counter-proof: the same barrier, on a pool that cannot satisfy it ───────────────────────
    // Without this the check above proves nothing — a barrier that always reports success would look
    // identical. One worker cannot have two tasks open at once, so this must come back as 1.
    const size_t alone = concurrentArrivals(1, 4, 2, 300);
    std::printf("    same barrier on a ONE-worker pool: %zu (it must not reach 2)\n", alone);
    check(alone == 1,
          "the barrier DOES go red when there is no concurrency (it is not a rubber stamp)");

    std::printf("\n== thread_pool: %d OK · %d FAILED ==\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
