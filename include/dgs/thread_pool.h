#ifndef DGS_THREAD_POOL_H
#define DGS_THREAD_POOL_H

#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>

namespace DGS
{
    class ThreadPool
    {
        public:
            ThreadPool(int numWorkers) : stop(false)
            {
                for (int i = 0; i < numWorkers; i++)
                {
                    workers.emplace_back([this]() { workersLoop(); });
                }
            }

            ~ThreadPool()
            {
                stop = true;
                cv.notify_all();
                for (auto& worker : workers) { worker.join(); }
            }

            void enqueue(std::function<void()> task)
            {
                {
                    std::unique_lock<std::mutex> lock(mutex);
                    tasks.push(task);
                }
                cv.notify_one();
            }
        private:
            void workersLoop()
            {
                while (true)
                {
                    std::function<void()> task;

                    {
                        std::unique_lock<std::mutex> lock(mutex);
                        cv.wait(lock, [this]() {
                            return stop || !tasks.empty();
                        });

                        if (stop && tasks.empty()) return;

                        task = tasks.front();
                        tasks.pop();
                    }

                    task();
                }
            }

            std::vector<std::thread>          workers;
            std::queue<std::function<void()>> tasks;
            std::mutex                        mutex;
            std::condition_variable           cv;
            bool                              stop;
    };
};

#endif