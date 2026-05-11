#include "include/dgs/thread_pool.h"
#include <iostream>
#include <chrono>

int main()
{
    DGS::ThreadPool pool(4);

    for (int i = 0; i < 10; i++)
    {
        pool.enqueue([i]() {
            std::cout << "Tarea " << i << " ejecutada por hilo " 
                      << std::this_thread::get_id() << std::endl;
        });
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    return 0;
}