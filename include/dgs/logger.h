#ifndef DGS_LOGGER_H
#define DGS_LOGGER_H

#include "include/dgs/thread_pool.h"
#include "include/dgs/types.h"

#include <chrono>
#include <fstream>



namespace DGS
{
    class Logger
    {
        public:
            Logger(const std::string& filename) : pool(1), file(filename, std::ios::app)
            {
                if (file.tellp() == 0)
                    file << "timestamp,type,entityType,uuid,fd,bytes,ram,performance\n";
            }
            
            void log(LogEntry e)
            {
                pool.enqueue([this, e]()
                {
                   file <<  e.time_stamp << "," << (int)e.type << "," << (int)e.entityType << "," << e.uuid << "," << e.fd << "," << e.bytes << ","  << e.ramUsage << "," << e.performance << "\n";
                });
            }
        private:
            ThreadPool pool;
            std::ofstream file;
    };
};

#endif