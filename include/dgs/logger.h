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
            // ⚠️ EL ORDEN DE ESTOS DOS MIEMBROS ES LA DIFERENCIA ENTRE ESCRIBIR EL LOG Y PERDERLO.
            // Members are destroyed in REVERSE declaration order. With `pool` declared before `file`,
            // destroying the Logger closed the `ofstream` FIRST and only afterwards did `~ThreadPool`
            // drain its queue: the pending tasks wrote into an already-destroyed stream. It is not just
            // that entries were lost — they were, measured: the file kept the header and NOTHING else —
            // it is undefined behaviour.
            //
            // With `file` declared first the pool is destroyed earlier, drains while the stream is
            // still alive, and only then is the file closed. The initialiser list follows this same
            // order because construction follows DECLARATION order, not list order.
            //
            // And this happens exactly when a node shuts down, which is when the log matters most.
            Logger(const std::string& filename) : file(filename, std::ios::app), pool(1)
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
            std::ofstream file;   // FIRST: destroyed LAST, once the pool has drained
            ThreadPool    pool;   // SECOND: destroyed first, flushing its queue into a live `file`
    };
};

#endif