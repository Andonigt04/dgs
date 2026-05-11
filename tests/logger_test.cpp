#include "include/dgs/logger.h"
#include <thread>
#include <chrono>

int main()
{
    DGS::Logger logger("server_logs.csv");

    DGS::LogEntry e{};
    e.time_stamp  = 1715000000;
    e.type        = DGS::LOG_TRANSFER;
    e.entityType  = DGS::ENT_PLAYER;
    e.uuid        = 777;
    e.fd          = 5;
    e.bytes       = 226;
    e.ramUsage    = 0.45f;
    e.performance = 0.12f;

    for (int i = 0; i < 10; i++)
    {
        e.uuid = i;
        logger.log(e);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    return 0;
}