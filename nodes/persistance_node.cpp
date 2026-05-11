#include "include/dgs/network.h"
#include "include/dgs/packet.h"
#include "include/dgs/logger.h"
#include "include/dgs/types.h"

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>
#include <bsoncxx/builder/stream/document.hpp>

void insertEntity(mongocxx::collection& col, const DGS::EntityTransfer& e)
{
    bsoncxx::builder::stream::document doc{};
    doc << "uuid"     << (int32_t)e.uuid
        << "type"     << (int32_t)e.type
        << "chunkX"   << e.chunkX
        << "chunkY"   << e.chunkY
        << "chunkZ"   << e.chunkZ
        << "localX"   << (double)e.pos[0]
        << "localY"   << (double)e.pos[1]
        << "localZ"   << (double)e.pos[2]
        << "angle"    << (int32_t)e.angle
        << "dataSize" << (int32_t)e.dataSize
        << "state"    << (int32_t)e.state;

    col.insert_one(doc.view());
}

int main()
{
    mongocxx::instance instance{};
    mongocxx::client   client{mongocxx::uri{"mongodb://localhost:27017"}};

    auto db       = client["dgs_persistance"];
    auto entities = db["entities"];

    std::cout << "[Persistence] Conectado a MongoDB" << std::endl;

    return 0;
}