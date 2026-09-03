#ifndef DGS_VIEWER_STATE_H
#define DGS_VIEWER_STATE_H

// ─────────────────────────────────────────────────────────────────────────────────────────────────
// What the viewer KNOWS, with no window attached.
//
// Deliberately free of raylib so it can be driven headless by a test: the interesting part of a viewer
// is not the drawing, it is whether what it draws matches what the cluster is actually doing. A viewer
// that renders beautifully from a stale or half-decoded feed is worse than none — it makes you trust a
// picture that is wrong.
//
// It consumes two feeds:
//   · the head's `ZoneListResponse` over TCP → the boxes (who serves what);
//   · each zone's UDP broadcast → the moving objects. A zone sends its clients raw `EntityTransfer`
//     structs and `Packet`-wrapped `GhostDelta`s; subscribing with PKT_OBSERVE gets that same stream.
//
// Two things it must get right, and both are about NOT lying:
//   · entities EXPIRE. UDP has no goodbye: a zone that dies, an entity that is evicted for cheating or
//     whose lease ran out, simply stops arriving. Without a TTL the viewer would keep drawing a player
//     who is no longer in the world.
//   · a GHOST is drawn as a ghost. It is the projection a neighbouring zone makes of an entity it does
//     NOT own; showing it as a real entity would hide the one thing this architecture is about.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/types.h"
#include "include/dgs/packet.h"

#include <cstdint>
#include <cstring>
#include <map>
#include <string>
#include <vector>

namespace DGS
{
    struct ViewedEntity
    {
        uint32_t uuid       = 0;
        float    x = 0, y = 0, z = 0;   // chunk space, the same units the zone boxes are drawn in
        int32_t  chunkX = 0, chunkY = 0, chunkZ = 0;
        uint16_t angle      = 0;
        bool     ghost      = false;    // projected by a neighbour, not owned by it
        uint64_t lastSeenMs = 0;
    };

    class ViewerState
    {
    public:
        /// `chunkSize` converts an entity's local position (metres inside its chunk) into the chunk
        /// space the zone boxes live in. It is not on the wire — the head only hands it to the nodes in
        /// their initial `Command` — so the viewer takes it from the environment and says so on screen.
        explicit ViewerState(float chunkSize = 1000.0f, uint64_t ttlMs = 3000)
            : m_chunkSize(chunkSize > 0.0f ? chunkSize : 1.0f), m_ttlMs(ttlMs) {}

        void setZones(const ZoneInfoPublic* z, uint8_t count)
        {
            m_zones.assign(z, z + count);
        }
        const std::vector<ZoneInfoPublic>& zones() const { return m_zones; }

        /// Feeds one datagram straight off a zone's UDP broadcast. @return whether it was understood.
        bool onDatagram(const uint8_t* data, int n, uint64_t nowMs)
        {
            if (!data || n <= 0) return false;

            // A raw `EntityTransfer` is recognised by its exact size — that is how the zone sends it and
            // how the nodes themselves recognise it. Anything else goes through the Packet decoder.
            if (n == (int)sizeof(EntityTransfer))
            {
                EntityTransfer e;
                std::memcpy(&e, data, sizeof(e));
                ViewedEntity& v = m_entities[e.uuid];
                v.uuid   = e.uuid;
                v.chunkX = e.chunkX; v.chunkY = e.chunkY; v.chunkZ = e.chunkZ;
                v.x = e.chunkX + e.pos[0] / m_chunkSize;
                v.y = e.chunkY + e.pos[1] / m_chunkSize;
                v.z = e.chunkZ + e.pos[2] / m_chunkSize;
                v.angle      = e.angle;
                v.ghost      = false;
                v.lastSeenMs = nowMs;
                return true;
            }

            Packet p;
            p.setBuffer(data, (size_t)n);
            if (p.getType() != PKT_GHOST_DELTA) return false;

            const GhostDelta g = p.unpackGhostDelta();
            ViewedEntity& v = m_entities[(uint32_t)g.uuid];
            v.uuid   = (uint32_t)g.uuid;
            v.chunkX = g.chunkX; v.chunkY = g.chunkY; v.chunkZ = g.chunkZ;
            v.x = g.chunkX + g.pos[0] / m_chunkSize;
            v.y = g.chunkY + g.pos[1] / m_chunkSize;
            v.z = g.chunkZ + g.pos[2] / m_chunkSize;
            // A ghost only marks the entity as projected if nothing REAL is arriving for it. A zone
            // that owns it and a neighbour that projects it both broadcast, and the owner is the truth.
            v.ghost      = true;
            v.lastSeenMs = nowMs;
            return true;
        }

        /// Drops whatever has not been heard from within the TTL. Must be called every frame: this is
        /// what keeps the viewer from drawing players who left.
        void expire(uint64_t nowMs)
        {
            for (auto it = m_entities.begin(); it != m_entities.end();)
            {
                if (nowMs - it->second.lastSeenMs > m_ttlMs) it = m_entities.erase(it);
                else ++it;
            }
        }

        std::vector<ViewedEntity> entities() const
        {
            std::vector<ViewedEntity> out;
            out.reserve(m_entities.size());
            for (const auto& kv : m_entities) out.push_back(kv.second);
            return out;
        }

        size_t entityCount() const { return m_entities.size(); }

        /// Which zone covers this entity, or -1. Used to colour it by owner and to make it visible when
        /// an entity is sitting in a chunk NO zone covers — which should not happen and is worth seeing.
        int zoneOf(const ViewedEntity& e) const
        {
            for (size_t i = 0; i < m_zones.size(); ++i)
            {
                const ZoneInfoPublic& z = m_zones[i];
                if (e.chunkX >= z.chunkXMin && e.chunkX <= z.chunkXMax &&
                    e.chunkY >= z.chunkYMin && e.chunkY <= z.chunkYMax &&
                    e.chunkZ >= z.chunkZMin && e.chunkZ <= z.chunkZMax) return (int)i;
            }
            return -1;
        }

        float chunkSize() const { return m_chunkSize; }

    private:
        std::vector<ZoneInfoPublic>       m_zones;
        std::map<uint32_t, ViewedEntity>  m_entities;
        float                             m_chunkSize;
        uint64_t                          m_ttlMs;
    };
}

#endif // DGS_VIEWER_STATE_H
