#ifndef DGS_PACKET_H
#define DGS_PACKET_H

#include <vector>
#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <stdexcept>

#include "include/dgs/types.h"

namespace DGS
{

    class Packet
    {

    public:

        Packet() : readPos(0) {}

        template<typename T>
        void write(T data)
        {
            const uint8_t* ptr = reinterpret_cast<const uint8_t*>(&data);
            buffer.insert(buffer.end(), ptr, ptr + sizeof(T));
        }

        template<typename T>
        T read()
        {
            if (readPos + sizeof(T) > buffer.size()) throw std::runtime_error("Packet read overflow");

            T data;
            std::memcpy(&data, &buffer[readPos], sizeof(T));
            readPos += sizeof(T);

            return data;
        }

        void writeRaw(const uint8_t* data, size_t size)
        {
            buffer.insert(buffer.end(), data, data + size);
        }

        void readRaw(uint8_t* dest, size_t size)
        {
            if (readPos + size > buffer.size()) throw std::runtime_error("Packet read overflow (Raw)");
            std::memcpy(dest, &buffer[readPos], size);
            readPos += size;
        }

        void writeString(const std::string& str)
        {
            write<uint16_t>(static_cast<uint16_t>(str.size()));
            buffer.insert(buffer.end(), str.begin(), str.end());
        }

        std::string readString()
        {
            uint16_t size = read<uint16_t>();
            if (readPos + size > buffer.size()) throw std::runtime_error("String read overflow");

            std::string str(buffer.begin() + readPos, buffer.begin() + readPos + size);
            readPos += size;

            return str;
        }

        void clear() { buffer.clear(); readPos = 0; }

        void pack(const EntityTransfer& data);
        void pack(const Command& data);
        void pack(const ServerMetrics& data);
        void pack(const ZoneQuery& data);
        void pack(const ZoneResponse& data);
        void pack(const ZoneListResponse& data);
        void pack(const GhostDelta& data);
        void pack(const ChatMessage& data);
        // Validation (PLAN_DGS_VALIDADOR §2.2): request/ack + the validator's telemetry.
        void pack(const ValidateRequest& data);
        void pack(const ValidateAck& data);
        void pack(const ValidatorStatus& data);
        void pack(const EntityReassign& data);
        void pack(const ZoneLifecycle& data);
        void packDelete(const ZoneLifecycle& data);   // §3.9: mismo struct, tipo PKT_DELETE_ZONE
        void pack(const ZoneRegion& data);
        // §3.7 Plano social/cuenta: deltas de guild/party + acciones de cuenta (ban/permisos).
        void pack(const SocialDelta& data);
        void pack(const AccountAction& data);
        void pack(const PacketType& t) { clear(); write<PacketType>(t); }

        EntityTransfer unpackEntityTransfer();
        Command unpackCommand();
        ServerMetrics unpackServerMetrics();
        ZoneQuery unpackZoneQuery();
        ZoneResponse unpackZoneResponse();
        ZoneListResponse unpackZoneListResponse();
        GhostDelta unpackGhostDelta();
        ChatMessage unpackChatMessage();
        ValidateRequest unpackValidateRequest();
        ValidateAck unpackValidateAck();
        ValidatorStatus unpackValidatorStatus();
        EntityReassign unpackEntityReassign();
        ZoneLifecycle unpackZoneLifecycle();
        ZoneRegion unpackZoneRegion();
        SocialDelta unpackSocialDelta();
        AccountAction unpackAccountAction();
        PacketType unpackPacketType() { PacketType data; data = read<PacketType>(); return data; };
        
        PacketType getType() const { return buffer.empty() ? static_cast<PacketType>(0) : static_cast<PacketType>(buffer[0]); }
        
        const uint8_t* getRawData() const { return buffer.data(); }
        size_t getSize() const { return buffer.size(); }
        void setBuffer(const uint8_t* data, size_t size) { buffer.assign(data, data + size); }

        // --- Length framing for TCP (§4.6 bug 6) ---------------------------------------------------
        // TCP is a STREAM with no message boundaries: one `receive` can bring half a packet or several
        // chained together, and with no explicit delimiter `Packet::read` interprets foreign bytes as
        // payload (→ `runtime_error`). So every packet travels PREFIXED with its length:
        // [len:4][payload]. `toFramed()` produces that prefix; `PacketFramer` (below) is the receiving
        // accumulator that hands complete packets out of arbitrary recvs.
        //
        // ⚠️ NETWORK BYTE ORDER, and that is a CHANGE. This code lived only in the vendored copy inside
        // `haruka-cpp/external/dgs/` and wrote the prefix LITTLE-ENDIAN, while `TCPSocket::send` in this
        // same repository writes it with `htonl`. Two incompatible framings for one protocol: a
        // `PacketFramer` fed from a real node's stream read every length byte-swapped — a 34-byte packet
        // announced 570425344, over MAX_PACKET_SIZE, so the framer declared the stream corrupt and
        // resynchronised for ever. Nothing caught it because the only users round-tripped
        // `toFramed()` → `feed()` → `next()` against themselves, where any order agrees with itself.
        // Aligned here to `htonl` so the framer can actually read what the nodes write.
        std::vector<uint8_t> toFramed() const
        {
            if (buffer.size() > DGS::MAX_PACKET_SIZE)
                throw std::runtime_error("Packet too large to frame (over MAX_PACKET_SIZE)");
            std::vector<uint8_t> out;
            out.reserve(buffer.size() + 4);
            const uint32_t len = (uint32_t)buffer.size();
            out.push_back((uint8_t)(len >> 24));
            out.push_back((uint8_t)(len >> 16));
            out.push_back((uint8_t)(len >> 8));
            out.push_back((uint8_t)(len >> 0));
            out.insert(out.end(), buffer.begin(), buffer.end());
            return out;
        }

    private:
        std::vector<uint8_t> buffer;
        size_t readPos;
    };

    // ==============================================================================================
    // PacketFramer — accumulates a TCP stream into COMPLETE packets (§4.6 bug 6).
    //
    // The sender writes `Packet::toFramed()` ([len:4] prefix + payload). The receiver feeds this
    // accumulator with whatever each `TCPSocket::receive` returns — a fragment, one whole packet, or
    // several chained — and `next()` hands out ONLY intact packets. Leftover bytes are kept for the
    // following call. If a prefix lies (len 0 or > MAX_PACKET_SIZE) the stream is corrupt or
    // desynchronised: one byte is dropped and it resynchronises, counting the loss in `discards()`
    // (the node adds it to `failedTransfers`). It does not allocate per packet: the internal buffer is
    // reused and copies are only made when a complete frame comes out.
    //
    // Ported back from `haruka-cpp/external/dgs/`, where it had been added directly to the vendored
    // copy — see the note on `toFramed()`.
    // ==============================================================================================
    class PacketFramer
    {
        public:
            PacketFramer() { pending.reserve(DGS::MAX_PACKET_SIZE); }

            // Consumes the bytes of one receive. Safe with partial, exact or multiple recvs.
            void feed(const uint8_t* data, size_t size)
            {
                if (!data || size == 0) return;
                pending.insert(pending.end(), data, data + size);
            }

            // Extracts the next COMPLETE packet. `out` is filled with the payload (no prefix).
            // Returns false while bytes are still missing; the caller waits for the next receive.
            bool next(std::vector<uint8_t>& out)
            {
                // Index-based scanning: never erase byte by byte (O(n^2) with 64 KB of garbage). `scan`
                // moves forward and the buffer is only compacted when a frame is extracted or on exit.
                const size_t NPOS = (size_t)-1;
                size_t park = NPOS;   // first PLAUSIBLE candidate that cannot be verified yet (resync)

                while (scan + 4 <= pending.size())
                {
                    const uint32_t len = prefixAt(scan);

                    if (len == 0 || len > DGS::MAX_PACKET_SIZE)
                    {
                        // A lying prefix: desynchronisation. Advance ONE byte and resynchronise.
                        resyncing = true;
                        ++scan;
                        continue;
                    }

                    const size_t end = scan + 4u + len;

                    if (!resyncing)
                    {
                        // In sync: the prefix is trustworthy, a half payload simply waits.
                        if (end > pending.size()) { compact(scan); return false; }
                        return deliver(scan, len, out);
                    }

                    // RESYNCHRONISING: a "valid" prefix can be a shifted read of the garbage (e.g. the
                    // low bytes of a real length read at an offset). Only a candidate VERIFIABLE with
                    // what is already buffered is accepted: entirely present and followed either by the
                    // end of the accumulator or by another valid prefix. If it cannot be verified for
                    // lack of bytes it is PARKED (it may be a legitimate fragmented frame) and the
                    // search goes on for one that can.
                    bool verified = false;
                    if (end <= pending.size())
                    {
                        const size_t rest = pending.size() - end;
                        if (rest == 0)      verified = true;
                        else if (rest >= 4) { const uint32_t n = prefixAt(end);
                                              verified = (n != 0 && n <= DGS::MAX_PACKET_SIZE); }
                        // rest 1..3 → not enough to look at the next prefix: not verifiable.
                    }

                    if (verified) { resyncing = false; return deliver(scan, len, out); }
                    if (park == NPOS && end + 4u > pending.size()) park = scan;   // could still complete
                    ++scan;
                }

                // No acceptable candidate: if a plausible one is waiting for bytes, stay on it.
                compact(park == NPOS ? scan : park);
                return false;
            }

            // Bytes waiting without completing a frame (fragmentation diagnostic).
            size_t buffered() const { return pending.size() - scan; }
            // Frames discarded through a corrupt header (desynchronisation). Adds to failedTransfers.
            uint64_t discards() const { return discarded; }

            void clear() { pending.clear(); scan = 0; discarded = 0; resyncing = false; }

        private:
            uint32_t prefixAt(size_t i) const
            {
                return ((uint32_t)pending[i]     << 24)
                     | ((uint32_t)pending[i + 1] << 16)
                     | ((uint32_t)pending[i + 2] << 8)
                     |  (uint32_t)pending[i + 3];
            }

            // Compacts the buffer up to `pos`: those bytes were NOT part of any valid frame, so they
            // are accounted for as loss (→ failedTransfers).
            void compact(size_t pos)
            {
                if (pos == 0) return;
                discarded += pos;
                pending.erase(pending.begin(), pending.begin() + pos);
                scan = 0;
            }

            // Delivers the frame starting at `pos`: the garbage before it is counted, the frame is not.
            bool deliver(size_t pos, uint32_t len, std::vector<uint8_t>& out)
            {
                out.assign(pending.begin() + pos + 4, pending.begin() + pos + 4 + len);
                compact(pos);                                   // prior discard (0 if it was in sync)
                pending.erase(pending.begin(), pending.begin() + 4 + len);
                scan = 0;
                return true;
            }

            std::vector<uint8_t> pending;
            size_t   scan = 0;
            uint64_t discarded = 0;
            bool     resyncing = false;   // after a corrupt prefix, until a frame is latched again
    };

    using PacketHandler = std::function<void(int, Packet&)>;

    class PacketDispatcher
    {
        public:
            void registerHandler(PacketType type, PacketHandler handler) {
                handlers[type] = handler;
            }

            void dispatch(int fd, Packet& p) {
                PacketType type = p.getType();
                if (handlers.count(type)) {
                    handlers[type](fd, p);
                }
            }

        private:
            std::map<PacketType, PacketHandler> handlers;
    };
};

#endif // DGS_PACKET_H