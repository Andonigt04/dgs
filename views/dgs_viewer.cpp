// ─────────────────────────────────────────────────────────────────────────────────────────────────
// DGS Viewer — the zones AND what is moving inside them.
//
// It used to draw only the boxes: it asked the head for the zone list and rendered six orthographic
// views of empty cubes. You could see how the world was split and nothing about what was happening in
// it, which is the half that tells you whether the cluster is alive.
//
// There was no way to watch, either. The head routes each entity to the zone that covers it and to
// nobody else, and a zone only broadcasts to clients that registered by SENDING a position — so the
// only way in was to inject a fake player into the world. Now there is a read-only subscription
// (`PKT_OBSERVE`, one byte, over UDP): the zone adds the sender to its own observer registry, feeds it
// the SAME snapshot its players get, and expires it on a lease. An observer never becomes an entity.
//
//     [head] --ZoneListResponse (TCP)--> [viewer]   the boxes: who serves what
//     [zone] <--PKT_OBSERVE (UDP)-------- [viewer]   "add me to your broadcast"
//     [zone] --EntityTransfer / GhostDelta--> [viewer]   the moving objects
//
// What is on screen, and why each distinction is worth pixels:
//   · a box per zone, coloured by node, labelled with addr:port and its live entity count;
//   · a REAL entity as a solid cube in its owning zone's colour — the node that simulates it;
//   · a GHOST as a wireframe, because it is a neighbour's projection of something it does not own:
//     the whole architecture lives or dies on that difference;
//   · an entity in a chunk NO zone covers in red — that should never happen and you want to see it.
//
// The decoding and expiry live in `viewer_state.h`, with no raylib, so they can be driven headless by
// `tests/viewer_e2e.cpp` against a real `zone_node`. What is verified there is that the picture
// matches the cluster; what is left here is drawing it.
//
// Usage:  dgs_viewer [head-host] [head-port]      env: DGS_CHUNK_SIZE (default 1000)
// Keys:   0 all six views · 1..6 one view · G toggle ghosts · TAB entity list
// ─────────────────────────────────────────────────────────────────────────────────────────────────
#include "include/dgs/types.h"
#include "include/dgs/network.h"
#include "include/dgs/packet.h"
#include "views/viewer_state.h"
#include <csignal>

#include <raylib.h>
#include <sys/socket.h>
#include <ctime>
#include <atomic>
#include <iostream>
#include <mutex>
#include <thread>
#include <chrono>
#include <algorithm>

static uint64_t nowMs()
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

enum ViewMode { ALL = 0, TOP_DOWN, DOWN_TOP, FRONT_BACK, BACK_FRONT, RIGHT_LEFT, LEFT_RIGHT };

struct View { Camera3D cam; const char* label; };

static Camera3D setupCamera(Vector3 pos, Vector3 target, Vector3 up, float fov)
{
    Camera3D cam = {};
    cam.position   = pos;
    cam.target     = target;
    cam.up         = up;
    cam.fovy       = fov;
    cam.projection = CAMERA_ORTHOGRAPHIC;
    return cam;
}

static Vector3 zoneCenter(const DGS::ZoneInfoPublic& z)
{
    return { (z.chunkXMin + z.chunkXMax) / 2.0f,
             (z.chunkYMin + z.chunkYMax) / 2.0f,
             (z.chunkZMin + z.chunkZMax) / 2.0f };
}

static Color zoneColor(int i, int total)
{
    return ColorFromHSV(i * (360.0f / std::max(total, 1)), 0.8f, 1.0f);
}

static void drawZoneCube(const DGS::ZoneInfoPublic& z, int i, int total)
{
    const Vector3 c = zoneCenter(z);
    // +1: the bounds are INCLUSIVE ([min,max] chunks), so a zone covering 0..49 is 50 chunks wide.
    // Drawing max-min left a one-chunk gap between neighbours that looked like a hole in the world.
    const Vector3 s = { (float)(z.chunkXMax - z.chunkXMin + 1),
                        (float)(z.chunkYMax - z.chunkYMin + 1),
                        (float)(z.chunkZMax - z.chunkZMin + 1) };
    const Color col = zoneColor(i, total);
    DrawCube(c, s.x, s.y, s.z, ColorAlpha(col, 0.08f));
    DrawCubeWires(c, s.x, s.y, s.z, col);
}

int main(int argc, char* argv[])
{
    // ⚠️ A NODE MUST NOT DIE BECAUSE A PEER HUNG UP. Writing to a socket whose other end has closed
    // raises SIGPIPE, and its default action is to KILL the process. No node installed this, and the
    // whole suite stayed green anyway: every test calls `signal(SIGPIPE, SIG_IGN)` before `fork()`, and
    // a child INHERITS an ignored disposition — so under CTest the nodes survived, and started from a
    // shell, systemd, Docker or `dgs run` they died the first time a peer disconnected.
    // Measured with the same binary and the same environment: parent ignoring SIGPIPE -> ran the full
    // 6 s; ordinary parent -> exit 141 (128 + SIGPIPE) within seconds of the head closing.
    // A closed peer is an ordinary event: `send` returns EPIPE and the reconnect paths handle it.
    std::signal(SIGPIPE, SIG_IGN);
    const char* headHost = argc > 1 ? argv[1] : "127.0.0.1";
    const int   headPort = argc > 2 ? std::atoi(argv[2]) : 42424;
    const float chunkSize = (float)std::atof(std::getenv("DGS_CHUNK_SIZE")
                                             ? std::getenv("DGS_CHUNK_SIZE") : "1000.0");

    DGS::ViewerState state(chunkSize);
    std::mutex       mtx;
    std::atomic<bool> quit{false};
    std::atomic<bool> headUp{false};
    std::atomic<int>  observedZones{0};

    DGS::TCPSocket head;
    if (!head.connect(headHost, headPort))
    {
        std::cerr << "[Viewer] cannot connect to the head at " << headHost << ":" << headPort
                  << std::endl;
        return 1;
    }
    headUp = true;
    { struct timeval tv { 1, 0 };
      setsockopt(head.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
      setsockopt(head.getSocketFD(), SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)); }

    // ── The zone list, from the head ────────────────────────────────────────────────────────────
    std::thread zonesThread([&]
    {
        DGS::Packet req; req.pack(DGS::PKT_ZONE_LIST);
        while (!quit)
        {
            if (!head.send(head.getSocketFD(), req.getRawData(), req.getSize()))
            {
                headUp = false;
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            }
            uint8_t buf[8192];
            const int n = head.receive(head.getSocketFD(), buf, sizeof(buf));
            if (n > 0)
            {
                DGS::Packet resp; resp.setBuffer(buf, n);
                if (resp.getType() == DGS::PKT_ZONE_LIST)
                {
                    const auto r = resp.unpackZoneListResponse();
                    std::lock_guard<std::mutex> lk(mtx);
                    state.setZones(r.zones, r.count);
                }
                headUp = true;
            }
            else if (n == 0) headUp = false;   // the head hung up
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    });

    // ── The moving objects, from every zone ─────────────────────────────────────────────────────
    // One socket for all of them: the subscription is per endpoint, so every zone answers to the same
    // address and the datagrams simply interleave.
    std::thread feedThread([&]
    {
        DGS::UDPSocket udp;
        udp.bind(0);
        { struct timeval tv { 0, 100000 };
          setsockopt(udp.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }

        DGS::Packet hello; hello.pack(DGS::PKT_OBSERVE);
        uint64_t lastHello = 0;
        uint8_t  buf[sizeof(DGS::EntityTransfer) * 2];
        std::string from; int port = 0;

        while (!quit)
        {
            // Re-subscribe periodically: the lease is what stops a zone feeding a viewer that closed,
            // so staying subscribed has to be an active act. It also picks up zones that appear later.
            const uint64_t t = nowMs();
            if (t - lastHello > 2000)
            {
                lastHello = t;
                std::vector<DGS::ZoneInfoPublic> zs;
                { std::lock_guard<std::mutex> lk(mtx); zs = state.zones(); }
                for (const auto& z : zs)
                    udp.send(z.addr, z.port, hello.getRawData(), hello.getSize());
                observedZones = (int)zs.size();
            }

            const int n = udp.receive(buf, sizeof(buf), from, port);
            if (n > 0)
            {
                std::lock_guard<std::mutex> lk(mtx);
                state.onDatagram(buf, n, nowMs());
            }
            { std::lock_guard<std::mutex> lk(mtx); state.expire(nowMs()); }
        }
    });

    // ── Window ──────────────────────────────────────────────────────────────────────────────────
    const int COLS = 3, ROWS = 2;
    InitWindow(1280, 720, "DGS Viewer");
    SetTargetFPS(60);

    const int vpW = GetScreenWidth() / COLS, vpH = GetScreenHeight() / ROWS;
    RenderTexture2D rts[6];
    for (int i = 0; i < 6; i++) rts[i] = LoadRenderTexture(vpW, vpH);

    View views[6] = { {{}, "Top"}, {{}, "Bottom"}, {{}, "Front"},
                      {{}, "Back"}, {{}, "Right"}, {{}, "Left"} };

    ViewMode mode      = ALL;
    bool     showGhosts = true;
    bool     showList   = false;

    while (!WindowShouldClose())
    {
        if (IsKeyPressed(KEY_ZERO))  mode = ALL;
        if (IsKeyPressed(KEY_ONE))   mode = TOP_DOWN;
        if (IsKeyPressed(KEY_TWO))   mode = DOWN_TOP;
        if (IsKeyPressed(KEY_THREE)) mode = FRONT_BACK;
        if (IsKeyPressed(KEY_FOUR))  mode = BACK_FRONT;
        if (IsKeyPressed(KEY_FIVE))  mode = RIGHT_LEFT;
        if (IsKeyPressed(KEY_SIX))   mode = LEFT_RIGHT;
        if (IsKeyPressed(KEY_G))     showGhosts = !showGhosts;
        if (IsKeyPressed(KEY_TAB))   showList   = !showList;

        std::vector<DGS::ZoneInfoPublic> zs;
        std::vector<DGS::ViewedEntity>   ents;
        std::vector<int>                 owner;
        {
            std::lock_guard<std::mutex> lk(mtx);
            zs   = state.zones();
            ents = state.entities();
            owner.reserve(ents.size());
            for (const auto& e : ents) owner.push_back(state.zoneOf(e));
        }

        // Frame the world on the zones; with none yet, on whatever is moving — otherwise connecting
        // before any zone has registered gives a black screen with no clue why.
        Vector3 center = { 0, 0, 0 };
        float   extent = 100.0f;
        if (!zs.empty())
        {
            float xMin = (float)zs[0].chunkXMin, xMax = (float)zs[0].chunkXMax;
            float yMin = (float)zs[0].chunkYMin, yMax = (float)zs[0].chunkYMax;
            float zMin = (float)zs[0].chunkZMin, zMax = (float)zs[0].chunkZMax;
            for (const auto& z : zs) {
                xMin = std::min(xMin,(float)z.chunkXMin); xMax = std::max(xMax,(float)z.chunkXMax);
                yMin = std::min(yMin,(float)z.chunkYMin); yMax = std::max(yMax,(float)z.chunkYMax);
                zMin = std::min(zMin,(float)z.chunkZMin); zMax = std::max(zMax,(float)z.chunkZMax);
            }
            center = { (xMin+xMax)/2, (yMin+yMax)/2, (zMin+zMax)/2 };
            extent = std::max({xMax-xMin, yMax-yMin, zMax-zMin}) * 1.5f + 1.0f;
        }
        else if (!ents.empty())
        {
            center = { ents[0].x, ents[0].y, ents[0].z };
            extent = 50.0f;
        }

        views[0].cam = setupCamera({center.x, center.y+extent, center.z}, center, {0,0,-1}, extent);
        views[1].cam = setupCamera({center.x, center.y-extent, center.z}, center, {0,0, 1}, extent);
        views[2].cam = setupCamera({center.x, center.y, center.z+extent}, center, {0,1, 0}, extent);
        views[3].cam = setupCamera({center.x, center.y, center.z-extent}, center, {0,1, 0}, extent);
        views[4].cam = setupCamera({center.x+extent, center.y, center.z}, center, {0,1, 0}, extent);
        views[5].cam = setupCamera({center.x-extent, center.y, center.z}, center, {0,1, 0}, extent);

        // An entity is a fraction of a chunk across, so at world scale it would be a sub-pixel dot.
        // It is drawn at a fixed fraction of the view instead: this is a monitor, not a simulation.
        const float dot = std::max(extent * 0.012f, 0.05f);

        auto renderScene = [&](Camera3D cam, int w, int h)
        {
            BeginMode3D(cam);
            for (int i = 0; i < (int)zs.size(); i++) drawZoneCube(zs[i], i, (int)zs.size());

            for (size_t i = 0; i < ents.size(); i++)
            {
                const DGS::ViewedEntity& e = ents[i];
                if (e.ghost && !showGhosts) continue;

                const Vector3 p = { e.x, e.y, e.z };
                // Red = in a chunk NO zone covers. It should be impossible, and if it happens it is
                // exactly the thing you opened the viewer to find.
                const Color col = owner[i] < 0 ? RED : zoneColor(owner[i], (int)zs.size());

                if (e.ghost) DrawCubeWires(p, dot, dot, dot, ColorAlpha(col, 0.9f));
                else         DrawCube(p, dot, dot, dot, col);
            }
            EndMode3D();

            for (int i = 0; i < (int)zs.size(); i++)
            {
                int live = 0;
                for (size_t k = 0; k < ents.size(); k++) if (owner[k] == i && !ents[k].ghost) ++live;
                const Vector2 sp = GetWorldToScreenEx(zoneCenter(zs[i]), cam, w, h);
                DrawText(TextFormat("%s:%d  [%d]", zs[i].addr, zs[i].port, live),
                         (int)sp.x, (int)sp.y, 10, zoneColor(i, (int)zs.size()));
            }
        };

        BeginDrawing();
        ClearBackground(BLACK);

        if (mode == ALL)
        {
            for (int i = 0; i < 6; i++)
            {
                BeginTextureMode(rts[i]);
                ClearBackground(BLACK);
                renderScene(views[i].cam, vpW, vpH);
                DrawText(views[i].label, 4, 4, 12, GRAY);
                EndTextureMode();
                const float x = (float)(i % COLS) * vpW, y = (float)(i / COLS) * vpH;
                // Y flipped: OpenGL textures are bottom-up.
                DrawTextureRec(rts[i].texture, { 0, 0, (float)vpW, -(float)vpH }, { x, y }, WHITE);
            }
        }
        else
        {
            const int idx = mode - 1;
            renderScene(views[idx].cam, GetScreenWidth(), GetScreenHeight());
            DrawText(views[idx].label, 4, 4, 16, GRAY);
        }

        // Status bar. It says what the viewer KNOWS, including when it knows nothing: a black screen
        // because the head is down and a black screen because the world is empty are different things.
        int ghosts = 0;
        for (const auto& e : ents) if (e.ghost) ++ghosts;
        const int sh = GetScreenHeight();
        DrawRectangle(0, sh - 46, GetScreenWidth(), 46, ColorAlpha(BLACK, 0.75f));
        DrawText(TextFormat("head %s:%d  %s   zones %d (observing %d)   entities %d (%d ghosts)   chunk %.0f",
                            headHost, headPort, headUp ? "UP" : "DOWN",
                            (int)zs.size(), observedZones.load(),
                            (int)ents.size() - ghosts, ghosts, chunkSize),
                 8, sh - 40, 14, headUp ? RAYWHITE : RED);
        DrawText("0 all · 1-6 single view · G ghosts · TAB list", 8, sh - 20, 12, GRAY);

        if (zs.empty())
            DrawText("no zones registered with the head yet", 8, 8, 16, ORANGE);

        if (showList)
        {
            int row = 0;
            for (size_t i = 0; i < ents.size() && row < 30; i++, row++)
                DrawText(TextFormat("%u  chunk(%d,%d,%d)  %s  zone %d",
                                    ents[i].uuid, ents[i].chunkX, ents[i].chunkY, ents[i].chunkZ,
                                    ents[i].ghost ? "ghost" : "real ", owner[i]),
                         8, 30 + row * 14, 12, RAYWHITE);
        }

        EndDrawing();
    }

    quit = true;
    zonesThread.join();
    feedThread.join();
    for (int i = 0; i < 6; i++) UnloadRenderTexture(rts[i]);
    CloseWindow();
    return 0;
}
