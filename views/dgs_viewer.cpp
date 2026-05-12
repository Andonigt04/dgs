#include "include/dgs/types.h"
#include "include/dgs/network.h"
#include "include/dgs/packet.h"

#include <raylib.h>
#include <sys/epoll.h>
#include <iostream>
#include <mutex>
#include <thread>
#include <chrono>
#include <algorithm>


enum ViewMode 
{ 
    ALL = 0,
    TOP_DOWN = 1,
    DOWN_TOP = 2,
    RIGHT_LEFT = 3,
    LEFT_RIGHT = 4,
    FRONT_BACK = 5,
    BACK_FRONT = 6
};

struct View {
    Camera3D cam;
    Rectangle viewport;
    const char* label;
};

Rectangle getViewport(int col, int row, int cols, int rows)
{
    float w = GetScreenWidth() / (float)cols;
    float h = GetScreenHeight() / (float)rows;
    
    return {col * w, row * h, w, h};
}

Camera3D setupCamera(Vector3 pos, Vector3 target, Vector3 up, float fov)
{
    Camera3D cam = {};
    cam.position = pos;
    cam.target = target;
    cam.up = up;
    cam.fovy = fov;
    cam.projection = CAMERA_ORTHOGRAPHIC;

    return cam;
}

static Vector3 zoneCenter(const DGS::ZoneInfoPublic& z)
{
    return { (z.chunkXMin + z.chunkXMax) / 2.0f,
             (z.chunkYMin + z.chunkYMax) / 2.0f,
             (z.chunkZMin + z.chunkZMax) / 2.0f };
}

void drawZoneCube(const DGS::ZoneInfoPublic& z, int i, int total)
{
    Vector3 center = zoneCenter(z);
    Vector3 size   = { (float)(z.chunkXMax - z.chunkXMin),
                       (float)(z.chunkYMax - z.chunkYMin),
                       (float)(z.chunkZMax - z.chunkZMin) };
    DrawCubeWires(center, size.x, size.y, size.z,
                  ColorFromHSV(i * (360.0f / total), 0.8f, 1.0f));
}

void drawZoneLabel(const DGS::ZoneInfoPublic& z, Camera3D cam)
{
    Vector2 pos = GetWorldToScreen(zoneCenter(z), cam);
    DrawText(z.addr, (int)pos.x, (int)pos.y, 10, WHITE);
}

float worldSize(const std::vector<DGS::ZoneInfoPublic>& snap)
{
    int32_t xMin = snap[0].chunkXMin, xMax = snap[0].chunkXMax;
    int32_t yMin = snap[0].chunkYMin, yMax = snap[0].chunkYMax;
    int32_t zMin = snap[0].chunkZMin, zMax = snap[0].chunkZMax;

    for (auto& z : snap) {
        xMin = std::min(xMin, z.chunkXMin); xMax = std::max(xMax, z.chunkXMax);
        yMin = std::min(yMin, z.chunkYMin); yMax = std::max(yMax, z.chunkYMax);
        zMin = std::min(zMin, z.chunkZMin); zMax = std::max(zMax, z.chunkZMax);
    }

    return std::max({xMax-xMin, yMax-yMin, zMax-zMin}) * 1.5f;
}

int main(int argc, char* argv[])
{
    DGS::TCPSocket tcpSocket;
    std::vector<DGS::ZoneInfoPublic> zones;
    std::mutex mtx;
    View views[6];

    const char* headHost = argc > 1 ? argv[1] : "127.0.0.1";
    int         headPort = argc > 2 ? std::atoi(argv[2]) : 42424;

    if (!tcpSocket.connect(headHost, headPort)) { std::perror("CAN NOT CONNECT TO HEAD"); return 1; }
    
    std::thread netThread([&]()
    {
        DGS::Packet req;
        req.pack(DGS::PKT_ZONE_LIST);

        while (true)
        {
            tcpSocket.send(tcpSocket.getSocketFD(), req.getRawData(), req.getSize());
            
            uint8_t buf[4096];
            int bytes = tcpSocket.receive(tcpSocket.getSocketFD(), buf, sizeof(buf));
            DGS::Packet resp;
            resp.setBuffer(buf, bytes);
            auto r = resp.unpackZoneListResponse();

            std::lock_guard<std::mutex> lock(mtx);
            zones.assign(r.zones, r.zones + r.count);

            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    });


    InitWindow(1280, 720, "DGS Viewer");

    while (!WindowShouldClose())
    {
        std::vector<DGS::ZoneInfoPublic> snap;
        { std::lock_guard<std::mutex> lock(mtx); snap = zones;}

        float dist = snap.empty() ? 100.0f : worldSize(snap);

        views[0] = { setupCamera({0, dist, 0},  {0,0,0}, {0,0,-1}, 45), getViewport(0,0,3,2), "Top"    };
        views[1] = { setupCamera({0,-dist, 0},  {0,0,0}, {0,0, 1}, 45), getViewport(1,0,3,2), "Bottom" };
        views[2] = { setupCamera({0,0, dist},   {0,0,0}, {0,1, 0}, 45), getViewport(2,0,3,2), "Front"  };
        views[3] = { setupCamera({0,0,-dist},   {0,0,0}, {0,1, 0}, 45), getViewport(0,1,3,2), "Back"   };
        views[4] = { setupCamera({dist,0, 0},   {0,0,0}, {0,1, 0}, 45), getViewport(1,1,3,2), "Right"  };
        views[5] = { setupCamera({-dist,0, 0},  {0,0,0}, {0,1, 0}, 45), getViewport(2,1,3,2), "Left"   };

        BeginDrawing();
        ClearBackground(BLACK);

        for (auto& v : views)
        {
            BeginScissorMode(v.viewport.x, v.viewport.y, v.viewport.width, v.viewport.height);
            BeginMode3D(v.cam);
                for (int i = 0; i < (int)snap.size(); i++)
                    drawZoneCube(snap[i], i, snap.size());
            EndMode3D();

            for (auto& z : snap)
                drawZoneLabel(z, v.cam);

            EndScissorMode();
        }

        EndDrawing();
    }

    CloseWindow();

    return 0;
}