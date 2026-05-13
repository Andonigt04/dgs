#include "include/dgs/types.h"
#include "include/dgs/network.h"
#include "include/dgs/packet.h"

#include <raylib.h>
#include <sys/socket.h>
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
    FRONT_BACK = 3,
    BACK_FRONT = 4,
    RIGHT_LEFT = 5,
    LEFT_RIGHT = 6
};

struct View {
    Camera3D cam;
    const char* label;
};

Camera3D setupCamera(Vector3 pos, Vector3 target, Vector3 up, float fov)
{
    Camera3D cam = {};
    cam.position = pos;
    cam.target   = target;
    cam.up       = up;
    cam.fovy     = fov;
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
    Color color = ColorFromHSV(i * (360.0f / total), 0.8f, 1.0f);
    DrawCube(center, size.x, size.y, size.z, ColorAlpha(color, 0.15f));
    DrawCubeWires(center, size.x, size.y, size.z, color);
}

void drawZoneLabel(const DGS::ZoneInfoPublic& z, Camera3D cam, int w, int h)
{
    Vector2 pos = GetWorldToScreenEx(zoneCenter(z), cam, w, h);
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

    const char* headHost = argc > 1 ? argv[1] : "127.0.0.1";
    int         headPort = argc > 2 ? std::atoi(argv[2]) : 42424;

    if (!tcpSocket.connect(headHost, headPort)) {
        std::cerr << "CAN NOT CONNECT TO HEAD " << headHost << ":" << headPort;
        return 1;
    }

    struct timeval tv { 3, 0 };
    setsockopt(tcpSocket.getSocketFD(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(tcpSocket.getSocketFD(), SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    std::thread netThread([&]()
    {
        DGS::Packet req;
        req.pack(DGS::PKT_ZONE_LIST);

        while (true)
        {
            try
            {
                tcpSocket.send(tcpSocket.getSocketFD(), req.getRawData(), req.getSize());

                uint8_t buf[8192];
                int bytes = tcpSocket.receive(tcpSocket.getSocketFD(), buf, sizeof(buf));
                if (bytes <= 0) {
                    std::cerr << "[Viewer] receive timeout/error bytes=" << bytes << " errno=" << errno << std::endl;
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                    continue;
                }

                DGS::Packet resp;
                resp.setBuffer(buf, bytes);
                auto r = resp.unpackZoneListResponse();
                std::cout << "[Viewer] Zonas recibidas: " << (int)r.count << std::endl;

                std::lock_guard<std::mutex> lock(mtx);
                zones.assign(r.zones, r.zones + r.count);
            }
            catch (const std::exception& e)
            {
                std::cerr << "[Viewer] Error en netThread: " << e.what() << std::endl;
            }

            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    });
    netThread.detach();

    const int COLS = 3, ROWS = 2;

    InitWindow(1280, 720, "DGS Viewer");
    SetTargetFPS(60);

    int vpW = GetScreenWidth()  / COLS;
    int vpH = GetScreenHeight() / ROWS;

    RenderTexture2D rts[6];
    for (int i = 0; i < 6; i++)
        rts[i] = LoadRenderTexture(vpW, vpH);

    View views[6] = {
        { {}, "Top"    },
        { {}, "Bottom" },
        { {}, "Front"  },
        { {}, "Back"   },
        { {}, "Right"  },
        { {}, "Left"   },
    };

    ViewMode currentMode = ALL;

    while (!WindowShouldClose())
    {
        if (IsKeyPressed(KEY_ZERO))  currentMode = ALL;
        if (IsKeyPressed(KEY_ONE))   currentMode = TOP_DOWN;
        if (IsKeyPressed(KEY_TWO))   currentMode = DOWN_TOP;
        if (IsKeyPressed(KEY_THREE)) currentMode = FRONT_BACK;
        if (IsKeyPressed(KEY_FOUR))  currentMode = BACK_FRONT;
        if (IsKeyPressed(KEY_FIVE))  currentMode = RIGHT_LEFT;
        if (IsKeyPressed(KEY_SIX))   currentMode = LEFT_RIGHT;

        std::vector<DGS::ZoneInfoPublic> snap;
        { std::lock_guard<std::mutex> lock(mtx); snap = zones; }

        float dist = snap.empty() ? 100.0f : worldSize(snap);

        Vector3 center = { 0, 0, 0 };
        if (!snap.empty())
        {
            float xMin = (float)snap[0].chunkXMin, xMax = (float)snap[0].chunkXMax;
            float yMin = (float)snap[0].chunkYMin, yMax = (float)snap[0].chunkYMax;
            float zMin = (float)snap[0].chunkZMin, zMax = (float)snap[0].chunkZMax;
            for (auto& z : snap) {
                xMin = std::min(xMin,(float)z.chunkXMin); xMax = std::max(xMax,(float)z.chunkXMax);
                yMin = std::min(yMin,(float)z.chunkYMin); yMax = std::max(yMax,(float)z.chunkYMax);
                zMin = std::min(zMin,(float)z.chunkZMin); zMax = std::max(zMax,(float)z.chunkZMax);
            }
            center = { (xMin+xMax)/2.0f, (yMin+yMax)/2.0f, (zMin+zMax)/2.0f };
        }

        views[0].cam = setupCamera({center.x, center.y+dist, center.z}, center, {0,0,-1}, dist);
        views[1].cam = setupCamera({center.x, center.y-dist, center.z}, center, {0,0, 1}, dist);
        views[2].cam = setupCamera({center.x, center.y, center.z+dist}, center, {0,1, 0}, dist);
        views[3].cam = setupCamera({center.x, center.y, center.z-dist}, center, {0,1, 0}, dist);
        views[4].cam = setupCamera({center.x+dist, center.y, center.z}, center, {0,1, 0}, dist);
        views[5].cam = setupCamera({center.x-dist, center.y, center.z}, center, {0,1, 0}, dist);

        auto renderScene = [&](Camera3D cam, int w, int h)
        {
            BeginMode3D(cam);
            for (int i = 0; i < (int)snap.size(); i++)
                drawZoneCube(snap[i], i, snap.size());
            EndMode3D();
            for (auto& z : snap)
                drawZoneLabel(z, cam, w, h);
        };

        BeginDrawing();
        ClearBackground(BLACK);

        if (currentMode == ALL)
        {
            for (int i = 0; i < 6; i++)
            {
                BeginTextureMode(rts[i]);
                ClearBackground(BLACK);
                renderScene(views[i].cam, vpW, vpH);
                DrawText(views[i].label, 4, 4, 12, GRAY);
                EndTextureMode();

                float x = (float)(i % COLS) * vpW;
                float y = (float)(i / COLS) * vpH;
                // Y flipped because OpenGL textures are bottom-up
                DrawTextureRec(rts[i].texture, { 0, 0, (float)vpW, -(float)vpH }, { x, y }, WHITE);
            }
        }
        else
        {
            int idx = currentMode - 1;
            int sw = GetScreenWidth(), sh = GetScreenHeight();
            renderScene(views[idx].cam, sw, sh);
            DrawText(views[idx].label, 4, 4, 16, GRAY);
        }

        EndDrawing();
    }

    for (int i = 0; i < 6; i++)
        UnloadRenderTexture(rts[i]);

    CloseWindow();
    return 0;
}
