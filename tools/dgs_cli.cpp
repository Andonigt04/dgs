// dgs — instalador/deployer y CLI del DGS (§3.8, P8).
//
// Un solo binario decide el modo:
//   dgs run                  — portable: arranca el DGS standalone (todos los nodos locales, fork/exec)
//                              desde donde esté, sin instalar nada. Backend de spawn LOCAL.
//   dgs install [--systemd]  — instala binarios en /opt/dgs y opcionalmente unidades systemd.
//   dgs uninstall            — revierte `dgs install`.
//   dgs up [--terraform]     — cluster: aplica k8s (o provisiona infra con terraform y luego k8s).
//   dgs down                 — detiene el DGS (local: SIGTERM a los procesos; cluster: kubectl delete).
//   dgs status               — muestra estado (funciona igual en los 3 modos).
//   dgs logs [nodo]          — cola de logs del nodo (o de todos).
//
// Regla (§3.8): el comportamiento del DGS no depende del modo. El modo solo cambia el backend de spawn
// y la infra. El orquestador usa el mismo enum SpawnBackend (orchestrator.h); aquí el CLI solo lanza
// los procesos del standalone o delega en kubectl/terraform para el cluster.

#include <iostream>
#include <string>
#include <vector>
#include <cstring>
#include <cstdlib>
#include <csignal>
#include <unistd.h>
#include <sys/wait.h>
#include <fstream>
#include <sys/stat.h>
#include <sstream>
#include <algorithm>

static const char* INSTALL_DIR = "/opt/dgs";
static const char* SYSTEMD_DIR = "/etc/systemd/system";
static const std::vector<std::string> NODOS =
    { "head_server_node", "persistance_node", "cache_node",
      "validador_node",   "social_node",      "zone_node" };

// Puertos/roles para mensajes (solo informativo en el CLI).
static const std::vector<std::string> NODO_PUERTO =
    { "TCP:42424", "TCP:42429", "TCP:42425/42426", "UDP:42427/TCP:42428", "TCP:42430", "UDP:42425" };

static void usage()
{
    std::cout << "uso: dgs <comando> [opciones]\n"
              << "  run                    standalone portable (todos los nodos locales, sin instalar)\n"
              << "  install [--systemd]    instala binarios en " << INSTALL_DIR << " [+ unidades systemd]\n"
              << "  uninstall              revierte la instalacion\n"
              << "  up [--terraform]       cluster: aplica k8s (y opcionalmente provisiona terraform)\n"
              << "  down                   detiene el DGS (local o cluster)\n"
              << "  status                 estado del DGS\n"
              << "  logs [nodo]            logs de un nodo o de todos\n";
}

// --- utilidades ----------------------------------------------------------------------------------

static std::string envOr(const char* name, const std::string& def)
{
    const char* v = std::getenv(name);
    return v && *v ? std::string(v) : def;
}

static bool dirExists(const std::string& p)
{
    struct stat st;
    return stat(p.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

static bool mkdirP(const std::string& p)
{
    std::string cur;
    for (size_t i = 1; i < p.size(); ++i)
    {
        if (p[i] == '/') { if (!cur.empty() && !dirExists(cur)) if (mkdir(cur.c_str(), 0755)) return false; }
        cur += p[i];
    }
    return true;
}

static void runCmd(const std::string& cmd)
{
    std::cout << "[dgs] $ " << cmd << std::endl;
    int rc = system(cmd.c_str());
    if (rc != 0) std::cerr << "[dgs] advertencia: comando fallo (rc=" << rc << ")" << std::endl;
}

// --- run (standalone portable) -------------------------------------------------------------------

static pid_t g_children[64];
static int   g_nchildren = 0;

static void onSignal(int)
{
    for (int i = 0; i < g_nchildren; ++i)
        if (g_children[i] > 0) kill(g_children[i], SIGTERM);
}

static int cmdRun(const std::vector<std::string>& args)
{
    std::string binDir  = envOr("DGS_BIN_DIR", ".");
    std::string logDir  = envOr("DGS_LOG_DIR", "logs");
    mkdirP(logDir);

    // Env compartido standalone: todos los nodos se ven entre sí en 127.0.0.1.
    std::string headHost = envOr("HEAD_SERVER_HOST", "127.0.0.1");
    std::string headPort = envOr("HEAD_SERVER_PORT", "42424");
    std::string persHost = envOr("PERSISTENCE_HOST", "127.0.0.1");
    std::string persPort = envOr("PERSISTENCE_PORT", "42429");
    std::string validHost = envOr("VALIDATOR_HOST", "127.0.0.1");
    std::string validTcp = envOr("VALIDATOR_TCP_PORT", "42428");
    std::string socialHost = envOr("SOCIAL_HOST", "127.0.0.1");
    std::string socialPort = envOr("SOCIAL_TCP_PORT", "42430");

    std::cout << "[dgs] run: standalone en " << binDir << " (logs en " << logDir << ")" << std::endl;

    // Orden: head primero (los demás se conectan), luego el resto; la zona base arranca al final.
    std::vector<std::string> orden = NODOS;   // head, persistance, cache, validador, social, zone

    signal(SIGINT, onSignal);
    signal(SIGTERM, onSignal);

    for (size_t i = 0; i < orden.size(); ++i)
    {
        const std::string& bin  = orden[i];
        std::string path = binDir + "/" + bin;
        std::string log  = logDir + "/" + bin + ".log";

        pid_t pid = fork();
        if (pid < 0) { std::cerr << "[dgs] fork fallo para " << bin << std::endl; continue; }

        if (pid == 0)
        {
            // Hijo: redirige stdout/stderr al log y ejecuta el nodo.
            freopen(log.c_str(), "a", stdout);
            freopen(log.c_str(), "a", stderr);
            setenv("HEAD_SERVER_HOST", headHost.c_str(), 1);
            setenv("HEAD_SERVER_PORT", headPort.c_str(), 1);
            setenv("PERSISTENCE_HOST", persHost.c_str(), 1);
            setenv("PERSISTENCE_PORT", persPort.c_str(), 1);
            setenv("VALIDATOR_HOST",   validHost.c_str(), 1);
            setenv("VALIDATOR_TCP_PORT", validTcp.c_str(), 1);
            setenv("SOCIAL_HOST",      socialHost.c_str(), 1);
            setenv("SOCIAL_TCP_PORT",  socialPort.c_str(), 1);
            setenv("MY_POD_IP", "127.0.0.1", 1);
            execl(path.c_str(), path.c_str(), (char*)nullptr);
            std::cerr << "[dgs] no se pudo ejecutar " << path << std::endl;
            _exit(127);
        }

        g_children[g_nchildren++] = pid;
        std::cout << "[dgs] " << bin << " pid=" << pid << " " << NODO_PUERTO[i]
                  << "  log=" << log << std::endl;
        usleep(300000);   // dar tiempo a que cada nodo bindee antes del siguiente (orden de conexiones)
    }

    std::cout << "[dgs] standalone arriba. Ctrl-C para detener." << std::endl;
    while (true)
    {
        int st = 0;
        pid_t dead = waitpid(-1, &st, 0);
        if (dead > 0)
        {
            std::cout << "[dgs] nodo " << dead << " termino (rc=" << st << ")" << std::endl;
            bool allDead = true;
            for (int i = 0; i < g_nchildren; ++i)
                if (g_children[i] > 0 && g_children[i] != dead && kill(g_children[i], 0) == 0) allDead = false;
            if (allDead) break;
        }
        else break;
    }
    return 0;
}

// --- install / uninstall -------------------------------------------------------------------------

static int cmdInstall(const std::vector<std::string>& args)
{
    bool systemd = std::find(args.begin(), args.end(), "--systemd") != args.end();
    std::string binDir = envOr("DGS_BIN_DIR", ".");

    if (!mkdirP(INSTALL_DIR + std::string("/bin"))) { std::cerr << "[dgs] no puedo crear " << INSTALL_DIR << std::endl; return 1; }

    for (const auto& bin : NODOS)
    {
        std::string src = binDir + "/" + bin;
        std::string dst = std::string(INSTALL_DIR) + "/bin/" + bin;
        std::string cmd = "install -m 0755 " + src + " " + dst;
        int rc = system(cmd.c_str());
        if (rc != 0) std::cerr << "[dgs] advertencia: " << cmd << std::endl;
        else         std::cout << "[dgs] instalado " << dst << std::endl;
    }

    if (systemd)
    {
        std::cout << "[dgs] generando unidades systemd en " << SYSTEMD_DIR << std::endl;
        for (const auto& bin : NODOS)
        {
            std::ofstream u(std::string(SYSTEMD_DIR) + "/dgs-" + bin + ".service");
            u << "[Unit]\nDescription=DGS " << bin << "\nAfter=network.target\n\n"
              << "[Service]\nType=simple\nExecStart=" << INSTALL_DIR << "/bin/" << bin << "\n"
              << "Restart=always\nRestartSec=2\n"
              << "Environment=HEAD_SERVER_HOST=127.0.0.1\n"
              << "Environment=HEAD_SERVER_PORT=42424\n"
              << "Environment=PERSISTENCE_HOST=127.0.0.1\n"
              << "Environment=PERSISTENCE_PORT=42429\n"
              << "Environment=VALIDATOR_HOST=127.0.0.1\n"
              << "Environment=VALIDATOR_TCP_PORT=42428\n"
              << "Environment=SOCIAL_HOST=127.0.0.1\n"
              << "Environment=SOCIAL_TCP_PORT=42430\n\n"
              << "[Install]\nWantedBy=multi-user.target\n";
            u.close();
        }
        runCmd("systemctl daemon-reload");
        std::cout << "[dgs] lista: systemctl start dgs-head_server_node dgs-zone_node ..." << std::endl;
    }
    else
        std::cout << "[dgs] instalado sin systemd (usa `dgs run` o `--systemd` para daemons)" << std::endl;

    return 0;
}

static int cmdUninstall()
{
    std::string cmd = "rm -rf " + std::string(INSTALL_DIR);
    runCmd(cmd);
    for (const auto& bin : NODOS)
        runCmd("rm -f " + std::string(SYSTEMD_DIR) + "/dgs-" + bin + ".service");
    runCmd("systemctl daemon-reload");
    std::cout << "[dgs] desinstalado." << std::endl;
    return 0;
}

// --- up / down (cluster) -------------------------------------------------------------------------

static int cmdUp(const std::vector<std::string>& args)
{
    bool terraform = std::find(args.begin(), args.end(), "--terraform") != args.end();

    if (terraform)
    {
        std::cout << "[dgs] provisionando infra con terraform..." << std::endl;
        runCmd("terraform -chdir=terraform init -input=false");
        runCmd("terraform -chdir=terraform apply -auto-approve");
    }

    std::cout << "[dgs] aplicando k8s..." << std::endl;
    runCmd("kubectl apply -f k8s/namespace.yaml");
    runCmd("kubectl apply -f k8s/mongodb");
    runCmd("kubectl apply -f k8s/persistence");
    runCmd("kubectl apply -f k8s/cache");
    runCmd("kubectl apply -f k8s/validador");
    runCmd("kubectl apply -f k8s/head-server");
    runCmd("kubectl apply -f k8s/zone-node");
    runCmd("kubectl rollout status deployment/zone-node -n dgs");
    return 0;
}

static int cmdDown()
{
    // Local: si hay procesos del standalone (dgs run), se matan. Cluster: kubectl delete.
    std::string pidfile = envOr("DGS_PIDFILE", "");
    (void)pidfile;
    for (const auto& bin : NODOS)
        runCmd("pkill -f " + bin + " 2>/dev/null || true");
    runCmd("kubectl delete -f k8s/zone-node -f k8s/head-server -f k8s/validador "
           "-f k8s/cache -f k8s/persistence -f k8s/mongodb --ignore-not-found 2>/dev/null || true");
    std::cout << "[dgs] down: nodos locales detenidos / cluster eliminado." << std::endl;
    return 0;
}

// --- status / logs -------------------------------------------------------------------------------

static int cmdStatus()
{
    bool haveKubectl = system("command -v kubectl >/dev/null 2>&1") == 0;
    std::cout << "[dgs] estado:" << std::endl;

    for (const auto& bin : NODOS)
    {
        std::string cmd = "pgrep -f '" + bin + "' | head -1";
        FILE* fp = popen(cmd.c_str(), "r");
        std::string pid;
        if (fp) { char buf[64]; if (fgets(buf, sizeof(buf), fp)) pid = buf; pclose(fp); }
        std::cout << "  " << bin << ": "
                  << (pid.empty() ? "no ejecutandose (local)" : "pid=" + pid);
        std::cout << std::endl;
    }

    if (haveKubectl)
        runCmd("kubectl get pods -n dgs");
    return 0;
}

static int cmdLogs(const std::vector<std::string>& args)
{
    std::string logDir = envOr("DGS_LOG_DIR", "logs");
    if (args.empty())
    {
        for (const auto& bin : NODOS)
        {
            std::string log = logDir + "/" + bin + ".log";
            std::ifstream f(log);
            if (!f) continue;
            std::cout << "--- " << bin << " ---" << std::endl;
            std::string line;
            while (std::getline(f, line)) std::cout << line << std::endl;
        }
        return 0;
    }

    std::string log = logDir + "/" + args[0] + ".log";
    runCmd("tail -f " + log);
    return 0;
}

// --- main ----------------------------------------------------------------------------------------

int main(int argc, char** argv)
{
    if (argc < 2) { usage(); return 1; }
    std::string cmd = argv[1];
    std::vector<std::string> args;
    for (int i = 2; i < argc; ++i) args.push_back(argv[i]);

    if      (cmd == "run")       return cmdRun(args);
    else if (cmd == "install")   return cmdInstall(args);
    else if (cmd == "uninstall") return cmdUninstall();
    else if (cmd == "up")        return cmdUp(args);
    else if (cmd == "down")      return cmdDown();
    else if (cmd == "status")    return cmdStatus();
    else if (cmd == "logs")      return cmdLogs(args);
    else if (cmd == "--help" || cmd == "-h" || cmd == "help") { usage(); return 0; }

    std::cerr << "[dgs] comando desconocido: " << cmd << std::endl;
    usage();
    return 1;
}
