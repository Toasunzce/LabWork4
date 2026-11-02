#include <windows.h>
#include <mmsystem.h>
#include <iostream>
#include <cstdio>
#include <cstring>
#include <string>
#include <cstdlib>
#include <deque>
#include <vector>
#include <algorithm>

#pragma comment(lib, "winmm.lib")


const int STAGES = 3;
const int MAX_HIGH_RUN = 3;
const int MAX_WORKERS = 20;
const DWORD JOIN_TIMEOUT_MS = 5000;

int REQUESTS = 500;
int HIGH_SHARE = 20;
int WORKERS = 2;
int SERVICE_MS = 10;
int INTERVAL_MS = 2;
int SEED = 1000;
int RUNS = 1;
bool PRIORITY_MODE = true;
bool VERBOSE = true;

int g_curSeed = 1000;


struct Request {
    int id;
    bool high;
    double enqueuedAt;
    double waited;
    unsigned long long seq;
    double service;
};


struct Queue {
    CRITICAL_SECTION cs;
    bool csReady;
    bool stageQueue;
    HANDLE items;
    std::deque<Request> high;
    std::deque<Request> normal;
    unsigned long long nextSeq;
    int highRun;
};


struct Stats {
    int completed[2];
    double wait[2];
    double maxWait[2];
    double service[2];
    double totalMs;
};


Queue g_queues[STAGES + 1];
CRITICAL_SECTION g_printCs;
HANDLE g_allDone = NULL;
HANDLE g_stop = NULL;
volatile LONG g_failed = 0;
volatile LONG g_created = 0;
volatile LONG g_rejected = 0;
Stats g_stats;
double g_busyMs[STAGES * MAX_WORKERS];
double g_startMs;


double NowMs() {
    static LARGE_INTEGER freq = [] { LARGE_INTEGER f; QueryPerformanceFrequency(&f); return f; }();
    LARGE_INTEGER c;
    QueryPerformanceCounter(&c);
    return c.QuadPart * 1000.0 / freq.QuadPart;
}


void Print(const std::string& s) {
    EnterCriticalSection(&g_printCs);
    std::cout << s << std::flush;
    LeaveCriticalSection(&g_printCs);
}


std::string Fmt(double v) {
    char b[32];
    sprintf_s(b, "%.2f", v);
    return b;
}


std::string CsvNum(double v) {
    char b[32];
    if (v == (double)(long long)v) sprintf_s(b, "%lld", (long long)v);
    else sprintf_s(b, "%.3f", v);
    return b;
}


bool ParseArgs(int argc, char* argv[]) {
    if ((argc - 1) % 2 != 0) {
        Print("Error: every parameter needs a value\n");
        return false;
    }
    for (int i = 1; i < argc; i += 2) {
        std::string key = argv[i];
        std::string val = argv[i + 1];
        int n = atoi(val.c_str());

        if (key == "--mode") {
            if (val == "fifo") PRIORITY_MODE = false;
            else if (val == "priority") PRIORITY_MODE = true;
            else {
                Print("Error: --mode must be fifo or priority\n");
                return false;
            }
        }
        else if (key == "--high-share") HIGH_SHARE = n;
        else if (key == "--requests") REQUESTS = n;
        else if (key == "--workers") WORKERS = n;
        else if (key == "--seed") SEED = n;
        else if (key == "--service-ms") SERVICE_MS = n;
        else if (key == "--interval-ms") INTERVAL_MS = n;
        else if (key == "--runs") RUNS = n;
        else if (key == "--verbose") VERBOSE = (n != 0);
        else {
            Print("Error: unknown parameter " + key + "\n");
            return false;
        }
    }
    if (WORKERS < 1 || WORKERS > MAX_WORKERS) {
        Print("Error: workers must be 1.." + std::to_string(MAX_WORKERS) + "\n");
        return false;
    }
    if (RUNS < 1) {
        Print("Error: runs must be at least 1\n");
        return false;
    }
    return true;
}


void PrintError(const std::string& op) {
    DWORD statusCode = GetLastError();
    char formatedMsg[256] = "";
    FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
        NULL, statusCode, MAKELANGID(LANG_ENGLISH, SUBLANG_ENGLISH_US),
        formatedMsg, sizeof(formatedMsg), NULL);

    std::string msg = formatedMsg;
    while (!msg.empty() && (msg.back() == '\n' || msg.back() == '\r' || msg.back() == ' '))
        msg.pop_back();

    Print("Error: " + op + ", code " + std::to_string(statusCode) + ", " + msg + "\n");
}


void Abort() {
    InterlockedExchange(&g_failed, 1);
    if (g_stop) SetEvent(g_stop);
}


std::string Tag(const std::string& role, int id, const std::string& extra = "") {
    return role + " " + std::to_string(id) +
        " | TID=" + std::to_string(GetCurrentThreadId()) +
        (extra.empty() ? "" : " | " + extra);
}


std::string ReqStr(const Request& r) {
    return "REQ=" + std::to_string(r.id) + (r.high ? " HIGH" : " NORMAL");
}


void Log(const std::string& tag, const std::string& event,
    const std::string& details = "", bool always = false) {
    if (!VERBOSE && !always) return;

    EnterCriticalSection(&g_printCs);
    char t[32];
    sprintf_s(t, "[%06d ms] ", (int)(NowMs() - g_startMs));
    std::cout << t << tag << " | " << event
        << (details.empty() ? "" : " " + details) << std::endl;
    LeaveCriticalSection(&g_printCs);
}


bool Delay(DWORD ms) {
    DWORD r = WaitForSingleObject(g_stop, ms);
    if (r == WAIT_TIMEOUT) return true;
    if (r == WAIT_FAILED) {
        PrintError("WaitForSingleObject(stop)");
        Abort();
    }
    return false;
}


bool Push(Queue& Q, Request r) {
    r.enqueuedAt = NowMs();
    EnterCriticalSection(&Q.cs);
    r.seq = Q.nextSeq++;
    (r.high ? Q.high : Q.normal).push_back(r);
    LeaveCriticalSection(&Q.cs);
    if (!ReleaseSemaphore(Q.items, 1, NULL)) {
        PrintError("ReleaseSemaphore");
        Abort();
        return false;
    }
    return true;
}


bool Pop(Queue& Q, Request& out) {
    HANDLE h[2] = { g_stop, Q.items };
    DWORD r = WaitForMultipleObjects(2, h, FALSE, INFINITE);
    if (r == WAIT_OBJECT_0) return false;
    if (r != WAIT_OBJECT_0 + 1) {
        PrintError("WaitForMultipleObjects(queue)");
        Abort();
        return false;
    }

    EnterCriticalSection(&Q.cs);
    if (Q.high.empty() && Q.normal.empty()) {
        LeaveCriticalSection(&Q.cs);
        Print("Error: queue is empty after semaphore wait\n");
        Abort();
        return false;
    }

    bool usePriority = PRIORITY_MODE && Q.stageQueue;
    std::deque<Request>* src;
    if (Q.normal.empty())
        src = &Q.high;
    else if (Q.high.empty())
        src = &Q.normal;
    else if (usePriority)
        src = (Q.highRun >= MAX_HIGH_RUN) ? &Q.normal : &Q.high;
    else
        src = (Q.high.front().seq < Q.normal.front().seq) ? &Q.high : &Q.normal;

    if (usePriority) Q.highRun = (src == &Q.high) ? Q.highRun + 1 : 0;
    out = src->front();
    src->pop_front();
    LeaveCriticalSection(&Q.cs);

    if (Q.stageQueue) out.waited += NowMs() - out.enqueuedAt;

    return true;
}


struct WorkerArg { int stage; int id; };


DWORD WINAPI Generator(LPVOID) {
    std::string tag = Tag("GENERATOR", 1);
    Log(tag, "STATE=WORKING");
    srand(g_curSeed);
    for (int i = 1; i <= REQUESTS; i++) {
        Request r = { i, rand() % 100 < HIGH_SHARE, 0, 0 };
        Log(tag, "CREATED", ReqStr(r));
        if (!Push(g_queues[0], r)) break;
        InterlockedIncrement(&g_created);
        if (!Delay(INTERVAL_MS)) break;
    }
    Log(tag, "STATE=FINISHED");
    return 0;
}


DWORD WINAPI Worker(LPVOID p) {
    WorkerArg* a = (WorkerArg*)p;
    int uid = a->stage * WORKERS + a->id;
    std::string tag = Tag("WORKER", uid, "STAGE=" + std::to_string(a->stage + 1));

    Request r;
    while (true) {
        Log(tag, "STATE=WAITING");
        if (!Pop(g_queues[a->stage], r)) break;

        Log(tag, "STATE=WORKING", ReqStr(r));
        double t0 = NowMs();
        if (!Delay(SERVICE_MS)) break;
        double spent = NowMs() - t0;
        r.service += spent;
        g_busyMs[uid - 1] += spent;

        Log(tag, "PASSED", ReqStr(r));
        if (!Push(g_queues[a->stage + 1], r)) break;
    }
    Log(tag, "STATE=FINISHED");
    return 0;
}


DWORD WINAPI Collector(LPVOID) {
    std::string tag = Tag("COLLECTOR", 1);
    int done = 0;
    Request r;
    while (true) {
        Log(tag, "STATE=WAITING");
        if (!Pop(g_queues[STAGES], r)) break;

        Log(tag, "RECEIVED", ReqStr(r));
        int k = r.high ? 1 : 0;
        g_stats.completed[k]++;
        g_stats.wait[k] += r.waited;
        g_stats.service[k] += r.service;
        if (r.waited > g_stats.maxWait[k]) g_stats.maxWait[k] = r.waited;
        done++;

        if (done == REQUESTS) {
            g_stats.totalMs = NowMs() - g_startMs;
            if (!SetEvent(g_allDone)) {
                PrintError("SetEvent(allDone)");
                Abort();
            }
            break;
        }
    }
    Log(tag, "STATE=FINISHED");
    return 0;
}


bool InitSync() {
    g_stop = CreateEventA(NULL, TRUE, FALSE, NULL);
    if (!g_stop) {
        PrintError("CreateEvent(stop)");
        Abort();
        return false;
    }

    g_allDone = CreateEventA(NULL, TRUE, FALSE, NULL);
    if (!g_allDone) {
        PrintError("CreateEvent(allDone)");
        Abort();
        return false;
    }

    for (int i = 0; i <= STAGES; i++) {
        InitializeCriticalSection(&g_queues[i].cs);
        g_queues[i].csReady = true;
        g_queues[i].stageQueue = (i < STAGES);
        g_queues[i].highRun = 0;
        g_queues[i].nextSeq = 0;
        g_queues[i].high.clear();
        g_queues[i].normal.clear();
        g_queues[i].items = CreateSemaphoreA(NULL, 0, LONG_MAX, NULL);
        if (!g_queues[i].items) {
            PrintError("CreateSemaphore(queue " + std::to_string(i) + ")");
            Abort();
            return false;
        }
    }
    return true;
}


void Cleanup() {
    for (int i = 0; i <= STAGES; i++) {
        if (g_queues[i].items) { CloseHandle(g_queues[i].items); g_queues[i].items = NULL; }
        if (g_queues[i].csReady) { DeleteCriticalSection(&g_queues[i].cs); g_queues[i].csReady = false; }
    }
    if (g_allDone) { CloseHandle(g_allDone); g_allDone = NULL; }
    if (g_stop) { CloseHandle(g_stop); g_stop = NULL; }
}


bool RunOnce(bool& joined) {
    joined = true;

    HANDLE threads[2 + STAGES * MAX_WORKERS];
    WorkerArg args[STAGES * MAX_WORKERS];
    int n = 0;

    if (InitSync()) {
        auto start = [&](LPTHREAD_START_ROUTINE fn, LPVOID arg) -> bool {
            HANDLE h = CreateThread(NULL, 0, fn, arg, 0, NULL);
            if (!h) {
                PrintError("CreateThread");
                Abort();
                return false;
            }
            threads[n++] = h;
            return true;
            };

        bool ok = start(Collector, NULL);
        for (int s = 0; ok && s < STAGES; s++) {
            for (int w = 0; ok && w < WORKERS; w++) {
                args[s * WORKERS + w] = { s, w + 1 };
                ok = start(Worker, &args[s * WORKERS + w]);
            }
        }
        if (ok) ok = start(Generator, NULL);

        if (ok) {
            HANDLE ev[2] = { g_allDone, g_stop };
            DWORD r = WaitForMultipleObjects(2, ev, FALSE, INFINITE);
            if (r == WAIT_FAILED) {
                PrintError("WaitForMultipleObjects(main)");
                Abort();
            }
        }

        if (!SetEvent(g_stop)) PrintError("SetEvent(stop)");

        if (n > 0) {
            DWORD jr = WaitForMultipleObjects(n, threads, TRUE, JOIN_TIMEOUT_MS);
            if (jr == WAIT_TIMEOUT) {
                joined = false;
                Print("Error: threads did not finish within " + std::to_string(JOIN_TIMEOUT_MS) + " ms\n");
                InterlockedExchange(&g_failed, 1);
            }
            else if (jr == WAIT_FAILED) {
                joined = false;
                PrintError("WaitForMultipleObjects(join)");
                InterlockedExchange(&g_failed, 1);
            }
        }
        for (int i = 0; i < n; i++) CloseHandle(threads[i]);
    }

    return g_failed == 0;
}


std::vector<std::string> MetricNames() {
    std::vector<std::string> n = {
        "created", "completed", "rejected", "check_ok", "total_ms", "throughput",
        "high_n", "high_avg_wait", "high_max_wait", "high_avg_service",
        "normal_n", "normal_avg_wait", "normal_max_wait", "normal_avg_service"
    };
    for (int st = 1; st <= STAGES; st++) n.push_back("load_stage" + std::to_string(st));
    return n;
}


std::vector<double> CollectResult() {
    int created = (int)g_created;
    int rejected = (int)g_rejected;
    int done = g_stats.completed[0] + g_stats.completed[1];
    double sec = g_stats.totalMs / 1000.0;

    std::vector<double> v;
    v.push_back(created);
    v.push_back(done);
    v.push_back(rejected);
    v.push_back(created == done + rejected ? 1 : 0);
    v.push_back(g_stats.totalMs);
    v.push_back(sec > 0 ? done / sec : 0.0);

    for (int k = 1; k >= 0; k--) {
        int n = g_stats.completed[k];
        v.push_back(n);
        v.push_back(n ? g_stats.wait[k] / n : 0.0);
        v.push_back(g_stats.maxWait[k]);
        v.push_back(n ? g_stats.service[k] / n : 0.0);
    }

    for (int st = 0; st < STAGES; st++) {
        double busy = 0;
        for (int w = 0; w < WORKERS; w++) busy += g_busyMs[st * WORKERS + w];
        v.push_back(g_stats.totalMs > 0 ? busy / (WORKERS * g_stats.totalMs) * 100.0 : 0.0);
    }
    return v;
}


double Median(std::vector<double> v) {
    std::sort(v.begin(), v.end());
    size_t n = v.size();
    return (n % 2) ? v[n / 2] : (v[n / 2 - 1] + v[n / 2]) / 2.0;
}


void PrintSummary(const std::vector<std::vector<double>>& all) {
    std::vector<std::string> names = MetricNames();
    std::string s = "\n=== SUMMARY over " + std::to_string(all.size()) + " runs ===\n";
    for (size_t m = 0; m < names.size(); m++) {
        std::vector<double> col;
        double sum = 0;
        for (size_t r = 0; r < all.size(); r++) {
            col.push_back(all[r][m]);
            sum += all[r][m];
        }
        s += names[m] + ": avg=" + Fmt(sum / col.size()) + " median=" + Fmt(Median(col)) + "\n";
    }
    Print(s);
}


int main(int argc, char* argv[]) {

    InitializeCriticalSection(&g_printCs);
    if (!ParseArgs(argc, argv)) {
        DeleteCriticalSection(&g_printCs);
        return 2;
    }

    if (!CreateDirectoryA("data", NULL) && GetLastError() != ERROR_ALREADY_EXISTS) {
        PrintError("CreateDirectory(data)");
        DeleteCriticalSection(&g_printCs);
        return 1;
    }
    SYSTEMTIME st;
    GetLocalTime(&st);
    char stamp[32];
    sprintf_s(stamp, "%04d%02d%02d_%02d%02d%02d",
        st.wYear, st.wMonth, st.wDay, st.wHour, st.wMinute, st.wSecond);
    std::string path = std::string("data\\") + (PRIORITY_MODE ? "priority" : "fifo") +
        "_h" + std::to_string(HIGH_SHARE) + "_n" + std::to_string(REQUESTS) +
        "_" + stamp + ".csv";

    FILE* csv = NULL;
    if (fopen_s(&csv, path.c_str(), "w") != 0 || !csv) {
        Print("Error: cannot open " + path + "\n");
        DeleteCriticalSection(&g_printCs);
        return 1;
    }
    std::string header;
    std::vector<std::string> names = MetricNames();
    for (size_t i = 0; i < names.size(); i++) header += (i ? "," : "") + names[i];
    fprintf(csv, "%s\n", header.c_str());
    fflush(csv);

    bool timerSet = (timeBeginPeriod(1) == TIMERR_NOERROR);
    if (!timerSet) Print("Warning: timeBeginPeriod(1) failed, delays will be coarse\n");

    std::string mainTag = "MAIN | PID=" + std::to_string(GetCurrentProcessId());
    g_startMs = NowMs();
    Log(mainTag, "CONFIG",
        std::string("mode=") + (PRIORITY_MODE ? "priority" : "fifo") +
        " high-share=" + std::to_string(HIGH_SHARE) +
        " requests=" + std::to_string(REQUESTS) +
        " workers=" + std::to_string(WORKERS) +
        " seed=" + std::to_string(SEED) +
        " service-ms=" + std::to_string(SERVICE_MS) +
        " interval-ms=" + std::to_string(INTERVAL_MS) +
        " runs=" + std::to_string(RUNS), true);
    Log(mainTag, "OUTPUT", path, true);

    std::vector<std::vector<double>> all;
    bool failed = false;
    bool joined = true;

    for (int run = 1; run <= RUNS; run++) {
        memset(&g_stats, 0, sizeof(g_stats));
        memset(g_busyMs, 0, sizeof(g_busyMs));
        g_created = 0;
        g_rejected = 0;
        g_failed = 0;
        g_curSeed = SEED + run - 1;
        g_startMs = NowMs();

        bool ok = RunOnce(joined);
        if (!joined) {
            failed = true;
            break;
        }
        Cleanup();
        if (!ok) {
            failed = true;
            break;
        }

        std::vector<double> v = CollectResult();
        std::string row;
        for (size_t i = 0; i < v.size(); i++) row += (i ? "," : "") + CsvNum(v[i]);
        fprintf(csv, "%s\n", row.c_str());
        fflush(csv);
        all.push_back(v);

        Print("run " + std::to_string(run) + "/" + std::to_string(RUNS) +
            ": total=" + Fmt(v[4]) + " ms, check=" + (v[3] == 1 ? "OK" : "FAIL") + "\n");
    }

    fclose(csv);
    if (!failed) PrintSummary(all);
    Log(mainTag, "FINISHED", std::string("status=") + (failed ? "ERROR" : "OK"), true);

    if (joined) DeleteCriticalSection(&g_printCs);
    if (timerSet) timeEndPeriod(1);
    return failed ? 1 : 0;
}


// parameters: --mode fifo --high-share 70 --requests 910 --seed 8954 --runs 100 --verbose 0
/*
TODO: remove irrelevant metrics such as load_stage_1, etc
      push to git
      do experiments
      build graphics
      write report
*/