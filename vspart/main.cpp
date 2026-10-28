#include <windows.h>
#include <iostream>
#include <cstdio>
#include <string>
#include <deque>
#include <algorithm>


const int REQUESTS = 30;
const int HIGH_SHARE = 80; // high-%
const int STAGES = 3;
const int WORKERS = 2;
const int SERVICE_MS = 10;
const int INTERVAL_MS = 2;
const bool PRIORITY_MODE = true;
const int MAX_HIGH_RUN = 3;
const bool VERBOSE = true;
const DWORD JOIN_TIMEOUT_MS = 5000;


struct Request {
    int id;
    bool high;
    double enqueuedAt;
    double waited;
};


struct Queue {
    CRITICAL_SECTION cs;
    bool csReady;
    HANDLE items;
    std::deque<Request> q;
    int highRun;
};


Queue g_queues[STAGES + 1];
CRITICAL_SECTION g_printCs;
HANDLE g_allDone = NULL;
HANDLE g_stop = NULL;
volatile LONG g_failed = 0;
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


// Фиксирует сбой и останавливает всех участников
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
    Q.q.push_back(r);
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
    if (Q.q.empty()) {
        LeaveCriticalSection(&Q.cs);
        Print("Error: queue is empty after semaphore wait\n");
        Abort();
        return false;
    }

    auto it = Q.q.begin();
    if (PRIORITY_MODE) {
        auto high = std::find_if(
            Q.q.begin(), Q.q.end(), [](const Request& r) { return r.high; }
        );
        auto normal = std::find_if(
            Q.q.begin(), Q.q.end(), [](const Request& r) { return !r.high; }
        );
        bool takeNormal = (
            high == Q.q.end()) || (Q.highRun >= MAX_HIGH_RUN && normal != Q.q.end()
                );
        it = takeNormal ? normal : high;
        Q.highRun = it->high ? Q.highRun + 1 : 0;
    }
    out = *it;
    Q.q.erase(it);
    LeaveCriticalSection(&Q.cs);

    out.waited += NowMs() - out.enqueuedAt;
    return true;
}


struct WorkerArg { int stage; int id; };


DWORD WINAPI Generator(LPVOID) {
    std::string tag = Tag("GENERATOR", 1);
    Log(tag, "STATE=WORKING");
    srand(1000);
    for (int i = 1; i <= REQUESTS; i++) {
        Request r = { i, rand() % 100 < HIGH_SHARE, 0, 0 };
        Log(tag, "CREATED", ReqStr(r));
        if (!Push(g_queues[0], r)) break;
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
        if (!Delay(SERVICE_MS)) break;
        if (!Push(g_queues[a->stage + 1], r)) break;
        Log(tag, "PASSED", ReqStr(r));
    }
    Log(tag, "STATE=FINISHED");
    return 0;
}


DWORD WINAPI Collector(LPVOID) {
    std::string tag = Tag("COLLECTOR", 1);
    int done = 0, cnt[2] = { 0, 0 };
    double wait[2] = { 0, 0 };
    Request r;
    while (true) {
        Log(tag, "STATE=WAITING");
        if (!Pop(g_queues[STAGES], r)) break;

        Log(tag, "RECEIVED", ReqStr(r));
        int k = r.high ? 1 : 0;
        cnt[k]++;
        wait[k] += r.waited;
        done++;
        if (done == REQUESTS) {
            Log(tag, "RESULT", "completed=" + std::to_string(done), true);
            Log(tag, "RESULT", "avg wait HIGH   = " +
                std::to_string(cnt[1] ? wait[1] / cnt[1] : 0.0) + " ms", true);
            Log(tag, "RESULT", "avg wait NORMAL = " +
                std::to_string(cnt[0] ? wait[0] / cnt[0] : 0.0) + " ms", true);
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


// Создание объектов синхронизации. При ошибке возвращает false, остальное освобождает Cleanup.
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
        g_queues[i].highRun = 0;
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


int main() {
    InitializeCriticalSection(&g_printCs);
    g_startMs = NowMs();
    std::string mainTag = "MAIN | PID=" + std::to_string(GetCurrentProcessId());
    Log(mainTag, "STARTED");

    const int N = 2 + STAGES * WORKERS;
    HANDLE threads[N];
    WorkerArg args[STAGES * WORKERS];
    int n = 0;
    bool joined = true;

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

    bool failed = (g_failed != 0);
    Log(mainTag, "FINISHED", std::string("status=") + (failed ? "ERROR" : "OK"), true);

    if (joined) {
        Cleanup();
        DeleteCriticalSection(&g_printCs);
    }
    return failed ? 1 : 0;
}