#include <windows.h>
#include <iostream>
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


struct Request {
    int id;
    bool high;
    double enqueuedAt;
    double waited;
};


struct Queue {
    CRITICAL_SECTION cs;
    HANDLE items;
    std::deque<Request> q;
    int highRun;
};


Queue g_queues[STAGES + 1];
CRITICAL_SECTION g_printCs;
HANDLE g_allDone;
DWORD g_start;


void Log(const std::string& s) {
    EnterCriticalSection(&g_printCs);
    std::cout << "[" << (GetTickCount() - g_start) << " ms] " << s << std::endl;
    LeaveCriticalSection(&g_printCs);
}


double NowMs() {
    static LARGE_INTEGER freq = [] { LARGE_INTEGER f; QueryPerformanceFrequency(&f); return f; }();
    LARGE_INTEGER c;
    QueryPerformanceCounter(&c);
    return c.QuadPart * 1000.0 / freq.QuadPart;
}


void Push(Queue& Q, Request r) {
    r.enqueuedAt = NowMs();
    EnterCriticalSection(&Q.cs);
    Q.q.push_back(r);
    LeaveCriticalSection(&Q.cs);
    ReleaseSemaphore(Q.items, 1, NULL);
}


bool Pop(Queue& Q, Request& out) {
    WaitForSingleObject(Q.items, INFINITE);
    EnterCriticalSection(&Q.cs);

    if (Q.q.empty()) {
        LeaveCriticalSection(&Q.cs);
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
    srand(1000);
    for (int i = 1; i <= REQUESTS; i++) {
        Request r = { i, rand() % 100 < HIGH_SHARE, 0, 0 };
        Log("GEN       REQ " + std::to_string(i) + (r.high ? " HIGH" : " NORMAL"));
        Push(g_queues[0], r);
        Sleep(INTERVAL_MS);
    }
    return 0;
}


DWORD WINAPI Worker(LPVOID p) {
    WorkerArg* a = (WorkerArg*)p;
    Request r;
    while (Pop(g_queues[a->stage], r)) {
        Log("STAGE " + std::to_string(a->stage + 1) + " W" + std::to_string(a->id) +
            " TID=" + std::to_string(GetCurrentThreadId()) +
            " REQ " + std::to_string(r.id) + (r.high ? " HIGH" : " NORMAL"));
        Sleep(SERVICE_MS);
        Push(g_queues[a->stage + 1], r);
    }
    return 0;
}


DWORD WINAPI Collector(LPVOID) {
    int done = 0, cnt[2] = { 0, 0 };
    double wait[2] = { 0, 0 };
    Request r;
    while (Pop(g_queues[STAGES], r)) {
        int k = r.high ? 1 : 0;
        cnt[k]++;
        wait[k] += r.waited;
        done++;
        if (done == REQUESTS) {
            Log("DONE: " + std::to_string(done) + " requests");
            Log("avg wait HIGH   = " + std::to_string(cnt[1] ? wait[1] / cnt[1] : 0.0) + " ms");
            Log("avg wait NORMAL = " + std::to_string(cnt[0] ? wait[0] / cnt[0] : 0.0) + " ms");
            SetEvent(g_allDone);
        }
    }
    return 0;
}


int main() {
    InitializeCriticalSection(&g_printCs);
    for (int i = 0; i <= STAGES; i++) {
        InitializeCriticalSection(&g_queues[i].cs);
        g_queues[i].items = CreateSemaphoreA(NULL, 0, LONG_MAX, NULL);
        g_queues[i].highRun = 0;
    }
    g_allDone = CreateEventA(NULL, TRUE, FALSE, NULL);
    g_start = GetTickCount();

    const int N = 2 + STAGES * WORKERS;
    HANDLE threads[N];
    WorkerArg args[STAGES * WORKERS];
    int n = 0;

    threads[n++] = CreateThread(NULL, 0, Collector, NULL, 0, NULL);
    for (int s = 0; s < STAGES; s++) {
        for (int w = 0; w < WORKERS; w++) {
            args[s * WORKERS + w] = { s, w + 1 };
            threads[n++] = CreateThread(NULL, 0, Worker, &args[s * WORKERS + w], 0, NULL);
        }
    }
    threads[n++] = CreateThread(NULL, 0, Generator, NULL, 0, NULL);

    WaitForSingleObject(g_allDone, 60000);

    for (int i = 0; i <= STAGES; i++)
        ReleaseSemaphore(g_queues[i].items, WORKERS, NULL);
    WaitForMultipleObjects(N, threads, TRUE, 5000);

    for (int i = 0; i < N; i++) CloseHandle(threads[i]);
    for (int i = 0; i <= STAGES; i++) {
        CloseHandle(g_queues[i].items);
        DeleteCriticalSection(&g_queues[i].cs);
    }
    CloseHandle(g_allDone);
    DeleteCriticalSection(&g_printCs);
    return 0;
}