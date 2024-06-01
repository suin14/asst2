#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <thread>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <queue>
#include <iostream>
#include <memory>
#include <vector>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial : public ITaskSystem {
public:
    TaskSystemSerial(int num_threads);
    ~TaskSystemSerial();
    const char* name() override;
    void run(IRunnable* runnable, int num_total_tasks) override;
    TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                            const std::vector<TaskID>& deps) override;
    void sync() override;
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn : public ITaskSystem {
public:
    TaskSystemParallelSpawn(int num_threads);
    ~TaskSystemParallelSpawn();
    const char* name() override;
    void run(IRunnable* runnable, int num_total_tasks) override;
    TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                            const std::vector<TaskID>& deps) override;
    void sync() override;

private:
    void threadRun(IRunnable* runnable, int num_total_tasks, std::mutex* mutex, int* curr_task);
    std::unique_ptr<std::thread[]> threads_pool_;
    int num_threads_;
};

/*
 * TasksState: This class is used to record the state of the
 * running task. Each worker thread and main thread must get
 * the lock first to read or write the attributes of this class.
 */
class TasksState {
public:
    TasksState();
    ~TasksState();

    std::unique_ptr<std::mutex> mutex_;
    std::unique_ptr<std::condition_variable> finished_;
    std::unique_ptr<std::mutex> finishedMutex_;
    IRunnable* runnable_;
    int finished_tasks_;
    int left_tasks_;
    int num_total_tasks_;
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning : public ITaskSystem {
public:
    TaskSystemParallelThreadPoolSpinning(int num_threads);
    ~TaskSystemParallelThreadPoolSpinning();
    const char* name() override;
    void run(IRunnable* runnable, int num_total_tasks) override;
    TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                            const std::vector<TaskID>& deps) override;
    void sync() override;

private:
    void spinningThread();
    std::unique_ptr<TasksState> state_;
    std::unique_ptr<std::thread[]> threads_pool_;
    bool killed;
    int num_threads_;
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping : public ITaskSystem {
public:
    TaskSystemParallelThreadPoolSleeping(int num_threads);
    ~TaskSystemParallelThreadPoolSleeping();
    const char* name() override;
    void run(IRunnable* runnable, int num_total_tasks) override;
    TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                            const std::vector<TaskID>& deps) override;
    void sync() override;

private:
    void sleepingThread();
    std::unique_ptr<TasksState> state_;
    std::unique_ptr<std::thread[]> threads_pool_;
    std::unique_ptr<std::condition_variable> hasTasks;
    std::unique_ptr<std::mutex> hasTasksMutex;
    bool killed;
    int num_threads_;
};

#endif
