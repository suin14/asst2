#include "tasksys.h"

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads) : ITaskSystem(num_threads) {}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads) : ITaskSystem(num_threads), num_threads_(num_threads) {
    threads_pool_ = std::make_unique<std::thread[]>(num_threads);
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::threadRun(IRunnable* runnable, int num_total_tasks, std::mutex* mutex, int* curr_task) {
    int turn = -1;
    while (turn < num_total_tasks) {
        mutex->lock();
        turn = *curr_task;
        *curr_task += 1;
        mutex->unlock();
        if (turn >= num_total_tasks) {
            break;
        }
        runnable->runTask(turn, num_total_tasks);
    }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    std::mutex mutex;
    int curr_task = 0;
    for (int i = 0; i < num_threads_; i++) {
        threads_pool_[i] = std::thread(&TaskSystemParallelSpawn::threadRun, this, runnable, num_total_tasks, &mutex, &curr_task);
    }
    for (int i = 0; i < num_threads_; i++) {
        threads_pool_[i].join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */
TasksState::TasksState()
        : mutex_(std::make_unique<std::mutex>()),
          finished_(std::make_unique<std::condition_variable>()),
          finishedMutex_(std::make_unique<std::mutex>()),
          runnable_(nullptr),
          finished_tasks_(-1),
          left_tasks_(-1),
          num_total_tasks_(-1) {}

TasksState::~TasksState() = default;

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads)
        : ITaskSystem(num_threads), num_threads_(num_threads), killed(false) {
    state_ = std::make_unique<TasksState>();
    threads_pool_ = std::make_unique<std::thread[]>(num_threads);
    for (int i = 0; i < num_threads; i++) {
        threads_pool_[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::spinningThread, this);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    killed = true;
    for (int i = 0; i < num_threads_; i++) {
        threads_pool_[i].join();
    }
}

void TaskSystemParallelThreadPoolSpinning::spinningThread() {
    int id;
    int total;
    while (true) {
        if (killed) break;
        state_->mutex_->lock();
        total = state_->num_total_tasks_;
        id = total - state_->left_tasks_;
        if (id < total) state_->left_tasks_--;
        state_->mutex_->unlock();
        if (id < total) {
            state_->runnable_->runTask(id, total);
            state_->mutex_->lock();
            state_->finished_tasks_++;
            if (state_->finished_tasks_ == total) {
                state_->mutex_->unlock();
                // this lock is necessary to ensure the main thread has gone to sleep
                state_->finishedMutex_->lock();
                state_->finishedMutex_->unlock();
                state_->finished_->notify_all();
            } else {
                state_->mutex_->unlock();
            }
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    std::unique_lock<std::mutex> lk(*(state_->finishedMutex_));
    state_->mutex_->lock();
    state_->runnable_ = runnable;
    state_->num_total_tasks_ = num_total_tasks;
    state_->left_tasks_ = num_total_tasks;
    state_->finished_tasks_ = 0;
    state_->mutex_->unlock();
    while (state_->finished_tasks_ < num_total_tasks) {
        state_->finished_->wait(lk);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads)
        : ITaskSystem(num_threads), num_threads_(num_threads), killed(false) {
    state_ = std::make_unique<TasksState>();
    threads_pool_ = std::make_unique<std::thread[]>(num_threads);
    hasTasks = std::make_unique<std::condition_variable>();
    hasTasksMutex = std::make_unique<std::mutex>();
    for (int i = 0; i < num_threads; i++) {
        threads_pool_[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::sleepingThread, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    killed = true;
    hasTasks->notify_all();
    for (int i = 0; i < num_threads_; i++) {
        threads_pool_[i].join();
    }
}

void TaskSystemParallelThreadPoolSleeping::sleepingThread() {
    int id;
    int total;
    while (true) {
        std::unique_lock<std::mutex> lk(*(hasTasksMutex));
        if (killed) break;
        while (!killed && state_->left_tasks_ <= 0) {
            hasTasks->wait(lk);
        }
        if (killed) break;
        state_->mutex_->lock();
        total = state_->num_total_tasks_;
        id = total - state_->left_tasks_;
        if (id < total) state_->left_tasks_--;
        state_->mutex_->unlock();
        if (id < total) {
            state_->runnable_->runTask(id, total);
            state_->mutex_->lock();
            state_->finished_tasks_++;
            if (state_->finished_tasks_ == total) {
                state_->mutex_->unlock();
                // this lock is necessary to ensure the main thread has gone to sleep
                state_->finishedMutex_->lock();
                state_->finishedMutex_->unlock();
                state_->finished_->notify_all();
            } else {
                state_->mutex_->unlock();
            }
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    std::unique_lock<std::mutex> lk(*(state_->finishedMutex_));
    state_->mutex_->lock();
    state_->runnable_ = runnable;
    state_->num_total_tasks_ = num_total_tasks;
    state_->left_tasks_ = num_total_tasks;
    state_->finished_tasks_ = 0;
    state_->mutex_->unlock();
    hasTasks->notify_all();
    while (state_->finished_tasks_ < num_total_tasks) {
        state_->finished_->wait(lk);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    return;
}
