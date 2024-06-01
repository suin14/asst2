# Assignment 2: Building A Task Execution Library from the Ground Up 从头开始构建任务执行库

> 姓名：温舒盈			学号：71066002

## Part A: Synchronous Bulk Task Launch 同步批量任务启动

Your job is to implement a task execution engine that efficiently uses your multi-core CPU. You will be graded on both the correctness of your implementation (it must run all the tasks correctly) as well as on its performance. This should be a fun coding challenge, but it is a non-trivial piece of work. To help you stay on the right track, to complete Part A of the assignment, we will have you implement multiple versions of the task system, slowly increasing in complexity and performance of your implementation. Your three implementations will be in the classes defined in `tasksys.cpp/.h`.
你的工作是实现一个任务执行引擎，有效地使用你的多核CPU。你将根据你的实现的正确性（它必须正确地运行所有的任务）以及它的性能来评分。这应该是一个有趣的编码挑战，但它是一个不平凡的工作。为了帮助您保持正确的轨道，完成任务的A部分，我们将让您实现任务系统的多个版本，慢慢增加您实现的复杂性和性能。您的三个实现将在 `tasksys.cpp/.h` 中定义的类中。

- `TaskSystemParallelSpawn`
- `TaskSystemParallelThreadPoolSpinning`
- `TaskSystemParallelThreadPoolSleeping`

**Implement your part A implementation in the `part_a/` sub-directory to compare to the correct reference implementation (`part_a/runtasks_ref_\*`).
在 `part_a/` 子目录中实现您的A部分实现，以与正确的参考实现（ `part_a/runtasks_ref_\*` ）进行比较。**

*Pro tip: Notice how the instructions below take the approach of "try the simplest improvement first". Each step increases the complexity of the task execution system's implementation, but on each step along the way you should have a working (fully correct) task runtime system.
专业提示：注意下面的说明是如何采取“先尝试最简单的改进”的方法的。每一步都增加了任务执行系统实现的复杂性，但是在沿着的每一步中，您都应该有一个工作的（完全正确的）任务运行时系统。*

We also expect you to create at least one test, which can test either correctness or performance. See the Running Tests section above for more information.
我们还希望您至少创建一个测试，可以测试正确性或性能。有关详细信息，请参阅上面的“运行测试”部分。

#### Step 1: Move to a Parallel Task System 第一步：转移到并行任务系统

**In this step please implement the class `TaskSystemParallelSpawn`.
在这一步中，请实现类 `TaskSystemParallelSpawn` 。**

The starter code provides you a working serial implementation of the task system in `TaskSystemSerial`. In this step of the assignment you will extend the starter code to execute a bulk task launch in parallel.
starter代码为您提供了 `TaskSystemSerial` 中任务系统的工作串行实现。在任务的这一步中，您将扩展启动器代码以并行执行批量任务启动。

- You will need to create additional threads of control to perform the work of a bulk task launch. Notice that `TaskSystem`'s constructor is provided a parameter `num_threads` which is the **maximum number of worker threads** your implementation may use to run tasks.
  您将需要创建额外的控制线程来执行批量任务启动的工作。请注意， `TaskSystem` 的构造函数提供了一个参数 `num_threads` ，这是您的实现可以用于运行任务的最大工作线程数。

- In the spirit of "do the simplest thing first", we recommend that you spawn worker threads at the beginning of `run()` and join these threads from the main thread before `run()` returns. This will be a correct implementation, but it will incur significant overhead from frequent thread creation.
  本着“先做最简单的事情”的精神，我们建议您在 `run()` 开始时生成工作线程，并在 `run()` 返回之前从主线程加入这些线程。这将是一个正确的实现，但它会因频繁的线程创建而产生大量开销。

  >最终代码：
  >
  >`tasksys.cpp`:
  >
  >```cpp
  >TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads) : ITaskSystem(num_threads), num_threads_(num_threads) {
  >    threads_pool_ = std::make_unique<std::thread[]>(num_threads);
  >}
  >
  >void TaskSystemParallelSpawn::threadRun(IRunnable* runnable, int num_total_tasks, std::mutex* mutex, int* curr_task) {
  >    int turn = -1;
  >    while (turn < num_total_tasks) {
  >        mutex->lock();
  >        turn = *curr_task;
  >        *curr_task += 1;
  >        mutex->unlock();
  >        if (turn >= num_total_tasks) {
  >            break;
  >        }
  >        runnable->runTask(turn, num_total_tasks);
  >    }
  >}
  >
  >void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
  >    std::mutex mutex;
  >    int curr_task = 0;
  >    for (int i = 0; i < num_threads_; i++) {
  >        threads_pool_[i] = std::thread(&TaskSystemParallelSpawn::threadRun, this, runnable, num_total_tasks, &mutex, &curr_task);
  >    }
  >    for (int i = 0; i < num_threads_; i++) {
  >        threads_pool_[i].join();
  >    }
  >}
  >```
  >
  >`tasksys.h`:
  >
  >```cpp
  >class TaskSystemParallelSpawn : public ITaskSystem {
  >public:
  >    TaskSystemParallelSpawn(int num_threads);
  >    ~TaskSystemParallelSpawn();
  >    const char* name() override;
  >    void run(IRunnable* runnable, int num_total_tasks) override;
  >    TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
  >                            const std::vector<TaskID>& deps) override;
  >    void sync() override;
  >
  >private:
  >    void threadRun(IRunnable* runnable, int num_total_tasks, std::mutex* mutex, int* curr_task);
  >    std::unique_ptr<std::thread[]> threads_pool_;
  >    int num_threads_;
  >};
  >
  >/*
  > * TasksState: This class is used to record the state of the
  > * running task. Each worker thread and main thread must get
  > * the lock first to read or write the attributes of this class.
  > */
  >class TasksState {
  >public:
  >    TasksState();
  >    ~TasksState();
  >
  >    std::unique_ptr<std::mutex> mutex_;
  >    std::unique_ptr<std::condition_variable> finished_;
  >    std::unique_ptr<std::mutex> finishedMutex_;
  >    IRunnable* runnable_;
  >    int finished_tasks_;
  >    int left_tasks_;
  >    int num_total_tasks_;
  >};
  >```

- How will you assign tasks to your worker threads? Should you consider static or dynamic assignment of tasks to threads?
  如何将任务分配给工作线程？应该考虑静态还是动态地将任务分配给线程？

  > 并行需要考虑动态地将任务分配给线程，每个线程从共享计数器中获取任务。
  >
  > 具体代码：
  >
  > ```cpp
  > void TaskSystemParallelSpawn::threadRun(IRunnable* runnable, int num_total_tasks, std::mutex* mutex, int* curr_task) {
  >     int turn = -1;
  >     while (turn < num_total_tasks) {
  >         mutex->lock();
  >         turn = *curr_task;
  >         *curr_task += 1;
  >         mutex->unlock();
  >         if (turn >= num_total_tasks) {
  >             break;
  >         }
  >         runnable->runTask(turn, num_total_tasks);
  >     }
  > }
  > 
  > void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
  >     std::mutex mutex;
  >     int curr_task = 0;
  >     for (int i = 0; i < num_threads_; i++) {
  >         threads_pool_[i] = std::thread(&TaskSystemParallelSpawn::threadRun, this, runnable, num_total_tasks, &mutex, &curr_task);
  >     }
  >     for (int i = 0; i < num_threads_; i++) {
  >         threads_pool_[i].join();
  >     }
  > }
  > ```
  >
  > **`threadRun` 函数：**
  >
  > - `threadRun` 函数是每个工作线程实际执行的任务。它接收一个指向 `IRunnable` 接口实例的指针，一个表示总任务数的整数，一个 `std::mutex` 互斥量的指针以及一个整数指针 `curr_task`，用于跟踪当前任务的索引。
  > - 在每次迭代中，线程会获取互斥量的锁，然后获取 `curr_task` 的值作为当前任务索引。
  > - 接着，线程会将 `curr_task` 的值增加 1，然后释放互斥量的锁。
  > - 然后，线程检查当前任务索引是否超过了总任务数，如果超过则跳出循环；否则，线程调用 `runTask` 方法执行当前任务。
  > - 这个过程会持续直到所有任务都被执行完成。
  >
  > **`run` 函数：**
  >
  > - `run` 函数负责启动线程池中的工作线程，并分配任务给它们执行。
  > - 首先，函数创建一个互斥量和一个整数 `curr_task`，它们作为共享状态传递给工作线程。
  > - 然后，函数使用 `std::thread` 创建线程池中的每个工作线程，并将 `threadRun` 函数作为线程的入口点。
  > - 每个工作线程都会接收相同的 `IRunnable` 实例指针和任务数量，以及共享的互斥量和任务索引。
  > - 最后，函数等待所有工作线程执行完成，然后返回。

- Are there shared variables (internal state of your task execution system) that you need to protect from simultaneous access from multiple threads? You may wish to review our [C++ synchronization tutorial](https://github.com/stanford-cs149/asst2/blob/master/tutorial/README.md) for more information on the synchronization primitives in the C++ standard library.
  是否有共享变量（任务执行系统的内部状态）需要防止多个线程同时访问？您可能希望查看我们的C++同步教程，以获得有关C++标准库中同步原语的更多信息。

> 有部分共享变量（`curr_task`、`TasksState` 中的 `mutex_`、`finished_`、`finishedMutex_`、`runnable_`、`finished_tasks_`、`left_tasks_`、`num_total_tasks_`）需要防止多个线程同时访问，主要通过使用多个`std::mutex` 和 `std::condition_variable` 来同步对共享变量的访问，防止多个线程同时访问和修改这些共享变量。



#### Step 2: Avoid Frequent Thread Creation Using a Thread Pool 第2步：避免使用线程池频繁创建线程

**In this step please implement the class `TaskSystemParallelThreadPoolSpinning`.
在这一步中，请实现类 `TaskSystemParallelThreadPoolSpinning` 。**

Your implementation in step 1 will incur overhead due to creating threads in each call to `run()`. This overhead is particularly noticeable when tasks are cheap to compute. At this point, we recommend you move to a "thread pool" implementation where your task execution system creates all worker threads up front (e.g., during `TaskSystem` construction, or upon the first call to `run()`).
您在步骤1中的实现将由于在每次调用 `run()` 时创建线程而产生开销。当任务的计算成本很低时，这种开销尤其明显。在这一点上，我们建议您转向“线程池”实现，其中您的任务执行系统预先创建所有工作线程（例如，在 `TaskSystem` 施工期间，或在第一次呼叫 `run()` 时）。

- As a starting implementation we recommend that you design your worker threads to continuously loop, always checking if there is more work to them to perform. (A thread entering a while loop until a condition is true is typically referred to as "spinning".) How might a worker thread determine there is work to do?
  作为一个开始的实现，我们建议您将工作线程设计为连续循环，始终检查是否有更多的工作要执行。（进入while循环直到条件为真的线程通常被称为“自旋”。工作线程如何确定有工作要做？

  >最终代码：
  >
  >`tasksys.cpp`:
  >
  >```cpp
  >TasksState::TasksState()
  >        : mutex_(std::make_unique<std::mutex>()),
  >          finished_(std::make_unique<std::condition_variable>()),
  >          finishedMutex_(std::make_unique<std::mutex>()),
  >          runnable_(nullptr),
  >          finished_tasks_(-1),
  >          left_tasks_(-1),
  >          num_total_tasks_(-1) {}
  >
  >TasksState::~TasksState() = default;
  >
  >const char* TaskSystemParallelThreadPoolSpinning::name() {
  >    return "Parallel + Thread Pool + Spin";
  >}
  >
  >TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads)
  >        : ITaskSystem(num_threads), num_threads_(num_threads), killed(false) {
  >    state_ = std::make_unique<TasksState>();
  >    threads_pool_ = std::make_unique<std::thread[]>(num_threads);
  >    for (int i = 0; i < num_threads; i++) {
  >        threads_pool_[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::spinningThread, this);
  >    }
  >}
  >
  >TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
  >    killed = true;
  >    for (int i = 0; i < num_threads_; i++) {
  >        threads_pool_[i].join();
  >    }
  >}
  >
  >void TaskSystemParallelThreadPoolSpinning::spinningThread() {
  >    int id;
  >    int total;
  >    while (true) {
  >        if (killed) break;
  >        state_->mutex_->lock();
  >        total = state_->num_total_tasks_;
  >        id = total - state_->left_tasks_;
  >        if (id < total) state_->left_tasks_--;
  >        state_->mutex_->unlock();
  >        if (id < total) {
  >            state_->runnable_->runTask(id, total);
  >            state_->mutex_->lock();
  >            state_->finished_tasks_++;
  >            if (state_->finished_tasks_ == total) {
  >                state_->mutex_->unlock();
  >                // this lock is necessary to ensure the main thread has gone to sleep
  >                state_->finishedMutex_->lock();
  >                state_->finishedMutex_->unlock();
  >                state_->finished_->notify_all();
  >            } else {
  >                state_->mutex_->unlock();
  >            }
  >        }
  >    }
  >}
  >```
  >
  >`tasksys.h`:
  >
  >```cpp
  >/*
  > * TaskSystemParallelThreadPoolSpinning: This class is the student's
  > * implementation of a parallel task execution engine that uses a
  > * thread pool. See definition of ITaskSystem in itasksys.h for
  > * documentation of the ITaskSystem interface.
  > */
  >class TaskSystemParallelThreadPoolSpinning : public ITaskSystem {
  >public:
  >    TaskSystemParallelThreadPoolSpinning(int num_threads);
  >    ~TaskSystemParallelThreadPoolSpinning();
  >    const char* name() override;
  >    void run(IRunnable* runnable, int num_total_tasks) override;
  >    TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
  >                            const std::vector<TaskID>& deps) override;
  >    void sync() override;
  >
  >private:
  >    void spinningThread();
  >    std::unique_ptr<TasksState> state_;
  >    std::unique_ptr<std::thread[]> threads_pool_;
  >    bool killed;
  >    int num_threads_;
  >};
  >```

- It is now non-trivial to ensure that `run()` implements the required synchronous behavior. How do you need to change the implementation of `run()` to determine that all tasks in the bulk task launch have completed?

  现在，确保 `run()` 实现所需的同步行为是很重要的。您需要如何更改 `run()` 的实现以确定批量任务启动中的所有任务都已完成？

> ```cpp
> void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
>     std::unique_lock<std::mutex> lk(*(state_->finishedMutex_));
>     state_->mutex_->lock();
>     state_->runnable_ = runnable;
>     state_->num_total_tasks_ = num_total_tasks;
>     state_->left_tasks_ = num_total_tasks;
>     state_->finished_tasks_ = 0;
>     state_->mutex_->unlock();
>     while (state_->finished_tasks_ < num_total_tasks) {
>         state_->finished_->wait(lk);
>     }
> }
> ```
>
> 这段代码确保了在所有任务完成之前，工作线程会一直等待。通过使用条件变量 `state_->finished_` 和互斥量 `state_->finishedMutex_`，线程能够以最有效的方式等待任务完成的通知，并且不会浪费 CPU 时间在空闲的轮询上。



#### Step 3: Put Threads to Sleep When There is Nothing to Do 第三步：无事可做时让线程休眠

**In this step please implement the class `TaskSystemParallelThreadPoolSleeping`.
在这一步中，请实现类 `TaskSystemParallelThreadPoolSleeping` 。**

One of the drawbacks of the step 2 implementation is that threads utilize a CPU core's execution resources as they "spin" waiting for something to do. For example, worker threads might loop waiting for new tasks to arrive. As another example, the main thread might loop waiting for the worker threads to complete all tasks so it can return from a call to `run()`. This can hurt performance since CPU resources are used to run these threads even though the threads are not doing useful work.
第2步实现的缺点之一是，线程在“旋转”等待某件事时会利用CPU核心的执行资源。例如，工作线程可能会循环等待新任务到达。作为另一个例子，主线程可能会循环等待工作线程完成所有任务，这样它就可以从对 `run()` 的调用中返回。这可能会影响性能，因为即使线程没有做有用的工作，CPU资源也会用于运行这些线程。

In this part of the assignment, we want you to improve the efficiency of your task system by putting threads to sleep until the condition they are waiting for is met.
在任务的这一部分，我们希望您通过将线程置于睡眠状态直到它们等待的条件得到满足来提高任务系统的效率。

- Your implementation may choose to use condition variables to implement this behavior. Condition variables are a synchronization primitive that enables threads to sleep (and occupy no CPU processing resources) while they are waiting for a condition to exist. Other threads "signal" waiting threads to wake up to see if the condition they were waiting for has been met. For example, your worker threads could be put to sleep if there is no work to be done (so they don't take CPU resources away from threads trying to do useful work). As another example, your main application thread that calls `run()` might want to sleep while it waits for all the tasks in a bulk task launch to be completed by the worker threads. (Otherwise a spinning main thread would take CPU resources away from the worker threads!) Please see our [C++ synchronization tutorial](https://github.com/stanford-cs149/asst2/blob/master/tutorial/README.md) for more information on condition variables in C++.
  您的实现可以选择使用条件变量来实现此行为。条件变量是一种同步原语，它使线程在等待条件存在时能够休眠（并且不占用CPU处理资源）。其他线程“发信号”等待线程唤醒，以查看它们等待的条件是否已经满足。例如，如果没有工作要做，可以将工作线程置于睡眠状态（这样它们就不会从尝试做有用工作的线程那里占用CPU资源）。再举一个例子，调用 `run()` 的主应用程序线程可能希望在等待工作线程完成批量任务启动中的所有任务时休眠。（否则，一个旋转的主线程会从工作线程那里拿走CPU资源！）有关C++中条件变量的更多信息，请参阅我们的C++同步教程。

- Your implementation in this part of the assignment may have tricky race conditions to think about. You'll need to consider many possible interleavings of thread behavior.
  您在这部分作业中的实作可能需要考虑棘手的竞态条件。您需要考虑线程行为的许多可能的交错。

- You might want to consider writing additional test cases to exercise your system. **The assignment starter code includes the workloads that the grading script will use to grade the performance of your code, but we will also test the correctness of your implementation using a wider set of workloads that we are not providing in the starter code!**
  您可能需要考虑编写额外的测试用例来测试您的系统。作业启动代码包括评分脚本将用于对代码性能进行评分的工作负载，但我们还将使用启动代码中未提供的更广泛的工作负载集来测试您的实现的正确性！

  >`tasksys.cpp`:
  >
  >```cpp
  >TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads)
  >        : ITaskSystem(num_threads), num_threads_(num_threads), killed(false) {
  >    state_ = std::make_unique<TasksState>();
  >    threads_pool_ = std::make_unique<std::thread[]>(num_threads);
  >    hasTasks = std::make_unique<std::condition_variable>();
  >    hasTasksMutex = std::make_unique<std::mutex>();
  >    for (int i = 0; i < num_threads; i++) {
  >        threads_pool_[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::sleepingThread, this);
  >    }
  >}
  >
  >TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
  >    killed = true;
  >    hasTasks->notify_all();
  >    for (int i = 0; i < num_threads_; i++) {
  >        threads_pool_[i].join();
  >    }
  >}
  >
  >void TaskSystemParallelThreadPoolSleeping::sleepingThread() {
  >    int id;
  >    int total;
  >    while (true) {
  >        std::unique_lock<std::mutex> lk(*(hasTasksMutex));
  >        if (killed) break;
  >        while (!killed && state_->left_tasks_ <= 0) {
  >            hasTasks->wait(lk);
  >        }
  >        if (killed) break;
  >        state_->mutex_->lock();
  >        total = state_->num_total_tasks_;
  >        id = total - state_->left_tasks_;
  >        if (id < total) state_->left_tasks_--;
  >        state_->mutex_->unlock();
  >        if (id < total) {
  >            state_->runnable_->runTask(id, total);
  >            state_->mutex_->lock();
  >            state_->finished_tasks_++;
  >            if (state_->finished_tasks_ == total) {
  >                state_->mutex_->unlock();
  >                // this lock is necessary to ensure the main thread has gone to sleep
  >                state_->finishedMutex_->lock();
  >                state_->finishedMutex_->unlock();
  >                state_->finished_->notify_all();
  >            } else {
  >                state_->mutex_->unlock();
  >            }
  >        }
  >    }
  >}
  >
  >void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
  >    std::unique_lock<std::mutex> lk(*(state_->finishedMutex_));
  >    state_->mutex_->lock();
  >    state_->runnable_ = runnable;
  >    state_->num_total_tasks_ = num_total_tasks;
  >    state_->left_tasks_ = num_total_tasks;
  >    state_->finished_tasks_ = 0;
  >    state_->mutex_->unlock();
  >    hasTasks->notify_all();
  >    while (state_->finished_tasks_ < num_total_tasks) {
  >        state_->finished_->wait(lk);
  >    }
  >}
  >```
  >
  >`tasksys.h`:
  >
  >```cpp
  >/*
  > * TaskSystemParallelThreadPoolSleeping: This class is the student's
  > * optimized implementation of a parallel task execution engine that uses
  > * a thread pool. See definition of ITaskSystem in
  > * itasksys.h for documentation of the ITaskSystem interface.
  > */
  >class TaskSystemParallelThreadPoolSleeping : public ITaskSystem {
  >public:
  >    TaskSystemParallelThreadPoolSleeping(int num_threads);
  >    ~TaskSystemParallelThreadPoolSleeping();
  >    const char* name() override;
  >    void run(IRunnable* runnable, int num_total_tasks) override;
  >    TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
  >                            const std::vector<TaskID>& deps) override;
  >    void sync() override;
  >
  >private:
  >    void sleepingThread();
  >    std::unique_ptr<TasksState> state_;
  >    std::unique_ptr<std::thread[]> threads_pool_;
  >    std::unique_ptr<std::condition_variable> hasTasks;
  >    std::unique_ptr<std::mutex> hasTasksMutex;
  >    bool killed;
  >    int num_threads_;
  >};
  >```
