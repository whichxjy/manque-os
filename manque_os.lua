---------------------------------------------------------------------
-------------------------=====  Queue  =====-------------------------
---------------------------------------------------------------------

-- Create Queue
function create_queue()
    local queue = {
        items = {}
    }

    function queue:push(item)
        table.insert(self.items, item)
    end

    function queue:pop()
        if self:is_empty() then
            return nil
        else
            return table.remove(self.items, 1)
        end
    end

    function queue:is_empty()
        if #self.items == 0 then
            return true
        else
            return false
        end
    end

    return queue
end

---------------------------------------------------------------------
-------------------------=====  Tasks  =====-------------------------
---------------------------------------------------------------------

-- class table
Task = {
    taskid = 0
}

-- Get a new task
function Task:new(target, o)
    if not target then
        error("no target")
    end

    o = o or {}

    self.taskid = self.taskid + 1
    self.__index = self
    setmetatable(o, self)

    -- Task ID
    o.tid = self.taskid
    -- Target coroutine
    o.target = target
    -- Value to send
    o.sendval = nil
    -- Call stack
    o.stack = {}

    return o
end

-- Run a task until it hits the next yield statement
function Task:run()
    local status, result = coroutine.resume(self.target, self.sendval)
    if coroutine.status(self.target) == "suspended" then
        return result
    elseif coroutine.status(self.target) == "dead" then
        error("coroutine is dead")
    end
end

---------------------------------------------------------------------
----------------------=====  System Call  =====----------------------
---------------------------------------------------------------------

-- class table
SystemCall = {}

function SystemCall:new(o)
    o = o or {}

    self.__index = self
    setmetatable(o, self)

    return o
end

function is_system_call(o)
    return (getmetatable(o) == SystemCall
            or (getmetatable(getmetatable(o)) == SystemCall))
end

-- [System Call: create a new task]
-- class table
NewTask = SystemCall:new()

function NewTask:new(target, o)
    if not target then
        error("no target")
    end

    o = o or {}

    self.__index = self
    setmetatable(o, self)

    -- Target coroutine (new task)
    o.target = target

    return o
end

function NewTask:handle()
    local tid = self.sched:create_task(self.target)
    self.task.sendval = tid
    self.sched:schedule(self.task)
end

-- [System Call: get task's ID number]
-- class table
GetTid = SystemCall:new()

function GetTid:new(o)
    o = o or {}

    self.__index = self
    setmetatable(o, self)

    return o
end

function GetTid:handle()
    self.task.sendval = self.task.tid
    self.sched:schedule(self.task)
end

-- [System Call: kill task]
-- class table
KillTask = SystemCall:new()

function KillTask:new(kill_tid, o)
    o = o or {}

    self.__index = self
    setmetatable(o, self)

    -- ID of the task to kill
    o.kill_tid = kill_tid

    return o
end

function KillTask:handle()
    -- Check if the task exists
    local task_to_kill = self.sched.taskmap[self.kill_tid]
    if task_to_kill then
        -- There is no explicit operation for terminating
        -- a coroutine, so I have to kill the coroutine by
        -- setting a debug hook.
        debug.sethook(task_to_kill.target, function()
            error("almost dead")
        end, "l")
        self.task.sendval = true
    else
        self.task.sendval = false
    end

    self.sched:schedule(self.task)
end

-- [System Call: wait task]
-- class table
WaitTask = SystemCall:new()

function WaitTask:new(wait_tid, o)
    o = o or {}

    self.__index = self
    setmetatable(o, self)

    -- ID of the task to wait
    o.wait_tid = wait_tid

    return o
end

function WaitTask:handle()
    local result = self.sched:wait_for_exit(self.task, self.wait_tid)
    self.task.sendval = result
    -- If waiting for a non-existent task,
    -- return immediately without waiting.
    if not result then
        self.sched:schedule(self.task)
    end
end

---------------------------------------------------------------------
-----------------------=====  Scheduler  =====-----------------------
---------------------------------------------------------------------

-- class table
Scheduler = {}

function Scheduler:new(o)
    o = o or {}

    self.__index = self
    setmetatable(o, self)

    -- Ready Queue
    o.ready = create_queue()
    -- Task Map
    o.taskmap = {}
    -- Tasks waiting for other tasks to exit
    o.exit_waiting = {}

    return o
end

function Scheduler:create_task(target)
    local newTask = Task:new(target)
    self.taskmap[newTask.tid] = newTask
    self:schedule(newTask)
    return newTask.tid
end

function Scheduler:schedule(task)
    self.ready:push(task)
end

function Scheduler:exit(task)
    print(string.format("Task %d terminated", task.tid))
    self.taskmap[task.tid] = nil
    -- Notify other tasks waiting for exit.
    local waiting_queue = self.exit_waiting[task.tid]
    if waiting_queue then
        -- Pop all waiting tasks off out of the waiting area
        -- and reschedule them.
        while not waiting_queue:is_empty() do
            local waiting_task = waiting_queue:pop()
            self:schedule(waiting_task)
        end
    end
end

-- A utility method that makes a task wait for another task.
-- It puts the task in the waiting area.
function Scheduler:wait_for_exit(task, wait_tid)
    if self.taskmap[wait_tid] then
        -- Get waiting queue
        self.exit_waiting[wait_tid] = self.exit_waiting[wait_tid] or create_queue()
        self.exit_waiting[wait_tid]:push(task)
        return true
    else
        return false
    end
end

function Scheduler:mainloop()
    -- If task map is not empty, pull task from ready queue
    while next(self.taskmap) ~= nil do
        local task = self.ready:pop()
        local status, result = pcall(Task.run, task)
        if status == false then
            -- Task is finished
            self:exit(task)
        elseif is_system_call(result) then
            -- Task is a system call
            print("Get a system call")
            result.task = task
            result.sched = self
            result:handle()
        else
            -- Normal task
            self:schedule(task)
        end
    end
end

---------------------------------------------------------------------
------------------------=====  Example  =====------------------------
---------------------------------------------------------------------

local foo = coroutine.create(
    function ()
        local tid = coroutine.yield(GetTid:new())
        for i = 1, 5 do
            print("I'm foo, tid = ", tid)
            coroutine.yield()
        end
    end
)

local bar = coroutine.create(
    function ()
        local tid = coroutine.yield(GetTid:new())
        for i = 1, 50 do
            print("I'm bar, tid = ", tid)
            coroutine.yield()
        end
    end
)

local main = coroutine.create(
    function ()
        print("Create foo task")
        local foo_tid = coroutine.yield(NewTask:new(foo))
        print("Waiting for foo")
        coroutine.yield(WaitTask:new(foo_tid))
        print("foo done")
        print("Create bar task")
        local bar_tid = coroutine.yield(NewTask:new(bar))
        for i = 1, 5 do
            coroutine.yield()
        end
        print("Kill the bar")
        coroutine.yield(KillTask:new(bar_tid))
        print("main done")
    end
)

local sched = Scheduler:new()
sched:create_task(main)
sched:mainloop()
