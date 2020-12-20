import time
import asyncio

now = lambda : time.time()

# async定义协程
async def do_some_work(x):
    print("waiting:",x)
    # await挂起阻塞， 相当于yield， 通常是耗时操作
    await asyncio.sleep(x)
    print(f"执行{x}")
    return "Done after {}s".format(x)

# 回调函数，和yield产出类似功能
def callback(future):
    print("callback:",future.result())

print("定义")
start = now()
tasks = []
for i in range(1, 4):
    # 定义多个协程，同时预激活
    coroutine = do_some_work(i)
    task = asyncio.ensure_future(coroutine)
    task.add_done_callback(callback)
    tasks.append(task)

# 定一个循环事件列表，把任务协程放在里面，
loop = asyncio.get_event_loop()
try:
    print("异步执行")
    # 异步执行协程，直到所有操作都完成， 也可以通过asyncio.gather来收集多个任务
    loop.run_until_complete(asyncio.wait(tasks))
    print("返回output")
    for task in tasks:
        print("Task ret:",task.result())
except KeyboardInterrupt as e: # 协程任务的状态控制
    print("User Ctrl+c...")
    print(asyncio.Task.all_tasks())
    print("Output task status with cancel...")
    for task in asyncio.Task.all_tasks():
        print(task.cancel())
    print("loop stop...")
    loop.stop()
    print("loop forever...")
    loop.run_forever()
finally:
    print("循环close")
    loop.close()

print("Time:", now()-start)