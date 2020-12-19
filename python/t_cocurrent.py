from collections import namedtuple
Result = namedtuple('Result', 'count average')

# 协程函数
def averager():
    print("协程执行")
    total = 0.0
    count = 0
    average = None
    while True:
        term = yield None  # 暂停，等待主程序传入数据唤醒
        if term is None:
            break  # 决定是否退出
        total += term
        count += 1
        average = total/count # 累计状态，包括上一次的状态
    return Result(count, average)

# 协程的触发
print("协程的触发")
coro_avg = averager()
# 预激活协程
print("预激活协程")
next(coro_avg)

# 调用者给协程提供数据
print("调用者给协程提供数据")
coro_avg.send(10)
coro_avg.send(30)
coro_avg.send(1000)
coro_avg.send(6.5)
try:
    coro_avg.send(None)
except StopIteration as exc: # 执行完成，会抛出StopIteration异常，返回值包含在异常的属性value里
    result = exc.value

print("执行完成")
print(result)