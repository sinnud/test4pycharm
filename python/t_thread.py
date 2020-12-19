import os
import time
import sys
from concurrent import futures

def to_do(info):  
    print(info)
    for i in range(100000000):
        pass
    return info[0]

start_time = time.time()

MAX_WORKERS = 10
param_list = []
for i in range(5):
    param_list.append(('text%s' % i, 'info%s' % i))

workers = min(MAX_WORKERS, len(param_list))
# with 默认会等所有任务都完成才返回，所以这里会阻塞
with futures.ThreadPoolExecutor(workers) as executor:
    results = executor.map(to_do, sorted(param_list))

# 打印所有
for index, result in enumerate(results, start=1):
    print(f"{index} result is: {result}")
print(time.time()-start_time)

start_time = time.time()
# 非阻塞的方式，适合不需要返回结果的情况
workers = min(MAX_WORKERS, len(param_list))
executor = futures.ThreadPoolExecutor(workers)
results = []
for idx, param in enumerate(param_list):
    result = executor.submit(to_do, param)
    results.append(result)
    print('result %s' % idx)

# 手动等待所有任务完成
executor.shutdown()
print('='*10)
for result in results:
    print(result.result())
print(time.time()-start_time)
