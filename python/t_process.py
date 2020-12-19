import os
import time
import sys
from concurrent import futures

def to_do(info):
    st=time.time()
    for i in range(10000000):
        pass
    print(f"{info[0]} and {info[1]} took {time.time()-st} seconds.")
    return info[0]

start_time = time.time()
MAX_WORKERS = 15
param_list = []
for i in range(15):
    param_list.append(('text%s' % i, 'info%s' % i))

workers = min(MAX_WORKERS, len(param_list))
# with 默认会等所有任务都完成才返回，所以这里会阻塞
with futures.ProcessPoolExecutor(workers) as executor:
    results = executor.map(to_do, sorted(param_list))
    
# 打印所有
for index, result in enumerate(results, start=1):
    print(f"{index} result is: {result}")

print(time.time()-start_time)
# 耗时0.3704512119293213s， 而线程版本需要14.935384511947632s