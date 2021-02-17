'''
- program file: myshell.py
- repository: test4pycharm branch shell-like
- purpose: demo how to wrap shell into python
  * 中文注释：来自网页https://www.233tw.com/python/27893
  * 更新：
'''
import subprocess

class PipePy:
    def __init__(self, *args, _pipe_input=None, **kwargs):
        # if _pipe_input is None:
        #     print(f"No pipe input in {args[0]}.__init__.")
        # else:
        #     print(f"In {args[0]}.__init__, pipe input is {_pipe_input}")
        self._args = args
        self._pipe_input = _pipe_input
        self._kwargs = kwargs
        self._result = None

    def __call__(self, *args, _pipe_input=None, **kwargs):
        '''
        | 惰性对象：不用每次我们要自定义命令时都去调用PipePy。
        '''
        # if _pipe_input is None:
        #     print(f"No pipe input in {args[0]}.__call__.")
        # else:
        #     print(f"In {args[0]}.__call__, pipe input is {_pipe_input}")
        args = self._args + args
        kwargs = {**self._kwargs, **kwargs}
        return self.__class__(*args, _pipe_input=None, **kwargs)

    def _evaluate(self):
        if self._result is not None:
            return
        self._result = subprocess.run(self._convert_args(),
                                      input=self._pipe_input,
                                      capture_output=True,
                                      text=True)

    def _convert_args(self):
        args = [str(arg) for arg in self._args]
        for key, value in self._kwargs.items():
            key = key.replace('_', '-')
            args.append(f"--{key}={value}")
        return args

    def __or__(left, right):
        return right(_pipe_input=left.stdout)
    # def __or__(self, other):
    #     return other(_pipe_input=self.stdout)

    @property
    def returncode(self):
        self._evaluate()
        return self._result.returncode

    @property
    def stdout(self):
        self._evaluate()
        return self._result.stdout

    def __str__(self):
        return self.stdout

    @property
    def stderr(self):
        self._evaluate()
        return self._result.stderr

if __name__ == '__main__':
    # mylog=os.path.basename(__file__).replace('.py','.log')
    # mylog=f"{config.WORKDIR}/{mylog}"
    # if os.path.exists(mylog):
        # os.remove(mylog)
    # logzero.logfile(mylog)
    ls = PipePy('ls')
    grep=PipePy('grep')
    print("DEBUG ls -l")
    print(ls('-l'))
    #print(ls('-l', sort='size'))
    print("DEBUG ls -l grep txt")
    print(ls('-l') | grep('txt'))
    #print(grep('txt')(ls))