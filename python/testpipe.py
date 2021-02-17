'''
- profile file: testpipe.py
- purpse: test overwrite __or__ method for pipe like operation
'''

class StrRenew(object):
    count=0
    def __init__(self, _pipe_input=None):
        StrRenew.count += 1
        self._pipe_input = _pipe_input
        self._result = None

    def __str__(self):
        return f"{str(self.__class__)}: {str(self.__dict__)}"


if __name__ == '__main__':
    a=StrRenew()
    print(f"{a} with count: {StrRenew.count} and {a.count}")
    b=StrRenew(_pipe_input="Something from pipe")
    print(f"{b} with count: {StrRenew.count} and {a.count} and {b.count}")