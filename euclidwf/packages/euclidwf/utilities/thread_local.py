import threading

threadLocal = threading.currentThread()
threadLocal.tasks={}
threadLocal.current=None

def get_current():
    return threadLocal.current

def set_current(method):
    threadLocal.current=method

def init_list(method):
    set_current(method)
    threadLocal.tasks[method]=[]

def get_list(method):
    return threadLocal.tasks[method]

def add_item(task):
    threadLocal.tasks[threadLocal.current].append(task)


