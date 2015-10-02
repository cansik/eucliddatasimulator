'''
Created on Jun 25, 2015

@author: martin.melchior
'''
from euclidwf.framework.workflow_dsl import parallel, pipeline, invoke_task
from euclidwf.framework.taskdefs import TaskProperties

@parallel(iterable='x')
def test_task_parallel(x,y):
    u,v=test_exec(a=x, b=y)
    w,z=test_exec(name="test_exec2", a=u, b=v)
    return u,w,z

@pipeline(outputs=('u'))        
def testpipe_parallel(x,y):
    return test_task_parallel(x=x,y=y)


def test_exec(**kwargs):
    inputnames=("a","b")
    outputnames=("c","d")
    taskprops=TaskProperties("test_exec", "test")
    return invoke_task(taskprops, inputnames, outputnames, **kwargs)


