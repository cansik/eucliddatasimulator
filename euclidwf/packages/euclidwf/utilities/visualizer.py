'''
Created on May 24, 2015

@author: martin.melchior (at) fhnw.ch
'''

import tempfile

from PIL import Image, ImageTk
import pygraphviz as pgv
from Tkconstants import HORIZONTAL, BOTTOM, RIGHT, VERTICAL, LEFT, BOTH
import Tkinter
from Tkinter import Tk, Frame, Scrollbar, Canvas, mainloop
from pydron.dataflow.graph import START_TICK, FINAL_TICK
from euclidwf.framework.drm_access import JOB_COMPLETED, JOB_ERROR, JOB_EXECUTING, JOB_QUEUED, JOB_PENDING
from euclidwf.framework.context import CONTEXT

def visualize_graph(graph):
    pygraph=create_pygraph(graph)
    _,tmpfilepath=tempfile.mkstemp(suffix='.png')
    #print tmpfilepath
    pygraph.draw(tmpfilepath,format='png')    
    display(tmpfilepath)
    

def create_pygraph(graph):
    
    pygraph = pgv.AGraph(directed=True, strict=True, compound=True, fontsize=10, fontname="Verdana")
    pygraph.node_attr['style']='filled'
    pygraph.node_attr['fontsize']=10
    pygraph.node_attr['fontname']='Verdana'
    
    _add_start_node(pygraph, START_TICK)
    ticks=sorted(graph.get_all_ticks())
    for tick in ticks:
        task=graph.get_task(tick)
        node_props={k:v for k,v in graph.get_task_properties(tick).iteritems()}
        node_props["tooltip"]="NAME" # test for whether the tooltip work- they do not at the moment.
        _add_node(pygraph, tick, task, node_props)
    _add_end_node(pygraph, FINAL_TICK)

    for edge in _get_edges(graph):
        src_key=edge[0]
        dest_key=edge[1]
        descr=edge[2]
        linewidth=3 if edge[3] else 1
        pygraph.add_edge(src_key, dest_key, style="setlinewidth(%s)"%str(linewidth), edgetooltip=descr)

    pygraph.layout('dot')
    return pygraph

def _add_start_node(pygraph, key):
    pygraph.add_node(key, fillcolor="white", fontcolor="black", width=0.75, height=0.5, style="filled", label="START")#, fixedsize=True)
    
def _add_end_node(pygraph, key):
    pygraph.add_node(key, fillcolor="white", fontcolor="black", width=0.75, height=0.5, style="filled", label="END")#, fixedsize=True)

def _add_node(pygraph, key, task, taskprops):
    name=taskprops['name'] +'\n' + str(key)
    path=taskprops['path']
    if 'summary' in taskprops.keys():
        summary=taskprops['summary']
        fillcolor,fontcolor=_get_node_color(summary.status)
    else:
        fillcolor,fontcolor="white","black"
    pygraph.add_node(key, fillcolor=fillcolor, fontcolor=fontcolor, width=0.75, height=0.5, style="filled", label=name, tooltip=path)#, fixedsize=True)


def _get_edges(graph):
    edges=[]
    ticks=[START_TICK]
    ticks.extend(graph.get_all_ticks())
    ticks.append(FINAL_TICK)
    for tick in ticks:
        in_connections={}
        for c in graph.get_in_connections(tick):
            if c[0].port != CONTEXT:
                if not c[0].tick in in_connections.keys():
                    in_connections[c[0].tick]=[]
                in_connections[c[0].tick].append((c[0].port,c[1].port))
        for src_tick,conns in in_connections.iteritems():
            if len(conns)==1:
                conn=conns[0]
                descr="%s->%s"%(conn[0],conn[1])
                edges.append((src_tick,tick,descr,True))
            else:
                descr=""
                for conn in conns:
                    descr=descr+"%s->%s\n"%(conn[0],conn[1])
                edges.append((src_tick,tick,descr,False))
    return edges
        

def _get_node_color(status):
    if status == JOB_COMPLETED:
        fillcolor="green"
        fontcolor="black"
    elif status == JOB_ERROR:
        fillcolor="red"
        fontcolor="black"
    elif status == JOB_EXECUTING:
        fillcolor="yellow"
        fontcolor="black"
    elif status == JOB_PENDING or status == JOB_QUEUED:
        fillcolor="white"
        fontcolor="black"
    else:
        fillcolor="lightgrey"
        fontcolor="black"        
    return fillcolor,fontcolor

def button_click_exit_mainloop (event):
    event.widget.quit() # this will cause mainloop to unblock.



def display(image_file):
    
    root = Tk()
    root.title("Dataflow Graph")
    screen_width=root.winfo_screenwidth()*1.0
    screen_height=root.winfo_screenheight()*0.875
    
    image1 = Image.open(image_file)
    width,height=image1.size
    if width>screen_width or height>screen_height:
        factor=max(width/screen_width,height/screen_height)
        image1=image1.resize((int(width/factor),int(height/factor)), Image.ANTIALIAS)

    
    frame = Frame(root, width=image1.size[0],height=image1.size[1])
    frame.grid(row=0,column=0)
    canvas=Canvas(frame,bg='#FFFFFF',width=image1.size[0],height=image1.size[1],scrollregion=(0,0,image1.size[0],image1.size[1]))
    img = ImageTk.PhotoImage(image1)
    canvas.create_image(0,0,image=img, anchor="nw")

    hbar=Scrollbar(frame,orient=HORIZONTAL)
    hbar.pack(side=BOTTOM,fill=Tkinter.X)
    hbar.config(command=canvas.xview)
    vbar=Scrollbar(frame,orient=VERTICAL)
    vbar.pack(side=RIGHT,fill=Tkinter.Y)
    vbar.config(command=canvas.yview)
    canvas.config(width=image1.size[0],height=image1.size[1])
    canvas.config(xscrollcommand=hbar.set, yscrollcommand=vbar.set)
    canvas.pack(side=LEFT,expand=True,fill=BOTH)

    frame.pack()
    # added so that the windows pops up (and is not minimized) 
    # --> see http://stackoverflow.com/questions/9083687/python-tkinter-gui-always-loads-minimized
    root.attributes('-topmost', 1)
    root.update()
    root.attributes('-topmost', 0)    
    mainloop()
 

if __name__ == '__main__':
    A=pgv.AGraph(directed=True, compound=True)
    # add some edges
    A.add_node(1,name="A")
    A.add_node(2,name="B")
    A.add_node(3,name="C")
    A.add_node(4,name="D")
    A.add_node(5,name="E")
    A.add_node(6,name="F")
    B=A.add_subgraph([4,5,6],name='clusters1')    
    B.graph_attr['rank']='same'
    A.add_edge(1,2)
    A.add_edge(2,3)
    A.add_edge(1,3)
    A.add_edge(3,4,lhead='clusters1')
    A.add_edge(4,6)
    # make a subgraph with rank='same'
    _,tmpfilepath=tempfile.mkstemp(suffix='.png')
    A.draw(tmpfilepath,format='png',prog='dot')    
    display(tmpfilepath)
    