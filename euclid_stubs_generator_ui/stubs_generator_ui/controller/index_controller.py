import os
import zipfile
from io import BytesIO

import time
from euclidwf.framework.graph_builder import build_graph
from euclidwf.framework.taskdefs import Executable
from flask import json
from pydron.dataflow.graph import Graph, _Connection, START_TICK


class IndexController(object):
    def build_graph_from_file(self, file_path):
        pipeline_globals = {}
        pipeline_locals = {}
        execfile(file_path)

        pipeline2 = None
        pipeline_found = False

        keys = locals().keys()
        for k in keys:
            var = locals().get(k)
            globals().update({k:var})
            if hasattr(var, '__call__') and hasattr(var,'ispipeline') and not pipeline_found:
                pipeline_found = True
                pipeline2 = var

        assert getattr(pipeline2,'ispipeline')
        assert pipeline2 is not None

        pydron_graph = build_graph(pipeline2)
        return pydron_graph

    def get_all_start_inputs_from_graph(self, pydron_graph):
        """
        :type pydron_graph: Graph
        :return:
        """
        files = {}

        for tick in pydron_graph._ticks.values():
            if len(tick.in_connections.items()) > 0:
                abc = tick.in_connections.items()[1]
                for key, item in tick.in_connections.items():
                    if isinstance(item, _Connection):
                        print("Key: "+key+" Input: "+item.source.tick.__str__())
                        if item.source.tick == START_TICK and key != 'context':
                            files.update({key: 32})
        return files

    def filter_executables_with_graph(self, pydron_graph):
        pass

    def parseWallTime(self,walltime):
        """
        :type walltime : str
        :return:
        """
        splits = walltime.split(':')
        seconds = 0
        while switch(len(splits)):
            if case(1):
                seconds = int(splits[0]) * 60*60
                break
            if case (2):
                seconds = int(splits[0]) * 60 * 60 + int(splits[1]) * 60
                break
            if case (3):
                seconds = int(splits[0]) * 60 * 60 + int(splits[1]) * 60 + int(splits[2])
                break
        return seconds

    def writeComputingResources(self, filterd_executables, outputFolder):
        computingResources = {}
        for key, value in filterd_executables.items():
            if isinstance(value, Executable):
                computingResources.update({key : {}})
                computingResources[key].update( {"cores" : value.resources.cores} )
                computingResources[key].update( {"ram" : value.resources.ram} )
                computingResources[key].update( {"walltime" : value.resources.walltime} )

        with open(os.path.join(outputFolder,'resources.txt'), 'w') as outfile:
            json.dump(computingResources, outfile)

    def createZip(self, outputFolder):
        memory_file = BytesIO()
        with zipfile.ZipFile(memory_file, 'w') as zf:
            for dirname, subdirs, files in os.walk(outputFolder):
                for filename in files:
                    data = zipfile.ZipInfo(filename)
                    data.date_time = time.localtime(time.time())[:6]
                    data.compress_type = zipfile.ZIP_DEFLATED
                    bytes = open(os.path.join(dirname, filename)).read()
                    zf.writestr(data, bytes)
        memory_file.seek(0)
        return memory_file

class switch(object):
    value = None
    def __new__(class_, value):
        class_.value = value
        return True

def case(*args):
    return any((arg == switch.value for arg in args))
