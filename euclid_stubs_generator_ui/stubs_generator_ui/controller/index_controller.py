import os
import zipfile
from io import BytesIO

import time

from euclid_stubs_generator.stub_info import StubInfo, NodeType
from euclidwf.framework.graph_builder import build_graph
from euclidwf.framework.graph_tasks import ExecTask, ParallelSplitTask, NestedGraphTask
from flask import json
from pydron.dataflow.graph import Graph, _Connection, START_TICK

from utils.context_manager import ChangeDir


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
            globals().update({k: var})
            if hasattr(var, '__call__') and hasattr(var, 'ispipeline') and not pipeline_found:
                pipeline_found = True
                pipeline2 = var

        assert getattr(pipeline2, 'ispipeline')
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
                        print("Key: " + key + " Input: " + item.source.tick.__str__())
                        if item.source.tick == START_TICK and key != 'context':
                            files.update({key: 32})
        return files

    def filter_executables_with_graph(self, pydron_graph, loaded_executables):
        """
        :type pydron_graph: Graph
        :type executables: [Executable]
        :type filtered_executables: Set
        :return:
        """
        filtered_executables = set()
        for key, value in pydron_graph._ticks.items():
            task = value.task

            if isinstance(task, ExecTask):
                print("Normal: %s" % task.command)

                matching = [s for s in loaded_executables if task.command in s]
                if len(matching) == 1 and loaded_executables[matching[0]].outputs[0].content_type == 'listfile':
                    filtered_executables.add(StubInfo(task.command, NodeType.normal, True))
                else:
                    filtered_executables.add(StubInfo(task.command, NodeType.normal))
                continue
            if isinstance(task, ParallelSplitTask):
                print("Split: %s" % task.name)
                filtered_executables.add(StubInfo(task.name, NodeType.split))
                subTasks = self.filter_executables_with_graph(task.body_graph, loaded_executables)
                filtered_executables = filtered_executables.union(subTasks)
                continue
            if isinstance(task, NestedGraphTask):
                print("Nested: %s" % task.name)
                filtered_executables.add(StubInfo(task.name, NodeType.nested))
                subTasks = self.filter_executables_with_graph(task.body_graph, loaded_executables)
                filtered_executables = filtered_executables.union(subTasks)
                continue

            print("Unkown: %s" % type(task))

        return filtered_executables

    def setDefaultComputingResources(self, executables, filtered_execs):
        """
        :type filtered_execs: set([StubInfo])
        :type executables: [Executable]
        :return:
        """
        for filtered_exec in filtered_execs:
            for executable in executables.items():
                if filtered_exec.command == executable[0]:
                    filtered_exec.cores = executable[1].resources.cores
                    filtered_exec.ram = int(executable[1].resources.ram)
                    filtered_exec.walltime = executable[1].resources.walltime
                    for file in executable[1].outputs:
                        filtered_exec.outputfiles.append((file.name, 50))
                    for file in executable[1].inputs:
                        filtered_exec.inputfiles.append((file.name))
                    break

    def parseWallTime(self, walltime):
        """
        :type walltime : str
        :return:
        """
        splits = walltime.split(':')
        seconds = 0
        while switch(len(splits)):
            if case(1):
                seconds = int(splits[0]) * 60 * 60
                break
            if case(2):
                seconds = int(splits[0]) * 60 * 60 + int(splits[1]) * 60
                break
            if case(3):
                seconds = int(splits[0]) * 60 * 60 + int(splits[1]) * 60 + int(splits[2])
                break
        return seconds

    def writeComputingResources(self, filterd_executables, outputFolder):
        computingResources = {}
        for stubinfo in filterd_executables:
            if isinstance(stubinfo, StubInfo):
                computingResources.update({stubinfo.command: {}})
                computingResources[stubinfo.command].update({"cores": stubinfo.cores})
                computingResources[stubinfo.command].update({"ram": stubinfo.ram})
                computingResources[stubinfo.command].update({"walltime": stubinfo.walltime})

        with open(os.path.join(outputFolder, 'resources.txt'), 'w') as outfile:
            json.dump(computingResources, outfile)

    def writePortMapping(self, input_files, outputFolder):
        output_text = "workdir=test_run\nlogdir=logdir\n"
        for k in input_files.keys():
            output_text += "%s=%s.dat\n" % (k, k)

        with open(os.path.join(outputFolder, 'portmapping.txt'), 'w') as outfile:
            outfile.write(output_text)

    def createZip(self, outputFolder):
        memory_file = BytesIO()

        base_dir = '/'.join(outputFolder.split('/')[0:-2])
        target_dir = '/'.join(outputFolder.split('/')[-2:])

        with ChangeDir(base_dir):
            with zipfile.ZipFile(memory_file, 'w') as zf:
                for dirname, subdirs, files in os.walk(target_dir):
                    zf.write(dirname)

                    for filename in files:
                        data = zipfile.ZipInfo(os.path.join(dirname, filename))
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
