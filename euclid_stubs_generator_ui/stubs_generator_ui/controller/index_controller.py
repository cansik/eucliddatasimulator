from euclidwf.framework.graph_builder import build_graph
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
                            files.update({key: 1024})
        return files