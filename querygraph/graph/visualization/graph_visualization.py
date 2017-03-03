from graphviz import Digraph

from querygraph.graph import QueryGraph
from querygraph.exceptions import QueryGraphException


# =============================================
# Exceptions
# ---------------------------------------------


class GraphVizualizatioException(QueryGraphException):
    pass


# =============================================
# Graph Visualization Class
# ---------------------------------------------

class GraphVisualization(object):

    def __init__(self, query_graph, save_path):
        if not isinstance(query_graph, QueryGraph):
            raise GraphVizualizatioException
        self.query_graph = query_graph
        self.save_path = save_path

    def render(self):
        dot = Digraph(comment='Query Graph Visualization')
        for node in self.query_graph:
            dot.node(node.name)
        for node in self.query_graph:
            for child_node in node.children:
                dot.edge(node.name, child_node.name)
                dot.edge(child_node.name, node.name, label=str(child_node.join_context), fontsize='8', style='dashed')
        dot.render(self.save_path)



dot = Digraph(comment='Query Graph')
dot.node('mo_seasons', 'mo_seasons')
dot.node('daily_ts', 'daily_ts')
dot.node('hourly_ts', 'hourly_ts')

dot.edge('mo_seasons', 'daily_ts', fontsize='8', minlen='3')
dot.edge('daily_ts', 'mo_seasons', label='RIGHT \n JOIN', fontsize='8', style='dashed')

dot.edge('mo_seasons', 'hourly_ts', fontsize='8', minlen='3')
dot.edge('hourly_ts', 'mo_seasons', label='LEFT \n JOIN', fontsize='8', style='dashed')
dot.render('query_graph', view=True)