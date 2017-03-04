from collections import OrderedDict
import copy

import pandas as pd

from querygraph.exceptions import QueryGraphException
from querygraph.query.template import QueryTemplate
from querygraph.query.node.join_context import JoinContext
from querygraph.query.node.execution_thread import ExecutionThread
from querygraph.db.connectors import DatabaseConnector
from querygraph.db.test_data import connectors
from querygraph.manipulation.expression.evaluator import Evaluator
from querygraph.manipulation.set import ManipulationSet


# =============================================
# Exceptions
# ---------------------------------------------

class JoinException(QueryGraphException):
    pass


class CycleException(QueryGraphException):
    pass


class AddColumnException(QueryGraphException):
    pass


# =============================================
# Query Node Class
# ---------------------------------------------

class QueryNode(object):

    def __init__(self, name, query, db_connector):
        self.name = name
        self.query = query
        if not isinstance(db_connector, DatabaseConnector):
            raise QueryGraphException("The db_connector for node '%s' must be a "
                                      "DatabaseConnector instance." % self.name)
        self.db_connector = db_connector
        self.children = list()
        self.parent = None
        self.join_context = JoinContext(child_node_name=self.name)
        self.df = None
        self.already_executed = False
        self._new_columns = OrderedDict()
        self.manipulation_set = ManipulationSet()

    def is_independent(self):
        if self.is_root_node:
            return True
        else:
            query_template = QueryTemplate(query=self.query, db_connector=self.db_connector)
            return not query_template.has_dependent_parameters()

    def root_node(self):
        if self.parent is None:
            return self
        else:
            return self.parent.root_node()

    @property
    def num_edges(self):
        return len(self.children)

    @property
    def is_root_node(self):
        return self.parent is None

    def __getitem__(self, item):
        return OnColumn(query_node=self, col_name=item)

    def __contains__(self, item):
        """ Check if given item is a child of this QueryNode. """
        return item in list(self)

    def __iter__(self):
        """
        Define iterator behaviour. Returns all nodes in the query graph in a topological order.

        """
        yield self
        for child in self.children:
            yield child

    def creates_cycle(self, child_node):
        """
        Check if adding the given node will result in a cycle.

        """
        return self in child_node

    def add_column(self, **kwargs):
        """
        Add/modify a column to this node's dataframe after its query has been executed.
        Only a single key-value argument should be given, where the key is the
        name of the column to be added/modified, and the value is the expression
        to evaluate that defines the column.

        """
        if len(kwargs.keys()) > 1:
            raise AddColumnException("Only one column should be added at a time due to Python"
                                     "kwargs being unordered.")
        for k, v in kwargs.items():
            self._new_columns[k] = v

    def _create_added_columns(self):
        """ Create any/all new columns that were defined. """
        evaluator = Evaluator()
        for k, v in self._new_columns.items():
            self.df[k] = evaluator.eval(eval_str=v, df=self.df)

    def _execute_manipulation_set(self):
        self.df = self.manipulation_set.execute(self.df)

    def join_with_parent(self):
        """
        Join this QueryNode with its parent node, using the defined join context.

        """
        if self.parent is None and self.df is None:
            raise QueryGraphException
        joined_df = self.join_context.apply_join(parent_df=self.parent.df, child_df=self.df)
        self.parent.df = joined_df

    def fold_children(self):
        """
        Join all QueryNode's with their parent in reverse topological order. This
        should only be called by the root QueryNode.

        """
        reverse_topological_ordering = list()
        for child in self:
            if child is not self:
                reverse_topological_ordering.insert(0, child)
        for query_node in reverse_topological_ordering:
            query_node.join_with_parent()

    def _execute(self, **independent_param_vals):
        """
        Execute this QueryNode's query. This requires:

            (1) Getting the parent node's dataframe, if one exists.
            (2) Passing the parent node's dataframe (if exists) to
                the QueryTemplate, and passing the independent parameter
                values, as defined in **kwargs, to the QueryTemplate.
            (3) Parsing the query - generating the actual query string
                that will be run on the node's database.
            (4) Getting the query results and defining the node's dataframe.
            (5) Creating any new columns.
            (6) Setting the node's 'already_executed' attribute to True.


        """
        query_template = QueryTemplate(query=self.query, db_connector=self.db_connector)
        if self.parent is not None:
            parent_df = self.parent.df
            df = query_template.execute(df=parent_df, **independent_param_vals)
        else:
            df = query_template.execute(**independent_param_vals)
        self.df = df
        if self.manipulation_set:
            self._execute_manipulation_set()
        self.already_executed = True

    def execution_thread(self, results_df_container, **independent_param_vals):
        thread_query_template = QueryTemplate(query=self.query, db_connector=copy.deepcopy(self.db_connector))
        if self.parent is None:
            parent_df = None
        else:
            parent_df = self.parent.df
        thread_query_template.pre_render(df=parent_df, **independent_param_vals)
        exec_thread = ExecutionThread(query_template=thread_query_template,
                                      node_name=self.name,
                                      result_df_container=results_df_container,
                                      independent_param_vals=independent_param_vals)
        return exec_thread

    def thread_process(self, results_dict):
        pass

    def execute(self, **independent_param_vals):
        """
        Execute the QueryGraph. If this QueryNode is the root node, then also
        fold all child nodes (join them with their parents).

        :param df:
        :param kwargs:
        :return:
        """
        print "Executing!"
        for query_node in self:
            query_node._execute(**independent_param_vals)
        if self.is_root_node:
            self.fold_children()


class OnColumn(object):

    def __init__(self, query_node, col_name):
        self.query_node = query_node
        self.col_name = col_name

    def __rrshift__(self, other):
        if isinstance(other, OnColumn):
            return {self.query_node.name: self.col_name, other.query_node.name: other.col_name}

    def __rshift__(self, other):
        if isinstance(other, OnColumn):
            return {self.query_node.name: self.col_name, other.query_node.name: other.col_name}


parent_node = QueryNode(query='', db_connector=connectors.daily_ts_connector, name='parent_node')
child_node = QueryNode(query='', db_connector=connectors.daily_ts_connector, name='child_node')

print parent_node['parent_col'] >> child_node['child_col']