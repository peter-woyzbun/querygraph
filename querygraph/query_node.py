from collections import OrderedDict
import multiprocessing

import pandas as pd

from querygraph.exceptions import QueryGraphException
from querygraph.query_templates.query_template import QueryTemplate
from querygraph.db.connectors import DatabaseConnector
from querygraph.evaluation.evaluator import Evaluator


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
        self.join_context = None
        self.df = None
        self.already_executed = False
        self._new_columns = OrderedDict()

    def is_independent(self):
        if self.is_root_node:
            return True
        else:
            query_template = QueryTemplate(query=self.query)
            return not query_template.has_dependent_parameters()

    @property
    def is_root_node(self):
        return self.parent is None

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

    def join(self, child_node, on_columns, how='inner'):
        """
        Join this node with the given child node. This creates an edge between
        the two nodes.

        Parameters
        ----------

        child_node : QueryNode
            The QueryNode to join with.
        on_columns : dict
            A dictionary whose key-value pairs indicate on what columns should be
            used to join the child node's and the parent node's query result
            dataframes. The key should be a column in the parent's (this) dataframe,
            the value should be a key in the child's dataframe.
        how : {'left', 'right', 'outer', 'inner'}, default 'inner'
            What type of join to use.

        """
        if not isinstance(child_node, QueryNode):
            raise JoinException("The child node to join with must be a QueryNode instance.")
        # Check that the child node doesn't already have a parent.
        if child_node.parent is not None:
            raise JoinException("You trying to join parent node '%s' with child node '%s', but '%s' already"
                                "has a parent ('%s')." % (self.name,
                                                          child_node.name,
                                                          child_node.name,
                                                          child_node.parent.name))
        # Make sure joining with the child node wont cause an infinite loop (cycle) in the graph.
        if self.creates_cycle(child_node):
            raise CycleException("Joining parent node '%s' with child node '%s' would create a cycle in the "
                                 "graph." % (self.name, child_node.name))
        join_context = {'how': how, 'on_columns': on_columns}
        child_node.parent = self
        child_node.join_context = join_context
        self.children.append(child_node)
        return self

    def join_with_parent(self):
        """
        Join this QueryNode with its parent node, using the defined join context.

        """
        if self.parent is None and self.df is None:
            raise QueryGraphException
        left_on = self.join_context['on_columns'].keys()
        right_on = self.join_context['on_columns'].values()
        parent_df = self.parent.df
        joined_df = parent_df.merge(self.df, how=self.join_context['how'], left_on=left_on, right_on=right_on)
        self.parent.df = joined_df

    def _fold_children(self):
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

    def _execute(self, **kwargs):
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
        query_template = QueryTemplate(query=self.query)
        if self.parent is not None:
            parent_df = self.parent.df
            parsed_query = query_template.parse(df=parent_df, independent_params=kwargs)
        else:
            parsed_query = query_template.parse(independent_params=kwargs)
        df = self.db_connector.execute_query(parsed_query)
        self.df = df
        if self._new_columns:
            self._create_added_columns()
        self.already_executed = True

    def execute(self, df=None, **kwargs):
        """
        Execute the QueryGraph. If this QueryNode is the root node, then also
        fold all child nodes (join them with their parents).

        :param df:
        :param kwargs:
        :return:
        """
        print "Executing!"
        for query_node in self:
            query_node._execute(**kwargs)
        if self.is_root_node:
            self._fold_children()


if __name__ == '__main__':
    multiprocessing.freeze_support()