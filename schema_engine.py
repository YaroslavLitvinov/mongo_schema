#!/usr/bin/env python

""" schema_engine.py: This module is providing classes and functions
for parsing bson data and transforming it's data to relational
structure.
'SchemaEngine' -- is loading json schema into tree of nodes;
'SchemaNode' -- is a main class for working with nodes of schema;
'DataEngine' -- data loader, for internal use;
'Tables' -- class for loading bson data into relational tables ('SqlTable');
'SqlTable' -- relational table with columns (SqlColumn);
'SqlColumn' -- table's column holds all column values.
"""

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import sys
import datetime
import json
import bson
import pprint
from bson import json_util
from logging import getLogger

def python_type_as_str(typo):
    """ python type to schema's type """
    res = None
    if typo is str or typo is unicode:
        res = "STRING"
    elif typo is int:
        res = "INT"
    elif typo is float:
        res = "DOUBLE"
    elif typo is datetime.datetime:
        res = "TIMESTAMP"
    elif typo is bool:
        res = "BOOLEAN"
    elif typo is bson.int64.Int64:
        res = "BIGINT"
    return res


def datetimes_flexible_tz(dt1, dt2):
    def datetime_no_tz(dtime):
        return datetime.datetime(dtime.year, dtime.month, dtime.day,
                                 dtime.hour, dtime.minute, dtime.second,
                                 dtime.microsecond)
    if dt1.tzinfo and not dt2.tzinfo:
        dt1 = datetime_no_tz(dt1)
    elif dt2.tzinfo and not dt1.tzinfo:
        dt2 = datetime_no_tz(dt2)
    else:
        dt2 = dt2.astimezone(dt1.tzinfo)
    return (dt1, dt2)

class SqlColumn:
    """ Column with values, related to self.node """
    def __init__(self, root, node):
        self.root = root
        self.node = node
        self.values = []
        if node.value == node.type_array:
            self.typo = 'BIGINT'
            if node.long_alias() == root.long_alias():
                self.name = 'idx'
            else:
                self.name = node.long_alias()+'_idx'
        else:
            self.name = node.short_alias()
            self.typo = node.value

    def __repr__(self): # pragma: no cover
        return '\n' + str(self.name) + ': ' + self.typo + \
            '; values: ' + str(self.values)

    def index_key(self):
        """ get index name or None if not index"""
        if self.node.value == self.node.type_array:
            index_key = self.node.long_alias()
            return index_key
        else:
            return None

class SqlTable:
    """ SqlTable is plain table which reflects array node's data,
    table columns are node childs, no columns related to nested arrays.

    self.sql_column_names - sorted column names list for deterministic
    columns order
    self.sql_columns dict of SqlColumn objects"""

    def __init__(self, root):
        """ Logical structure of table
        params:
        root - SchemaNode which is array node """
        assert(root.value == root.type_array)
        self.sql_column_names = []
        self.sql_columns = {}
        self.root = root
        self.table_name = root.long_plural_alias()
        for node in root.list_non_array_nodes():
            sqlcol = SqlColumn(root, node)
            self.sql_column_names.append(sqlcol.name)
            self.sql_columns[sqlcol.name] = sqlcol
        self.sql_column_names.sort()
        for idx_node in self.idx_nodes():
            sqlcol = SqlColumn(root, idx_node)
            self.sql_column_names.append(sqlcol.name)
            self.sql_columns[sqlcol.name] = sqlcol

    def __repr__(self): # pragma: no cover
        ppinter = pprint.PrettyPrinter(indent=4)
        whole_table = {}
        for colname in self.sql_column_names:
            whole_table[colname] = self.sql_columns[colname].values
        return ppinter.pformat(whole_table)

    def idx_nodes(self):
        idx_nodes = []
        parent_arrays = [i for i in self.root.all_parents() \
                             if i.value == i.type_array]
        for node in parent_arrays:
            # add node to list of index nodes only if:
            # 1. self table is not super root
            # 2. node from parents list is not a super root
            if self.root.parent and node.parent:
                idx_nodes.append(node)
        return idx_nodes

    def compare_with_sample(self, sample):
        """ sample is a dict with dict of tables """
        res = True
        if self.table_name not in sample:
            res = False
            getLogger(__name__).error("not equal: Couldn't locate table %s" % \
                                      self.table_name)
        else:
            sample_columns = sample[self.table_name]
            for column_name, data in sample_columns.iteritems():
                try:
                    sqlcolumn = self.sql_columns[column_name]
                except:
                    getLogger(__name__).error(sys.exc_info())
                    getLogger(__name__).error("Available columns are: %s" % \
                                              self.sql_columns.keys())
                    res = False
                if Tables.cmp_values(sqlcolumn.values, data):
                    getLogger(__name__).info("cmp %s ok" % column_name)
                else:
                    res = False
                    str_values = [str(i) for i in sqlcolumn.values ]
                    str_values2 = [str(i) for i in data ]
                    getLogger(__name__).error(
                        "not equal: %s.%s column values=%s, sample=%s" % \
                        (self.table_name, column_name, str_values, str_values2) )
        return res


class SchemaNode:
    """ Base class for creating tree of nodes from json schema """
    type_struct = 11 #'STRUCT'
    type_array = 12 #'ARRAY'

    def __init__(self, parent):
        self.parent = parent
        self.children = []
        self.all_parents_cached = None
        self.long_alias_cached = None
        self.public_name_cached = None
        self.name_components_cached = None
        self.name = None
        self.value = None
        self.reference = None

    def __repr__(self): # pragma: no cover
        gap = ''.join(['----' for s in xrange(len(self.all_parents()))])
        strval = "%s%s : %s" % (gap, self.public_name(), self.value)
        for child in self.children:
            strval += "\n"+child.__repr__()
        return strval

    def json_inject_data(self, value, object_id_name, object_id_val,
                         internal=False):
        """ Return artificially created bson object with value assigned
        to field corresponding to self node. Object id will be taken from
        object_id_val."""
        #getLogger(__name__).\
        #    debug('%s node_type=%s, long_alias=%s, internal=%s, value=%s' % \
        #              ("json_inject_data", self.value, self.long_alias(), 
        #               internal, value))
        res = value
        # if node itself is array
        if self.value is self.type_array:
            if internal:
                value = [value]
            # value must be array
            if self.parent and self.name:
                res = {self.name: value}
            else:
                res = value
        # if node itself is struct
        elif self.value is self.type_struct:
            # value must be struct
            if self.name:
                res = {self.name: value}
            else:
                res = value
            if not self.parent.parent \
                    and self.parent.value is self.type_array:
                res[object_id_name] = object_id_val
        # if node itself a base type field
        elif self.parent and self.parent.value is self.type_struct:
            res = {self.name: value}

        if self.parent:
            res = self.parent.json_inject_data(res,
                                               object_id_name,
                                               object_id_val,
                                               True)
        else:
            res = value
        return res

    def get_nested_array_type_nodes(self):
        """ return list of nested arrays """
        nodes = []
        for child in self.children:
            nodes.extend(child.get_nested_array_type_nodes())
            if child.value == self.type_array:
                nodes = [child] + nodes
        return nodes

    def all_parents(self):
        """ return list of parents and node itself, cache results """
        if self.all_parents_cached is not None:
            return self.all_parents_cached
        if self.parent:
            res = self.parent.all_parents() + [self]
        else:
            res = [self]
        self.all_parents_cached = res
        return res

    def get_id_node(self):
        """ Get id node from node's children list """
        node_parent_id = self.locate(['_id']) or self.locate(['id'])
        if node_parent_id:
            node_parent_id_oid = node_parent_id.locate(['oid'])
            if node_parent_id_oid:
                return node_parent_id_oid
            else:
                return node_parent_id
        return None

    def list_non_array_nodes(self):
        """ Get child nodes list except of array nodes """
        fields = []
        for item in self.children:
            if item.value != self.type_array:
                if item.value != self.type_struct:
                    fields.append(item)
                fields.extend(item.list_non_array_nodes())
        return fields

    def locate(self, names_list):
        """ return SchemaNode object
        names_list -- components list identifying specific node"""
        for item in self.children:
            if not item.name and item.value == self.type_struct:
                return item.locate(names_list)
            elif item.name == names_list[0]:
                rest = names_list[1:]
                if not len(rest):
                    return item
                else:
                    return item.locate(rest)
        return None

    def load(self, name, json_schema):
        """ load json schema and assign it to field name
        name --
        json_schema --"""
        self.name = name
        if type(json_schema) is dict:
            self.value = self.type_struct
            for key, val in json_schema.iteritems():
                child = SchemaNode(self)
                child.load(key, val)
                self.children.append(child)
        elif type(json_schema) is list:
            self.value = self.type_array
            for val in json_schema:
                child = SchemaNode(self)
                child.load(None, val)
                self.children.append(child)
        else:
            self.value = json_schema

    def super_parent(self):
        # get root for super parent (collection itself)
        root = [i for i in self.all_parents() \
                    if i.value == i.type_array and not i.parent][0]
        return root

    def add_parent_reference(self):
        """ For every array node except root node add to self node references
        to parent nodes """
        root = self.super_parent()
        idnode = root.get_id_node()
        # if self node is not a root and idnode is located
        if self.long_alias() != root.long_alias() and idnode:
            child = SchemaNode(self)
            child.name = idnode.long_alias()
            child.value = idnode.value
            child.reference = idnode
            self.children.append(child)

    def name_components(self):
        """ List of names of parent components """
        if self.name_components_cached is not None:
            return self.name_components_cached
        components = [i.name for i in self.all_parents()[1:] if i.name]
        self.name_components_cached = components
        return components


#methods related to naming conventions

    def public_name(self):
        """ Name of node exposed by public interface """
        if self.public_name_cached is not None:
            return self.public_name_cached
        temp = ''
        if self.name:
            temp = self.name
        elif not self.name and self.parent.value == self.type_array:
            # the same name for "array" and "noname struct in array"
            temp = self.parent.name
        if len(temp) and temp[0] == '_':
            self.public_name_cached = temp[1:]
        else:
            self.public_name_cached = temp
        return self.public_name_cached

    def public_nonplural_name(self):
        """ For array node get non plural name, else get public_name() """
        externname = self.public_name().lower()
        if self.value == self.type_array and \
           len(externname) and externname[-1] == 's':
            return externname[:-1]
        else:
            return externname

    def short_alias(self):
        """ Short name of node"""
        if self.reference:
            # long_alias is always used for referenced item name
            return self.public_name()
        else:
            parent_name = ''
            if self.parent and self.parent.name and \
               self.parent.value == self.type_struct:
#parent is named struct
                parent_name = self.parent.short_alias()
                return '_'.join([parent_name, self.public_name()])
            elif self.name:
                return self.public_name()
            elif not self.name and self.value != self.type_struct:
#non struct node with empty name
                return self.parent.public_name()
            else:
#struct without name
                return ''

    def long_alias(self, delimeter='_'):
        """ Name of node where name components joined and separated by delim.
        @param delimiter alternative delimiter between components """
        if self.long_alias_cached is not None:
            return self.long_alias_cached
        if self.reference:
            # long_alias is always used for referenced item name
            self.long_alias_cached = self.name
        else:
            parents = self.all_parents()
            components = [item.public_name() for item in parents if item.name]
            self.long_alias_cached = delimeter.join(components)
        return self.long_alias_cached

    def long_plural_alias(self, delimiter='_'):
        """ 'Plural' name of self node constructed from name components.
        It keeps trailing 's' char and removes 's' chars in components[:-1]
        @param delimiter alternative delimiter between components"""
        if self.reference:
            # long_alias is always used for referenced item name
            return self.name
        else:
            components = []
            parents = self.all_parents()
            num = 0
            for item in parents:
                num += 1
                if item.value == self.type_array and num != len(parents):
                    components.append(item.public_nonplural_name())
                elif item.name:
                    components.append(item.public_name())
            return delimiter.join(components)


class SchemaEngine:
    """ holds a tree of nodes, and json schema itself """
    def __init__(self, name, schema):
        self.root_node = SchemaNode(None)
        self.root_node.load(name, schema)
        for item in self.root_node.get_nested_array_type_nodes():
            if item.children[0].value == item.type_struct:
                item.children[0].add_parent_reference()
            else:
                item.add_parent_reference()
        self.schema = schema

    def locate(self, fields_list):
        """ return SchemaNode object
        fields_list -- components list identifying specific node"""
        return self.root_node.locate(fields_list)

    def get_tables_list(self, delimiter='_'):
        """ Unordered list of "table names" where optiopnal delimiter can be \
        placed between name components.
        delimiter -- if default delim is using then result will be a list
        of table names as returned by Tables.tables.keys(),
        optional delim can be placed between name components"""
        table_names = [self.root_node.long_plural_alias(delimiter)] + \
                      [i.long_plural_alias(delimiter) for i in \
                       self.root_node.get_nested_array_type_nodes()]
        return table_names


class DataEngine:
    """ Must not be used directly.
    Tables class must be used instead for loading data"""
    def __init__(self, root, bson_data, callback, callback_param):
        """
        root -- root node of table
        bson_data -- Data in bson format
        callback -- Function which fills tables by data
        callback_param -- external param for callback"""
        self.root = root
        self.data = bson_data
        self.callback = callback
        self.callback_param = callback_param
        self.cursors = {}
        self.indexes = {} #Note: names of indexes is differ from col names

    def inc_single_index(self, key_name):
        """ Increment row index, to be to load value for next row.
        key_name -- index name to increment"""
        if key_name not in self.indexes:
            self.indexes[key_name] = 0
        self.indexes[key_name] += 1

    def reset_single_index(self, key_name):
        """ Prepare index to load new array.
        key_name -- index name to increment"""
        self.indexes[key_name] = 0

    def load_tables_skeleton_recursively(self, node):
        """ Do initial sqltables load.
        node --"""
        if node.value == node.type_struct:
            for child in node.children:
                if child.value == child.type_struct or \
                   child.value == child.type_array:
                    self.load_tables_skeleton_recursively(child)
        elif node.value == node.type_array:
#if expected and real types are the same
            self.load_tables_skeleton_recursively(node.children[0])
            self.callback(self.callback_param, node)

    def load_data_recursively(self, data, node):
        """ Do initial data load. Calculate data indexes,
            exec callback for every new array
        params:
        data -- data corresponding to node
        node -- node corresponding to data"""
        if node.value == node.type_struct:
            for child in node.children:
                if child.value == child.type_struct or \
                   child.value == child.type_array:
                    if type(data) is not dict and type(data) is not list:
                        getLogger(__name__).warning(\
                            'Wrong type %s for %s. Data is: %s' % \
                                (str(type(data)),
                                 node.long_alias(),
                                 str(data)
                                 ))
                    elif child.name in data:
                        self.load_data_recursively(data[child.name], child)
        elif node.value == node.type_array and type(data) is list:
#if expected and real types are the same
            key_name = node.long_alias()
            self.cursors[key_name] = 0
            self.reset_single_index(key_name)
            for data_i in data:
                self.inc_single_index(key_name)
                self.load_data_recursively(data_i, node.children[0])
                self.callback(self.callback_param, node)
                if self.cursors[key_name]+1 < len(data):
                    self.cursors[key_name] += 1

    def get_current_record_data(self, node):
        """ Returns data related to node, pointed by current cursor
        param:
        node -- node related to data for load"""
        if node.reference:
            return self.get_current_record_data(node.reference)
        curdata = self.data
        components = node.name_components()
        component_idx = 0

        for parnode in node.all_parents():
            if curdata is None:
                break
            if parnode.value == parnode.type_array:
                cursor = self.cursors[parnode.long_alias()]
                curdata = curdata[cursor]
            elif parnode.value == parnode.type_struct:
                if type(curdata) is bson.objectid.ObjectId:
                    pass
                else:
                    collection_name = parnode.all_parents()[0].name
                    fieldname = components[component_idx]
                    if type(curdata) is not dict and type(curdata) is not list:
                        getLogger(__name__).error("collection=%s field=%s bad type=%s data=%s" % 
                                                  (collection_name,
                                                   fieldname,
                                                   str(type(curdata)),
                                                   str(curdata)))
                        getLogger(__name__).error("components=%s, idx=%d" % 
                                                  (str(components), 
                                                   component_idx))
                    if fieldname in curdata.keys():
                        curdata = curdata[fieldname]
                        component_idx += 1
                    else:
                        curdata = None

        if node.parent and \
           node.parent.value == node.type_struct and \
           type(curdata) is bson.objectid.ObjectId:
            if node.name == 'oid':
                curdata = str(curdata)
            elif node.name == 'bsontype':
                curdata = 7
        return curdata

def load_table_callback(tables, node):
    """ Callback for loading bson arrays, which are table equivalents.
    It's using tables.data_engine 'DataEngine' as data source.
    params:
    tables -- 'Tables' object to write results
    node -- array node corresponding to table, which data is loading."""
    table_name = node.long_plural_alias()
    if table_name not in tables.tables:
        tables.tables[table_name] = SqlTable(node)
    sqltable = tables.tables[table_name]
    if tables.data_engine.data is None:
        #support to load just tables structure w/o data
        return
    for column_name, column in sqltable.sql_columns.iteritems():
        if column.node.value == column.node.type_array: #index
            idxcolkey = column.node.long_alias()
            column.values.append(tables.data_engine.indexes[idxcolkey])
        else:
            colval = tables.data_engine.get_current_record_data(column.node)
            valtype = python_type_as_str(type(colval))
            coltype = column.typo
            if valtype == column.typo \
                    or colval is None \
                    or (valtype == 'INT' and coltype == 'DOUBLE'):
                column.values.append(colval)
            elif (type(colval) is list and coltype == 'TINYINT') \
                    or (type(colval) is dict and coltype == 'TINYINT'):
                column.values.append(None)
            elif (type(colval) is list and coltype == 'STRING') \
                    or (type(colval) is dict and coltype == 'STRING'):
                column.values.append('')
            else:
                column.values.append(None)
                colname = column.node.long_alias(delimeter='.')
                coltype = column.typo
                if valtype == 'STRING':
                    colval = ''
                error = "wrong value %s(%s) for %s(%s)" % \
                            (str(colval), valtype, colname, coltype)
                if error in tables.errors.keys():
                    tables.errors[error] += 1
                else:
                    tables.errors[error] = 1

class Tables:
    """ Tables object intended to store all tables data related
    to one mongo record. It's exposing interface for loading data
    from file and memory, and gives comparator for Tables obj. """
    def __init__(self, schema_engine, bson_data):
        self.tables = {} # {'table_name' : SqlTable}
        self.data = bson_data
        self.schema_engine = schema_engine
        self.data_engine = \
                DataEngine(schema_engine.root_node, self.data, \
                           load_table_callback, self)
        self.errors = {}

    def load_all(self):
        """ Load data from self.data into self.tables """
        root = self.schema_engine.root_node
        if self.data is None:
            self.data_engine.load_tables_skeleton_recursively(root)
        else:
            self.data_engine.load_data_recursively(self.data, root)

    def load_external_tables_data(self, arrays_dict):
        """ Load external data into self.tables,
        result is equivalent to load_all() function.
        param:
        arrays_dict -- {table_name: [rows] }"""
        for name, array in arrays_dict.iteritems():
            sqltable = self.tables[name]
            for colname_i in xrange(len(sqltable.sql_column_names)):
                colname = sqltable.sql_column_names[colname_i]
                sqlcol = sqltable.sql_columns[colname]
                for row in array:
                    sqlcol.values.append(row[colname_i])
                self.tables[name].sql_columns[colname] = sqlcol

    def rec_id(self):
        collection_name = self.schema_engine.root_node.name
        id_node = self.schema_engine.root_node.get_id_node()
        sqlcol = self.tables[collection_name]\
            .sql_columns[id_node.short_alias()]
        return sqlcol.values[0]

    @staticmethod
    def cmp_values(vallist1, vallist2):
        res = True
        if len(vallist1) != len(vallist2):
            res = False
        else:
            for idx in xrange(len(vallist1)):
                val = vallist1[idx]
                val2 = vallist2[idx]
                if type(val) is datetime.datetime and \
                   type(val2) is datetime.datetime:
                    date1, date2 = datetimes_flexible_tz(val, val2)
                    if date1 != date2:
                        res = False
                elif val != val2:
                    res = False
        return res

    def compare(self, tables_obj):
        """ Compare table objects, return True if equal, or False if not.
        param:
        tables_obj -- 'Tables' object to compare to self"""
        def table_vals(sqltable):
            """ dict {colname, [values]} """
            res = {}
            for col_name, col in sqltable.sql_column_names:
                if len(col.values):
                    res[col_name] = col.values
            return res

        for table_name in self.tables:
            sqltable = self.tables[table_name]
            if table_name not in tables_obj.tables and \
                    table_vals(self.tables[table_name]) == {}:
                # table w/o values and absent tables are both empty
                continue
            sqltable2 = tables_obj.tables[table_name]
            if sqltable.sql_column_names != sqltable2.sql_column_names:
                msg_fmt = "not equal: Table %s has different columns %s and %s"
                getLogger(__name__).info(msg_fmt % (table_name,
                                                    sqltable.sql_column_names,
                                                    sqltable2.sql_column_names))
                return False
            for colname in sqltable.sql_column_names:
                sqlcol = sqltable.sql_columns[colname]
                sqlcol2 = sqltable2.sql_columns[colname]
                if len(sqlcol.values) != len(sqlcol2.values):
                    msg_fmt = "not equal: Column %s.%s has different \
rows count %d and %d"
                    getLogger(__name__).info(msg_fmt % (table_name, sqlcol.name,
                                                        len(sqlcol.values), 
                                                        len(sqlcol2.values)))
                    getLogger(__name__).debug('Different columns are: ' +
                                              "colvals1 = " + str(sqlcol.values) +
                                              "colvals2 = " + str(sqlcol2.values))
                    return False

                if not Tables.cmp_values(sqlcol.values, sqlcol2.values):
                    msg_fmt = "not equal: %s.%s column values val=%s, val2=%s"
                    str_values = [str(i) for i in sqlcol.values ]
                    str_values2 = [str(i) for i in sqlcol2.values ]
                    getLogger(__name__).info(msg_fmt % (table_name, 
                                                        sqlcol.name,
                                                        str_values,
                                                        str_values2))
                    return False
        return True

    def compare_with_sample(self, sample):
        res = True
        if sorted(self.tables.keys()) != sorted(sample.keys()):
            res = False
            getLogger(__name__).error(
                "different tables tables=%s, data_tables=%s" % \
                (sorted(self.tables.keys()), sorted(sample.keys()) ))
        for table_name, columns in sample.iteritems():
            if not self.tables[table_name].compare_with_sample(sample):
                res = False
        return res

    def is_empty(self):
        isempty = True
        for table_name, table in self.tables.iteritems():
            for colname, column in table.sql_columns.iteritems():
                if column.values != []:
                    isempty = False
                    break
        return isempty

def create_schema_engine(collection_name, schemapath):
    """ Returns 'SchemaEngine' object based on collection's schema file
    params:
    collection_name --
    schemapath -- json schema filepath"""
    with open(schemapath, "r") as input_schema_f:
        schema = [json.load(input_schema_f)]
        return SchemaEngine(collection_name, schema)

def create_tables_load_bson_data(schema_engine, bson_data):
    """ Create 'Tables' object from bson data
    params:
    bson_data -- one object in list"""
    tables = Tables(schema_engine, bson_data)
    tables.load_all()
    return tables

def create_tables_load_file(schema_engine, datapath):
    """ Create 'Tables' object from bson file on filesystem"""
    with open(datapath, "r") as input_f:
        data = input_f.read()
        bson_data = json_util.loads(data)
        getLogger(__name__).info("Loaded %d recs from %s"
                                 % (len(bson_data), datapath))
        return create_tables_load_bson_data(schema_engine, bson_data)

def log_table_errors(txtinfo, table_errors):
    """ Print to log Tables.errors
    params:
    txtinfo -- info to output
    table_errors -- Tables.errors dict"""
    if len(table_errors):
        getLogger(__name__).warning(txtinfo)
        ppinter = pprint.PrettyPrinter(indent=4)
        getLogger(__name__).warning(ppinter.pformat(table_errors))


if __name__ == "__main__": # pragma: no cover
    pass

