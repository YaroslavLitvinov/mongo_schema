#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import sys
import datetime
import json
import bson
from bson import json_util

def message(mes, cr='\n'):
    sys.stderr.write( mes + cr)

def python_type_as_str(t):
    if t is str or t is unicode:
        return "STRING"
    elif t is int:
        return "INT"
    elif t is float:
        return "DOUBLE"
    elif t is datetime.datetime:
        return "TIMESTAMP"
    elif t is bool:
        return "BOOLEAN"
    elif t is bson.int64.Int64:
        return "BIGINT"
    else:
        return None

class SqlColumn:
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
            None

class SqlTable:
    def __init__(self, root):
        """ Logical structure of table """
        assert(root.value == root.type_array)
        self.sql_column_names = []
        self.sql_columns = {}
        self.root = root
        self.table_name = root.long_plural_alias()
        for node in root.list_non_array_nodes():
            sqlcol = SqlColumn(root, node)
            self.sql_column_names.append(sqlcol.name)
            self.sql_columns[sqlcol.name] = sqlcol
        parent_arrays = [i for i in root.all_parents() \
                         if i.value == i.type_array]
        self.sql_column_names.sort()
        for parent_array in parent_arrays: 
#add indexes
            # Allow to add index only if 
            # 1. table is not super root
            # 2. if index is not referring to super root
            if root.parent and parent_array.parent:
                sqlcol = SqlColumn(root, parent_array)
                self.sql_column_names.append(sqlcol.name)
                self.sql_columns[sqlcol.name] = sqlcol

    def __repr__(self): # pragma: no cover
        return self.table_name + ' ' + str(self.sql_columns)

class SchemaNode:
    type_struct = 11 #'STRUCT'
    type_array = 12 #'ARRAY'

    def __init__(self, parent):
        self.parent = parent
        self.children = []
        self.all_parents_cached = None
        self.long_alias_cached = None
        self.external_name_cached = None
        self.name_components_cached = None
        self.name = None
        self.value = None
        self.reference = None

    def __repr__(self): # pragma: no cover
        gap = ''.join(['----' for s in xrange(len(self.all_parents()))])
        s = "%s%s : %s" % (gap, self.external_name(), self.value)
        for c in self.children:
            s += "\n"+c.__repr__()
        return s

    def json_inject_data(self, value, object_id_name, object_id_val, 
                         internal = False):
        # print "json_inject_data", self.value, internal, value
        res = value
        if self.value is self.type_array:
            if internal:
                value = [value]
            # value must be array
            if self.parent and self.name:
                res = {self.name: value}
            else:
                res = value
        elif self.value is self.type_struct:
            # value must be struct
            if self.name:
                res = {self.name: value}
            else:
                res = value
            if not self.parent.parent \
                    and self.parent.value is self.type_array:
                node_name = self.get_id_node().name
                res[object_id_name] = object_id_val
            
        if self.parent:
            return self.parent.json_inject_data(res, 
                                                object_id_name, 
                                                object_id_val,
                                                True)
        else:
            return res

    def get_nested_array_type_nodes(self):
        l = []
        for i in self.children:
            l.extend(i.get_nested_array_type_nodes())
            if i.value == self.type_array:
                l = [i] + l
        return l

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
        node_parent_id = self.locate(['_id']) or self.locate(['id'])
        if node_parent_id:
            node_parent_id_oid = node_parent_id.locate(['oid'])
            if node_parent_id_oid:
                return node_parent_id_oid
            else:
                return node_parent_id

    def list_non_array_nodes(self):
        fields = []
        for item in self.children:
            if item.value != self.type_array:
                if item.value != self.type_struct:
                    fields.append(item)
                fields.extend( item.list_non_array_nodes() )
        return fields

    def locate(self, names_list):
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
        self.name = name
        if type(json_schema) is dict:
            self.value = self.type_struct
            for k, v in json_schema.iteritems():
                child = SchemaNode(self)
                child.load(k, v)
                self.children.append(child)
        elif type(json_schema) is list:
            self.value = self.type_array
            for v in json_schema:
                child = SchemaNode(self)
                child.load(None, v)
                self.children.append(child)
        else:
            self.value = json_schema

    def add_parent_references(self):
        # find root
        root = [i for i in self.all_parents() \
                    if i.value == i.type_array and not i.parent][0]
        idnode = root.get_id_node()
        if self.long_alias() != root.long_alias() and idnode:
            child = SchemaNode(self)
            child.name = '_'.join([item.external_name() \
                                   for item in idnode.all_parents() \
                                   if item.name])
            child.value = idnode.value
            child.reference = idnode
            self.children.append(child)

    def name_components(self):
        if self.name_components_cached is not None:
            return self.name_components_cached
        components = [i.name for i in self.all_parents()[1:] if i.name]
        self.name_components_cached = components
        return components


#methods related to naming conventions

    def external_name(self):
        if self.external_name_cached is not None:
            return self.external_name_cached
        temp = ''
        if self.name:
            temp = self.name 
#the same name for "array" and "noname struct in array"
        elif not self.name and self.parent.value == self.type_array:
            temp = self.parent.name
        if len(temp) and temp[0] == '_':
            self.external_name_cached = temp[1:]
        else:
            self.external_name_cached = temp
        return self.external_name_cached

    def external_nonplural_name(self):
        externname = self.external_name().lower()
        if self.value == self.type_array and \
           len(externname) and externname[-1] == 's':
            return externname[:-1]
        else:
            return externname        

    def short_alias(self):
        if self.reference:
#for reference items the name is always long_alias()
           return self.external_name()
        else:
            parent_name = ''
            if self.parent and self.parent.name and \
               self.parent.value == self.type_struct:
#parent is named struct
                parent_name = self.parent.short_alias()
                return '_'.join([parent_name, self.external_name()])
            elif self.name:
                return self.external_name()
            elif not self.name and self.value != self.type_struct:
#non struct node with empty name
                return self.parent.external_name()
            else:
#struct without name
                return ''

    def long_alias(self, delimeter = '_'):
        if self.long_alias_cached is not None:
            return self.long_alias_cached
        if self.reference:
            self.long_alias_cached = self.name
        else:
            p = self.all_parents()
            self.long_alias_cached = delimeter.join([item.external_name() for item in p if item.name])
        return self.long_alias_cached

    def long_plural_alias(self, delimiter='_'):
        if self.reference:
#reference items name always is long_alias()
           return self.name
        else:
            l = []
            p = self.all_parents()
            n = 0
            for item in p:
                n += 1
                if item.value == self.type_array and n != len(p):
                    l.append(item.external_nonplural_name())
                elif item.name:
                    l.append(item.external_name())
            return delimiter.join(l)


class SchemaEngine:
    def __init__(self, name, schema):
        self.root_node = SchemaNode(None)
        self.root_node.load(name, schema)
        for item in self.root_node.get_nested_array_type_nodes():
            if item.children[0].value == item.type_struct:
                item.children[0].add_parent_references()
            else:
                item.add_parent_references()
        self.schema = schema

    def locate(self, fields_list):
        """ return SchemaNode object"""
        return self.root_node.locate(fields_list)

    def get_tables_list(self, delimiter='_'):
        table_names = [self.root_node.long_plural_alias(delimiter)] + \
                      [i.long_plural_alias(delimiter) for i in \
                       self.root_node.get_nested_array_type_nodes()]
        return table_names


class DataEngine:
    def __init__(self, root, bson_data, callback, callback_param):
        """ 
        @param root - SchemaNode root node for table
        @param bson_data Data in bson format
        @param callback Function fill tables by data"""
        self.root = root
        self.data = bson_data
        self.callback = callback
        self.callback_param = callback_param
        self.cursors = {}
        self.indexes = {} #Note: names of indexes is differ from col names

    def inc_single_index(self, key_name):
        if key_name not in self.indexes:
            self.indexes[key_name] = 0
        self.indexes[key_name] += 1

    def load_tables_structure_only_recursively(self, node):
        """ Do initial sqltables load."""
        if node.value == node.type_struct:
            for child in node.children:
                if child.value == child.type_struct or \
                   child.value == child.type_array:
                    self.load_tables_structure_only_recursively(child)
        elif node.value == node.type_array:
#if expected and real types are the same
            self.load_tables_structure_only_recursively(node.children[0])
            self.callback(self.callback_param, node)

    def load_data_recursively(self, data, node):
        """ Do initial data load. Calculate data indexes, 
            exec callback for every new array"""
        if node.value == node.type_struct:
            for child in node.children:
                if child.value == child.type_struct or \
                   child.value == child.type_array:
                    if child.name in data:
                        self.load_data_recursively(data[child.name], child)
        elif node.value == node.type_array and type(data) is list:
#if expected and real types are the same
            key_name = node.long_alias()
            self.cursors[key_name] = 0
            for data_i in data:
                self.inc_single_index(key_name)
                self.load_data_recursively(data_i, node.children[0])
                self.callback(self.callback_param, node)
                if self.cursors[key_name]+1 < len(data):
                    self.cursors[key_name] += 1

    def get_current_record_data(self, node):
        """ Get current data pointed by cursors"""
        if node.reference:
            return self.get_current_record_data(node.reference)
        curdata = self.data
        components = node.name_components()
        component_idx = 0

        for parnode in node.all_parents():
            if not curdata:
                break
            if parnode.value == parnode.type_array:
                cursor = self.cursors[parnode.long_alias()]
                curdata = curdata[cursor]
            elif parnode.value == parnode.type_struct:
                if type(curdata) is bson.objectid.ObjectId:
                    pass
                else:
                    fieldname = components[component_idx]
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
            column.values.append( tables.data_engine.indexes[idxcolkey] )
        else:
            colval = tables.data_engine.get_current_record_data(column.node)
            valtype = python_type_as_str(type(colval))
            coltype = column.typo
            if valtype == column.typo \
                    or colval is None \
                    or (valtype == 'INT' and coltype == 'DOUBLE'):
                column.values.append( colval )
            elif (type(colval) is list and coltype == 'TINYINT') \
                    or (type(colval) is dict and coltype == 'TINYINT'):
                column.values.append( None )
            elif (type(colval) is list and coltype == 'STRING') \
                    or (type(colval) is dict and coltype == 'STRING'):
                column.values.append( '' )
            else:
                column.values.append( None )
                colname = column.node.long_alias(delimeter='.')
                coltype = column.typo
                if valtype == 'STRING':
                    colval = ''
                error = "wrong value %s(%s) for %s(%s)" % \
                            (str(colval), valtype, colname, coltype )
                if error in tables.errors.keys():
                    tables.errors[error] += 1
                else:
                    tables.errors[error] = 1

class Tables:
    def __init__(self, schema_engine, bson_data):
        self.tables = {}
        self.data = bson_data
        self.schema_engine = schema_engine
        self.data_engine = \
                DataEngine(schema_engine.root_node, self.data, \
                           load_table_callback, self)
        self.errors = {}

    def load_all(self):
        """ Load data into root"""
        root = self.schema_engine.root_node
        if self.data is None:
            self.data_engine.load_tables_structure_only_recursively(root)
        else:
            self.data_engine.load_data_recursively(self.data, root)

    def load_external_tables_data(self, arrays_dict):
        """ @param arrays_dict {table_name: rows }"""
        for name, array in arrays_dict.iteritems():
            sqltable = self.tables[name]
            for colname_i in xrange(len(sqltable.sql_column_names)):
                colname = sqltable.sql_column_names[colname_i]
                sqlcol = sqltable.sql_columns[colname]
                for row in array:
                    sqlcol.values.append(row[colname_i])
                self.tables[name].sql_columns[colname] = sqlcol
 
    def compare(self, tables_obj):
        """ Get separate bson record. Collect data from all tables and columns. 
        Operation is opposite to self.load_all()"""
        res = {}
        cursors = {}
        if self.tables.keys() != tables_obj.tables.keys():
            return False
        for table_name in self.tables:
            sqltable = self.tables[table_name]
            sqltable2 = tables_obj.tables[table_name]
            if sqltable.sql_column_names != sqltable2.sql_column_names:
                msg_fmt = "Table %s has different columns %s and %s"
                message(msg_fmt % (table_name, 
                                   sqltable.sql_column_names,
                                   sqltable2.sql_column_names))
                return False
            for colname in sqltable.sql_column_names:
                sqlcol = sqltable.sql_columns[colname]
                sqlcol2 = sqltable2.sql_columns[colname]
                if len(sqlcol.values) != len(sqlcol2.values):
                    msg_fmt = "Column %s.%s has different rows count %d and %d"
                    message(msg_fmt % (table_name, sqlcol.name,
                                       len(sqlcol.values), len(sqlcol2.values)))
                    return False
                for idx in xrange(len(sqlcol.values)):
                    val = sqlcol.values[idx]
                    val2 = sqlcol2.values[idx]
                    if val != val2:
                        msg_fmt = "Column %s.%s[%d] has different values %s and %s"
                        message(msg_fmt % (table_name, sqlcol.name, idx,
                                           str(val), str(val2)))
                        return False
        return True

def create_schema_engine(collection_name, schemapath):
    with open(schemapath, "r") as input_schema_f:
        schema = [json.load(input_schema_f)]
        return SchemaEngine(collection_name, schema)

def create_tables_load_bson_data(schema_engine, bson_data):
    tables = Tables(schema_engine, bson_data)
    tables.load_all()
    return tables

def create_tables_load_file(schema_engine, datapath):
    with open(datapath, "r") as input_f:
        data = input_f.read()
        bson_data = json_util.loads(data)
        return create_tables_load_bson_data(schema_engine, bson_data)

if __name__ == "__main__": # pragma: no cover
    from test_schema_engine import test_all_tables
    test_all_tables()

