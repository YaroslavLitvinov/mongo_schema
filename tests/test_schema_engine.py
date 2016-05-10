#!/usr/bin/env python

import os
from bson.json_util import loads
from mongo_schema.schema_engine import create_schema_engine
from mongo_schema.schema_engine import create_tables_load_file
from mongo_schema.schema_engine import create_tables_load_bson_data
from mongo_schema.schema_engine import Tables
from mongo_schema.schema_engine import SqlColumn

files = {'a_inserts': ('test_files/json_schema2.txt',
                       'test_files/bson_data2.txt')}

def get_schema_engine(collection_name):
    dirpath=os.path.dirname(os.path.abspath(__file__))
    schema_fname = files[collection_name][0]
    schema_path = os.path.join(dirpath, schema_fname)
    return create_schema_engine(collection_name, schema_path)

def get_schema_tables(schema_engine_obj):
    collection_name = schema_engine_obj.root_node.name
    dirpath=os.path.dirname(os.path.abspath(__file__))
    data_fname = files[collection_name][1]
    data_path = os.path.join(dirpath, data_fname)
    tables_with_data = create_tables_load_file(schema_engine_obj, data_path)
    tables_no_data = create_tables_load_bson_data(schema_engine_obj, None)
    assert(tables_with_data.tables.keys() == tables_no_data.tables.keys())
    return tables_with_data

def get_test_node(full_path):
    engine = get_schema_engine( full_path[0] )
    assert(engine.root_node.all_parents()==[engine.root_node])
    if len(full_path) > 1:
        return engine.locate(full_path[1:])
    else:
        return engine.root_node

def test_all():
    full_path = ['a_inserts', 'comments', 'items']
    root = get_test_node([full_path[0]])
    assert("items" == root.locate(full_path[1:]).name)
    parents = [i.name \
               for i in root.locate(full_path[1:]).all_parents() \
               if i.name is not None]
    items_node = get_test_node(full_path)
    assert(items_node.value == items_node.type_array)
    assert(items_node.children[0].short_alias()=='')
    assert(items_node.get_id_node())
    field = get_test_node(full_path+['data'])
#the same name for "array" and "noname struct in array"
    assert(field.parent.public_name()==field.parent.parent.public_name())
    assert("data" == field.name)
    assert(full_path==parents)


def test_all_aliases():
    def test_alias(full_path, short_alias, long_alias, long_plural_alias):
        node1 = get_test_node(full_path)
        assert(short_alias == node1.short_alias())
        assert(long_alias == node1.long_alias())
        assert(long_plural_alias == node1.long_plural_alias())
#test parental field name
    test_alias(['a_inserts', 'comments', 'a_inserts_id_oid'], \
               'a_inserts_id_oid', 'a_inserts_id_oid', 'a_inserts_id_oid')
#test structrure field name
    test_alias(['a_inserts', 'comments', 'body'], \
               'body', 'a_inserts_comments_body', \
               'a_insert_comment_body')
#test nested struct field name
    test_alias(['a_inserts', 'comments', '_id', 'oid'], \
               'id_oid', 'a_inserts_comments_id_oid', \
               'a_insert_comment_id_oid')
#test 1 level array name
    test_alias(['a_inserts'], 'a_inserts', 'a_inserts', 'a_inserts')
#test nested array name
    test_alias(['a_inserts', 'comments', 'items'], \
               'items', 'a_inserts_comments_items', \
               'a_insert_comment_items')

def check_one_column(sqltable, colname, values):
    sqlcol = sqltable.sql_columns[colname]
    assert(sqlcol.name == colname)
    assert(sqlcol.values == values)

def check_a_inserts_table(tables):
    sqltable = tables.tables["a_inserts"]
    check_one_column(sqltable, 'body', ['body3'])
    check_one_column(sqltable, 'id_oid', ['56b8f05cf9fcee1b00000010'])

def check_comments_table(tables):
    sqltable = tables.tables["a_insert_comments"]
    check_one_column(sqltable, 'id_oid', ['56b8f05cf9fcee1b00000110',\
                                          '56b8f05cf9fcee1b00000011'])
    check_one_column(sqltable, 'body', ['body3', 'body2'])
    check_one_column(sqltable, 'idx', [1, 2])
    assert('a_inserts_idx' not in sqltable.sql_columns)

def check_items_table(tables):
    sqltable = tables.tables["a_insert_comment_items"]
    check_one_column(sqltable, 'data', ['1', '2'])
    check_one_column(sqltable, 'idx', [1, 2])
    check_one_column(sqltable, 'a_inserts_comments_idx', [1, 2])
    assert('a_inserts_idx' not in sqltable.sql_columns)

def check_indices_table(tables):
    sqltable = tables.tables["a_insert_comment_item_indices"]
    check_one_column(sqltable, 'idx', [1, 2, 3, 4, 5, 6])
    check_one_column(sqltable, 'a_inserts_comments_idx', [1, 1, 1, 2, 2, 2])
    check_one_column(sqltable, 'indices', [10, 11, 12, 13, 14, 15])
    assert('a_inserts_idx' not in sqltable.sql_columns)

def test_all_tables():
    collection_name = 'a_inserts'
    schema_engine_obj = get_schema_engine( collection_name )
    tables = get_schema_tables(schema_engine_obj)
    table_names_list1 = tables.tables.keys()
    table_names_list1.sort()
    table_names_list2 = schema_engine_obj.get_tables_list()
    table_names_list2.sort()
    assert(table_names_list2 == table_names_list1)
    assert(table_names_list1 == ['a_insert_comment_item_indices',
                                 'a_insert_comment_items', 
                                 'a_insert_comments',
                                 'a_inserts'])
    check_a_inserts_table(tables)
    check_comments_table(tables)
    check_items_table(tables)
    check_indices_table(tables)
    return tables

def test_partial_record1():
    locate_name = ['comments']
    bson_raw_id_data = '{"_id": {"$oid": "56b8da51f9fcee1b00000006"}}'
    array_bson_raw_data = '[{\
"_id": {"$oid": "56b8f344f9fcee1b00000018"},\
"updated_at": {"$date": "2016-02-08T19:57:56.678Z"},\
"created_at": {"$date": "2016-02-08T19:57:56.678Z"}}]'
    collection_name = 'a_inserts'
    schema_engine = get_schema_engine( collection_name )
    node = schema_engine.locate(locate_name)
    bson_object_id = loads(bson_raw_id_data) 
    bson_data = loads(array_bson_raw_data) 
    whole_bson = node.json_inject_data(bson_data, 
                                       bson_object_id.keys()[0],
                                       bson_object_id.values()[0])
    print whole_bson
    tables = Tables(schema_engine, whole_bson)
    tables.load_all()
    print "tables.tables.keys()", tables.tables.keys()
    table_name = 'a_insert_comments'
    comments_t = tables.tables[table_name]
    print "sql_column_names", comments_t.sql_column_names
    assert(comments_t.sql_columns['updated_at'])
    id_oid = comments_t.sql_columns['id_oid'].values[0]
    print "id_oid", id_oid
    assert(id_oid == "56b8f344f9fcee1b00000018")
    assert(len(comments_t.sql_columns['id_oid'].values) == 1)
    parent_id_oid = comments_t.sql_columns['a_inserts_id_oid'].values[0]
    print "parent_id_oid", parent_id_oid
    assert(parent_id_oid == "56b8da51f9fcee1b00000006")
    #both tables: a_inserts, comments should be available
    assert(len(tables.tables.keys())==2)


def test_partial_record2():
    locate_name = ['comments', 'items', 'indices']
    bson_raw_id_data = '{"_id": {"$oid": "56b8da51f9fcee1b00000006"}}'
    array_bson_raw_data = '[21, 777]'
    collection_name = 'a_inserts'
    schema_engine = get_schema_engine( collection_name )
    node = schema_engine.locate(locate_name)
    bson_object_id = loads(bson_raw_id_data) 
    bson_data = loads(array_bson_raw_data) 
    whole_bson = node.json_inject_data(bson_data, 
                                       bson_object_id.keys()[0],
                                       bson_object_id.values()[0])
    print whole_bson
    tables = Tables(schema_engine, whole_bson)
    tables.load_all()
    print "tables.tables.keys()", tables.tables.keys()
    table_name = 'a_insert_comment_item_indices'
    indices_t = tables.tables[table_name]
    print "sql_column_names", indices_t.sql_column_names
    assert(indices_t.sql_columns['indices'])
    # verify parent ids
    parent_id_oid = indices_t.sql_columns['a_inserts_id_oid'].values[0]
    # Old bag fixed: no more parent ids
    assert('a_inserts_comments_id_oid' not in indices_t.sql_columns.keys())
    assert('a_inserts_comments_items_id_oid' not in indices_t.sql_columns.keys())
    assert('a_inserts_comments_items_indices_id_oid' not in indices_t.sql_columns.keys())
    print "parent_id_oid", parent_id_oid
    assert(parent_id_oid == "56b8da51f9fcee1b00000006")
    #both tables: a_inserts, comments should be available
    assert(len(tables.tables.keys())==4)


def test_comparator1():
    tables1 = test_all_tables()
    tables2 = test_all_tables()
    assert(tables1.compare(tables2))

def test_comparator2():
    tables1 = test_all_tables()
    # inject error 
    tmp_tables = test_all_tables()
    tmp_node = tmp_tables.tables['a_insert_comments'].root
    tmp_tables.tables['a_insert_comments'].sql_column_names.append('foo')
    tmp_tables.tables['a_insert_comments'].sql_columns['foo'] = SqlColumn(tmp_node, tmp_node)
    assert(not tables1.compare(tmp_tables))

def test_comparator3():
    tables1 = test_all_tables()
    # inject error 
    tmp_tables = test_all_tables()
    tmp_tables.tables['a_insert_comments'].sql_columns['body'].values[0] = 'error'
    assert(not tables1.compare(tmp_tables))

def test_comparator4():
    tables1 = test_all_tables()
    # inject error 
    tmp_tables = test_all_tables()
    tmp_tables.tables['a_insert_comments'].sql_columns['body'].values.append('error2')
    assert(not tables1.compare(tmp_tables))


def test_external_data_loader():

    def table_rows_list(table):
        """ get list of rows, every row is values list
        @param table object schema_engine.SqlTable"""
        res = []
        firstcolname = table.sql_column_names[0]
        reccount = len(table.sql_columns[firstcolname].values)
        for val_i in xrange(reccount):
            values = []
            for column_name in table.sql_column_names:
                col = table.sql_columns[column_name]
                values.append(col.values[val_i])
            res.append( values )
        return res

    tables1 = test_all_tables()
    ext_tables = create_tables_load_bson_data(tables1.schema_engine, None)
    ext_tables_data = {}
    for name in tables1.tables:
        ext_tables_data[name] = table_rows_list(tables1.tables[name])
    ext_tables.load_external_tables_data(ext_tables_data)
    assert(tables1.compare(ext_tables))



if __name__=='__main__':
    test_external_data_loader()
