'''
Django bulk operations on simple models.
Does not attempt to cover all corner cases and related models.

Originally from http://people.iola.dk/olau/python/bulkops.py

'''
from itertools import repeat
from django.db import models, connections, transaction


def _model_fields(model):
    return [f for f in model._meta.fields
            if not isinstance(f, models.AutoField)]


def _prep_values(fields, obj, con):
    return tuple(f.get_db_prep_save(f.pre_save(obj, True), connection=con)
                 for f in fields)


def _insert_many(model, objects, using="default"):
    """
    Insert list of Django objects in one SQL query. Objects must be of the same
    Django model. Note that save is not called and signals on the model are not
    raised.

    """
    if not objects:
        return

    con = connections[using]

    fields = _model_fields(model)
    parameters = [_prep_values(fields, o, con) for o in objects]

    table = model._meta.db_table
    col_names = ",".join(con.ops.quote_name(f.column) for f in fields)
    placeholders = ",".join(repeat("%s", len(fields)))

    sql = "INSERT INTO %s (%s) VALUES (%s)" % (table, col_names, placeholders)
    con.cursor().executemany(sql, parameters)


def insert_many(*args, **kwargs):
    _insert_many(*args, **kwargs)
    transaction.commit_unless_managed()


def _update_many(model, objects, keys=None, using="default"):
    """
    Update list of Django objects in one SQL query. Objects must be of the same
    Django model. Note that save is not called and signals on the model are not
    raised.

    """
    if not objects:
        return

    # If no keys specified, use the primary key by default
    keys = keys or [model._meta.pk.name]

    con = connections[using]

    # Split the fields into the fields we want to update and the fields we want
    # to update by in the WHERE clause.
    key_fields = [f for f in model._meta.fields if f.name in keys]
    value_fields = [f for f in _model_fields(model) if f.name not in keys]

    assert key_fields, "Empty key fields"

    # Combine the fields for the parameter list
    param_fields = value_fields + key_fields
    parameters = [_prep_values(param_fields, o, con) for o in objects]

    # Build the SQL
    table = model._meta.db_table
    assignments = ",".join(("%s=%%s" % con.ops.quote_name(f.column))
                           for f in value_fields)
    where_keys = " AND ".join(("%s=%%s" % con.ops.quote_name(f.column))
                              for f in key_fields)
    sql = "UPDATE %s SET %s WHERE %s" % (table, assignments, where_keys)
    con.cursor().executemany(sql, parameters)


def update_many(*args, **kwargs):
    _update_many(*args, **kwargs)
    transaction.commit_unless_managed()


def insert_or_update_many(model, objects, keys=None, using="default"):
    '''
    Insert or update a list of Django objects.

    Does not work with SQLite as it does not support tuple comparison.

    '''
    if not objects:
        return

    keys = keys or [model._meta.pk.name]
    con = connections[using]

    # Select key tuples from the database to find out which ones need to be
    # updated and which ones need to be inserted.
    key_fields = [f for f in model._meta.fields if f.name in keys]
    assert key_fields, "Empty key fields"

    object_keys = [(o, _prep_values(key_fields, o, con)) for o in objects]
    parameters = [i for (_, k) in object_keys for i in k]

    table = model._meta.db_table
    col_names = ",".join(con.ops.quote_name(f.column) for f in key_fields)

    # repeat tuple values
    tuple_placeholder = "(%s)" % ",".join(repeat("%s", len(key_fields)))
    placeholders = ",".join(repeat(tuple_placeholder, len(objects)))

    sql = "SELECT %s FROM %s WHERE (%s) IN (%s)" % (
        col_names, table, col_names, placeholders)
    cursor = con.cursor()
    cursor.execute(sql, parameters)
    existing = set(cursor.fetchall())

    # Split the objects that need to be updated and the objects that need to be
    # inserted.
    update_objects = [o for (o, k) in object_keys if k in existing]
    insert_objects = [o for (o, k) in object_keys if k not in existing]

    _update_many(model, update_objects, keys=keys, using=using)
    _insert_many(model, insert_objects, using=using)
    transaction.commit_unless_managed()
