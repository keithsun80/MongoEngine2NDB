# -*- coding:utf-8 -*-

import functools


class Query():
    pass


class query():
    Query = Query()


class Cursor():
    def __init__(self, **kw):
        self.cursor = kw.get('urlsfafe')

    def urlsafe(self):
        return self.cursor


def Key(cls, oid, *clauses):
    if isinstance(cls, str):
        return None
    return cls.objects(id=str(oid)).first()


def get_multi(objects):
    return objects


def put_multi(objects):
    keys = []
    for obj in objects:
        obj.put()
        keys.append(obj.key)
    return keys


def delete_multi(objects):
    # To do
    pass


def transactional(*args, **kwargs):
    # to do
    # NDB transactional
    if args:
        func = args[0]

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper

    def decorator(func, *args, **kwargs):
        if not hasattr(func, '__call__'):
            return args[0](func)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper

    return decorator


def AND(*clauses):
    # default is 'logic and', and NDB didn't use complex condition
    # To do : add specil symbol to clauses, in query method use Q
    return clauses
