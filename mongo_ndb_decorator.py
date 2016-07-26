# -*- coding:utf-8 -*-
from datetime import datetime
import ast
import time
import json
import functools
from bson import ObjectId
from mongoengine import Document, Q
from mongoengine.base.datastructures import BaseList
from mongoengine import(
    IntField,
    StringField,
    DateTimeField,
    DictField,
    FloatField,
    SequenceField,
    ListField,
)


def transactional(method):
    # To do
    # NDB transaction
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        return method(self, *args, **kwargs)
    return wrapper


class BaseNDBColumnSet():

    def __eq__(elements, other):
        return elements, "", other

    def __gt__(elements, other):
        return elements, "__gt", other

    def __lt__(elements, other):
        return elements, "__lt", other

    def __ge__(elements, other):
        return elements, "__gte", other

    def __le__(elements, other):
        return elements, "__lte", other

    def __ne__(elements, other):
        return elements, "__ne", other

    def __sub__(elements, other):
        pass

    def __neg__(self):
        return self, "-"

    def __pos__(self):
        return self, "+"

    def IN(elements, other):
        return elements, "__in", other


class IntegerProperty(BaseNDBColumnSet, IntField):
    pass


class StringProperty(BaseNDBColumnSet, StringField):
    def __init__(self, regex=None, max_length=None, min_length=None, **kw):
        self.regex = re.compile(regex) if regex else None
        self.max_length = max_length
        self.min_length = min_length
        super(StringField, self).__init__(**kw)


class JsonProperty(BaseNDBColumnSet, ListField):

    def __get__(self, instance, owner):
        value = super(ListField, self).__get__(instance, owner)
        if not value:
            return value
        if type(value) is BaseList:
            value = value[0]
        return value

    def __set__(self, instance, value):
        if value and type(value) == dict:
            value = [dict((str(k), v) for (k, v) in value.iteritems())]

        if value and type(value) == list:
            new_container = []
            for item in value:
                if type(item) != dict:
                    continue
                convert_d = dict((str(k), v) for (k, v) in item.iteritems())
                new_container.append(convert_d)
            value = new_container

        super(ListField, self).__set__(instance, value)

    def validate(self, value):
        """Make sure that a list of valid fields is being used.
        """
        if not isinstance(value, dict) and not isinstance(value, list):
            self.error('Only dictionaries may be used in a DictField')

        super(ListField, self).validate(value)


class DateTimeProperty(BaseNDBColumnSet, DateTimeField):

    def __init__(self, **kw):
        if kw.get('auto_now_add', False) or kw.get('auto_now_add', False):
            super(DateTimeField, self).__init__(default=datetime.now)
            return
        super(DateTimeField, self).__init__(**kw)


class FloatProperty(BaseNDBColumnSet, FloatField):
    pass


class Key():
    def __init__(self, cls, oid, *clauses):
        if type(cls) == str:  # don't need support ClassNameParent
            return None
        return cls.objects(id=oid).first()


class Model(Document):
    meta = {'allow_inheritance': True}
    id = StringField(primary_key=True)

    def __init__(self, **kw):
        self.key = type('AnonymousObject', (object,), {},)()
        setattr(self.key, "id", lambda: str(self.id))
        setattr(self.key, "delete", lambda: self.delete())
        super(Document, self).__init__(**kw)

    @classmethod
    def query(cls, *clauses, **kw):

        def get_key(item):
            for key in cls.__dict__.keys():
                if id(item[0]) == id(getattr(cls, key)):
                    return key

        conditions = {}
        for item in clauses:
            key = get_key(item)
            if not key:
                continue
            conditions[key+item[1]] = item[2]

        datas = cls.objects(**conditions)

        def fetch(limit=len(datas)):
            return datas[:limit]

        def order(*orders):
            order_arr = []
            for item in orders:
                if type(item) != tuple:
                    item = (item, "")
                key = get_key(item)
                if not key:
                    continue
                order_arr.append(item[1]+key)
            order_datas = datas.order_by(*order_arr)
            setattr(order_datas, "fetch", fetch)
            return order_datas

        setattr(datas, "get", datas.first)
        setattr(datas, "fetch", fetch)
        setattr(datas, "iter", fetch)
        setattr(datas, "order", order)
        return datas

    @classmethod
    def get_by_id(cls, id, **kw):
        return cls.objects(id=str(id)).first()

    def put(self):
        if not self.id:
            # self.id = str(ObjectId())
            self.id = str(long(time.time()*1000000))
        self.save()
        return self.key

    def to_dict(self):
        return ast.literal_eval(self.to_json(ensure_ascii=False,
                                             encoding='utf8'))

    @classmethod
    def get_or_insert(cls, key_name, parent=None, app=None, namespace=None,
                      context_options=None, **constructor_args):

        # if constructor_args not in class attributes throw Exception
        datas = cls.objects(**constructor_args)
        if datas:
            return datas.first()
        else:
            instance = cls()
            instance.id = key_name
            for k, v in constructor_args.iteritems():
                setattr(instance, k, v)
            instance.save()
            return instance


def get_multi(objects):
    return objects


def delete_multi(objects):
    # To do
    pass


def transactional(func):
    # To do
    def func_wrapper(path):
        return func(path)

    return func_wrapper


def AND(*clauses):
    # default is 'logic and', and NDB didn't use complex condition
    # To do : add specil symbol to clauses, in query method use Q
    return clauses


class Test(Model):

    gname = StringProperty(indexed=False)
    xname = IntegerProperty()
    question = JsonProperty(indexed=False)
    xtime = DateTimeProperty(auto_now_add=True, indexed=False)


if __name__ == '__main__':
    from mongoengine import connect

    conn = connect("vonvon-test",
                   username="keith",
                   password="vonvon-keith",
                   host="localhost"
                   )

    t = Test()
    t.question = {1: 123}
    print Key(Test, 123), "Key test"
    # print t.put()
    print t.get_or_insert("key", gname="123123").to_json(), "get or insert"
    print Test.query(), "search all"
    print Test.query(Test.gname == 'test').fetch(limit=2), "limit=2"
    print Test.query(AND(Test.gname == 'test', Test.gname == 'test')), "multiple condition"
    print Test.query(Test.gname == 'test').order(-Test.gname, Test.gname).fetch(5), "order"
    t1 = Test.query().get()
    t1.question.iteritems()
    print t1.to_json(), t1.to_dict(), "to json"
    print Test.get_by_id(1467621452778378), "get by id"
