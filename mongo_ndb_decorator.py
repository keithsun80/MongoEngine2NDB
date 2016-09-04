# -*- coding:utf-8 -*-
from datetime import datetime
import ast
import copy
import time
import json
import functools
from bson import ObjectId
from easydict import EasyDict
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
    BooleanField,
    BinaryField,
    ReferenceField,
)



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

    def __init__(self, field=None, **kwargs):
        self.field = field
        self._rewrite_data = None
        kwargs.setdefault('default', lambda: [])
        super(ListField, self).__init__(**kwargs)

    def __get__(self, instance, owner):
        value = super(ListField, self).__get__(instance, owner)
        if isinstance(value, BaseList) or isinstance(value, list):
            if value.count('dict') or value.count('list'):
                if self._rewrite_data:
                    value[1] = self._rewrite_data
                return self._form_base_type_decorator(value[1])
        return value

    def __set__(self, instance, value):
        if value and isinstance(value, list):
            if value.count('dict') or value.count('list'):
                super(ListField, self).__set__(instance, value[1])
                return
            new_container = []
            for item in value:
                if not isinstance(item, dict):  # Mark
                    continue
                convert_d = dict((str(k), v) for (k, v) in item.iteritems())
                new_container.append(convert_d)
            value = ["list", new_container]

        if value and isinstance(value, dict):
            value = ["dict", dict((str(k), v) for (k, v) in value.iteritems())]
        super(ListField, self).__set__(instance, value)

    def _form_base_type_decorator(self, value):
        result = self._from_base_type(value)
        if isinstance(result, EasyDict): # need to improve
            self._rewrite_data = result
        return result

    def _from_base_type(self, value):
        """I didn't find a good way to support this function T. T
           I tried use this way.
            value[1] = _from_base_type
           it's doesn't working
        """
        return value

    def validate(self, value):
        """Make sure that a list of valid fields is being used.
        """
        if not isinstance(value, dict) and not isinstance(value, list):
            self.error('Only support dict or list')

        super(ListField, self).validate(value)


class DateTimeProperty(BaseNDBColumnSet, DateTimeField):

    def __init__(self, **kw):
        if kw.get('auto_now_add', False) or kw.get('auto_now_add', False):
            super(DateTimeField, self).__init__(default=datetime.now)
            return
        super(DateTimeField, self).__init__(**kw)


class FloatProperty(BaseNDBColumnSet, FloatField):
    pass


class KeyProperty(BaseNDBColumnSet, ReferenceField):
    def __init__(self, **kw):
        kind = kw.get('kind', False)
        self.dbref = False
        self.document_type_obj = kind
        self.reverse_delete_rule = 0
        if kind:
            super(ReferenceField, self).__init__()


class BooleanProperty(BaseNDBColumnSet, BooleanField):
    pass


class BlobProperty(BaseNDBColumnSet, BinaryField):
    pass


class ComputedProperty(object):
    def __init__(self, func, name=None, indexed=None, repeated=None,
                 verbose_name=None):

        self._func = func

    def __get__(self, instance, owner):
        return self._func(instance)

    def __set__(self, instance, value):
        raise Exception("Cannot assign to a ComputedProperty")


def Key(cls, oid, *clauses):
    if isinstance(cls, str):
        return None
    return cls.objects(id=oid).first()


class Query():
    pass


class query():
    Query = Query()


class Cursor():
    def __init__(self, **kw):
        self.cursor = kw.get('urlsfafe')

    def urlsafe(self):
        return self.cursor

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
            key = item[0].name
            if not key:
                continue
            conditions[key+item[1]] = item[2]

        datas = cls.objects(**conditions)

        def fetch(limit=len(datas)):
            return datas[:limit]

        def fetch_page(count, keys_only=False, start_cursor=None):
            cursor = start_cursor.cursor if start_cursor else 0
            more = len(datas) < cursor+count
            result = datas[cursor: cursor+count]
            if keys_only:
                result = [obj.key for obj in result]
            return result, Cursor(urlsfafe=cursor+count+1), more

        def order(*orders):
            if not datas:
                return datas
            order_arr = []
            for item in orders:
                if not isinstance(item, tuple):
                    item = (item, "")
                key = item[0].name
                if not key:
                    continue
                order_arr.append(item[1]+key)
            print order_arr, "order by arr ========"
            order_datas = datas.order_by(*order_arr)
            setattr(order_datas, "fetch", fetch)
            setattr(order_datas, "fetch_page", fetch_page)
            return order_datas

        setattr(datas, "get", datas.first)
        setattr(datas, "fetch", fetch)
        setattr(datas, "iter", fetch)
        setattr(datas, "fetch_page", fetch_page)
        setattr(datas, "order", order)
        return datas

    @classmethod
    def get_by_id(cls, id, **kw):
        return cls.objects(id=str(id)).first()

    def put(self):
        if not self.id:
            # self.id = str(ObjectId())
            self.id = str(long(time.time()*1000000))
            self._created = True
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
    def decorator(func):
        if not hasattr(func, '__call__'):
            return args[0](func)

        @functools.wraps(func)
        def wrapper(*args, **kw):
            return func(*args, **kw)
        return wrapper

    return decorator


def AND(*clauses):
    # default is 'logic and', and NDB didn't use complex condition
    # To do : add specil symbol to clauses, in query method use Q
    return clauses


class EasyDictProperty(JsonProperty):
    def _from_base_type(self, value):
        return EasyDict(value)


class KeyPropertyTest(Model):
    rname = StringProperty(indexed=False)


class Test(Model):
    gname = StringProperty(indexed=False)
    xname = IntegerProperty()
    question = JsonProperty(indexed=False)
    option = JsonProperty(indexed=False)
    properties = EasyDictProperty()
    xtime = DateTimeProperty(auto_now_add=True, indexed=False)
    compute = ComputedProperty(lambda self: self.t_key)
    wtk = StringProperty(indexed=False)
    t_key = KeyProperty(kind=KeyPropertyTest)

class ReferenceTest(Model):
    test = KeyProperty(kind=Test)
    rname = StringProperty(indexed=False)



@transactional()
def test(path):
    return path


def __delete_test__():
    for t in Test.objects:
        t.delete()


def __api_test__history():
    reference = ReferenceTest()
    reference.test = t
    print reference.__class__._fields.get('test').name, "====what ?=====",
    reference.put()

    print query.Query, "query attributes"
    print test(), "wrapper test"

    print t.get_or_insert("key", gname="123123").to_json(), "get or insert"
    print Test.query(), "search all"
    print Test.query(Test.gname == 'test').fetch(limit=2), "limit=2"
    print Test.query(AND(Test.gname == 'test', Test.gname == 'test')), "multiple condition"
    print Test.query(Test.gname == 'test').order(-Test.gname, Test.gname).fetch(5), "order"
    t1 = Test.query().get()
    print t1.to_json(), t1.to_dict(), "to json"
    print Test.get_by_id(1467621452778378), "get by id"

def __api_test__json():
    t = Test()
    t.question = [{"question--list": 123}, {"question-list2": 123}]
    t.option = {"option-dict": 123}
    t.properties = {"properties": None, "image_url": None}
    t.gname = "gname?"
    t.put()
    print Key(Test, 123), "Key test"
    print type(t.properties), t.properties
    t.properties.image_url = "the path"
    print t.properties
    print "-------------------"
    print t.question, t.option
    print "-------------------"
    for x in Test.objects:
        print x.question, "question======"
        print x.option, "option======="


if __name__ == '__main__':
    from mongoengine import connect

    conn = connect("vonvon-test",
                   username="keith",
                   password="vonvon-keith",
                   host="localhost"
                   )
    __delete_test__()
    t = Test()
    t.properties = {"properties": None, "image_url": 123}
    t.gname = "gname?"
    t.put()
    print "wrapper ------", test("path")
    print Test.query(Test.wtk == 'gname?1').fetch()
    print t.compute, "ComputedProperty======"
    print "-----------------"
    print type(t.properties), t.properties.image_url
    t.properties.image_url = "the path"
    # import ipdb; ipdb.set_trace()
    print t.properties
