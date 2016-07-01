# -*- coding:utf-8 -*-
from datetime import datetime
import time
from bson import ObjectId
from mongoengine import Document, Q
from mongoengine import(
    IntField,
    StringField,
    DateTimeField,
    DictField,
    FloatField,
    SequenceField,
    ListField,
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


class JsonProperty(BaseNDBColumnSet, DictField):

    def __get__(self, instance, owner):
        # PS: NDB code has some integer key, if it must be int covert it
        #   if not owner and type(owner) == dict:
        #       owner = dict((str(k), v) for (k, v) in owner.iteritems())
        super(DictField, self).__get__(instance, owner)

    def __set__(self, instance, value):
        if value and type(value) == dict:
            value = dict((str(k), v) for (k, v) in value.iteritems())
        super(DictField, self).__set__(instance, value)

    def validate(self, value):
        """Make sure that a list of valid fields is being used.
        """
        if not isinstance(value, dict):
            self.error('Only dictionaries may be used in a DictField')

        super(DictField, self).validate(value)


class DateTimeProperty(BaseNDBColumnSet, DateTimeField):

    def __init__(self, **kw):
        if kw.get('auto_now_add', False) or kw.get('auto_now_add', False):
            super(DateTimeField, self).__init__(default=datetime.now)
            return
        super(DateTimeField, self).__init__(**kw)


class FloatProperty(BaseNDBColumnSet, FloatField):
    pass


class Key():
    def __init__(cls, oid):
        return cls.objects.get(id=oid).first()


class Model(Document):
    meta = {'allow_inheritance': True}
    id = StringField(primary_key=True)

    def __init__(self, **kw):
        self.key = type('AnonymousObject', (object,), {},)()
        setattr(self.key, "id", lambda: str(self.id))
        setattr(self.key, "delete", lambda: self.delete())
        super(Document, self).__init__(**kw)

    @classmethod
    def query(cls, *clauses):
        conditions = {}
        for item in clauses:
            for key in cls.__dict__.keys():
                if id(item[0]) == id(getattr(cls, key)):
                    conditions[key+item[1]] = item[2]

        datas = cls.objects(**conditions)

        def fetch(limit=len(datas)):
            return datas[:limit]

        setattr(datas, "get", datas.first)
        setattr(datas, "fetch", fetch)
        setattr(datas, "iter", fetch)
        return datas

    @classmethod
    def get_by_id(cls, id):
        return cls.objects(id=id).first()

    def put(self):
        if not self.id:
            self.id = str(ObjectId())
        self.save()
        return self


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

    conn = connect("test",
                   username="test",
                   password="test-keith",
                   host="localhost"
                   )

    t = Test()
    t.question = {1: 123}
    print t.put()
    print Test.query()
    print Test.query(Test.gname == 'test').fetch(limit=2)
    print Test.query(AND(Test.gname == 'test', Test.gname == 'test'))
    t1 = Test.query().get()
    print t1.to_json()
