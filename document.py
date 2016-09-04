# -*- coding:utf-8 -*-
import ast
import time
import functools
from bson import ObjectId
from mongoengine import Document, Q
from mongoengine import StringField
from queryset import Cursor


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
