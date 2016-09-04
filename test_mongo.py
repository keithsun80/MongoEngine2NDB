# -*- coding:utf-8 -*-
from datetime import datetime
import ast
import copy
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
    BooleanField,
    BinaryField,
    ReferenceField,
)


class Test(Document):
    gname = StringField()


class ReferenceTest(Document):
    test = ReferenceField(Test)


if __name__ == '__main__':
    from mongoengine import connect

    conn = connect("vonvon-test",
                   username="keith",
                   password="vonvon-keith",
                   host="localhost"
                   )
    t = Test()
    t.gname = "gname?"
    t.save()

    reference = ReferenceTest()
    reference.test = t
    reference.save()
    print reference.__class__._fields.get('test').name, "what ?====="
    print reference.to_json()
