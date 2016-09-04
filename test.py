# -*- coding:utf-8 -*-

from fields import *
from queryset import *

from document import Model


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
    compute = ComputedProperty(lambda self: self.t_key.id())
    wtk = StringProperty(indexed=False)
    t_key = KeyProperty(kind=KeyPropertyTest)


class ReferenceTest(Model):
    test = KeyProperty(kind=Test)
    rname = StringProperty(indexed=False)
    compute = ComputedProperty(lambda self: self.test.id())


@transactional()
def test(path):
    return path


def __delete_test__():
    for t in Test.objects:
        t.delete()


def test_keyproperty():
    kt = KeyPropertyTest()
    kt.rname = "rname--"
    kt.put()

    t = Test()
    t.t_key = kt
    t.put()

    print t.t_key.id, type(t.t_key), "====k_key"
    print t.compute

if __name__ == '__main__':
    from mongoengine import connect

    conn = connect("vonvon-test",
                   username="keith",
                   password="vonvon-keith",
                   host="localhost"
                   )
    __delete_test__()
    test_keyproperty()
