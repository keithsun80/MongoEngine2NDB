# -*- coding:utf-8 -*-

from fields import *
from queryset import *

from document import Model


def add_str(val):
    return val+"xixi"


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
    compute = ComputedProperty(lambda self: add_str(self.gname))
    wtk = StringProperty(indexed=False)
    t_key = KeyProperty(kind=KeyPropertyTest)

    @classmethod
    @transactional
    def increase(cls, poll_id, choice):
        print "decorator function", poll_id, choice


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


def test_search_function():
    print "search all====", Test.query()
    for poll_key in Test.query():
        print poll_key.id()
    print [poll_key.id() for poll_key in Test.query()]

    # result = Test.query(Test.compute == "a")
    # print "search by condition====", result


def test_keyproperty():
    kt = KeyPropertyTest()
    kt.rname = "rname--"
    kt.put()

    t = Test()
    t.gname = "gname"
    t.t_key = kt
    t.put()
    # print t.t_key.id, t.t_key.id(), type(t.t_key), "====k_key"


def test_jsonproperty():
    t = Test()
    d = dict()
    d['haha'] = [1, 2, 3]
    d['xixi'] = [4, 5, 6]
    t.question = d
    t.put()
    print t.question, "======JsonProperty"
    print t.question.haha, "======JsonProperty attr"
    t.question.haha.append(4)
    t.question.xixi.append(5)
    t.put()
    print t.question.haha, "reset JsonProperty"
    print Test.objects().first().to_json()


def test_decorator():
    Test.increase('poll_id', "choice")


if __name__ == '__main__':
    from mongoengine import connect

    conn = connect("vonvon-test",
                   username="keith",
                   password="vonvon-keith",
                   host="localhost"
                   )
    __delete_test__()
    # test_keyproperty()
    # test_search_function()
    test_jsonproperty()
    # test_decorator()
