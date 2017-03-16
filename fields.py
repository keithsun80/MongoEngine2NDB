# -*- coding:utf-8 -*-

from easydict import EasyDict
from datetime import datetime
from bson import Binary, DBRef, SON
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

        if value:
            for k, v in value.iteritems():
                setattr(value, k, v)
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
        if isinstance(result, EasyDict):  # need to improve
            self._rewrite_data = result
        for key, value in result.iteritems():
            setattr(result, key, value)
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

    def __get__(self, instance, owner):
        """Descriptor to allow lazy dereferencing.
        """
        if instance is None:
            # Document class being used rather than a document object
            return self

        # Get value from document instance if available
        value = instance._data.get(self.name)
        self._auto_dereference = instance._fields[self.name]._auto_dereference
        # Dereference DBRefs Research mark !!!
        if self._auto_dereference and isinstance(value, DBRef):
            if hasattr(value, 'cls'):
                # Dereference using the class type specified in the reference
                cls = get_document(value.cls)
            else:
                cls = self.document_type
            value = cls._get_db().dereference(value)
            if value is not None:
                instance._data[self.name] = cls._from_son(value)

        reference = super(ReferenceField, self).__get__(instance, owner)
        if type(reference.id) == unicode:
            id_val = reference.id
            setattr(reference, "id", lambda: id_val)
        return reference

    def get():
        return self.reference


class BooleanProperty(BaseNDBColumnSet, BooleanField):
    pass


class BlobProperty(BaseNDBColumnSet, BinaryField):
    pass


class ComputedProperty(object):
    """
        I have no idea to implement NDB ComputedProperty use mongoengine
        sns = ndb.ComputedProperty(lambda self: get_sns_from_fb_id(self.sns_id))
                                            ↑  instance
        PollUser.query(PollUser.sns == 'vv')
                         ↑   class

    """
    def __init__(self, func, name=None, indexed=None, repeated=None,
                 verbose_name=None):
        self._func = func

    def __get__(self, instance, owner):
        # if class directly to use ComputedProperty
        if not instance:
            pass
        return self._func(instance)

    def __set__(self, instance, value):
        raise Exception("Cannot assign to a ComputedProperty")
