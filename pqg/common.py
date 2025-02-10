
import dataclasses
import datetime
import json
import time
import typing
import uuid

# Aliases for type hint convenience
OptionalStr = typing.Optional[str]
OptionalInt = typing.Optional[int]
OptionalFloat = typing.Optional[float]
OptionalDateTime = typing.Optional[datetime.datetime]
StringList = typing.Optional[typing.List[str]]
IntegerList = typing.Optional[typing.List[int]]
FloatList = typing.Optional[typing.List[float]]
DateTimeList = typing.Optional[typing.List[datetime.datetime]]

def getUnixTimestamp():
    return int(time.time())


def getUUID():
    return uuid.uuid4().hex


class JSONDateTimeEncoder(json.JSONEncoder):
    """Handle datetime.datetime encoding in json.

    e.g. json.dumps(o, cls=JSONDateTimeEncoder)
    """
    def default(self, o: typing.Any) -> typing.Any:
        if isinstance(o, datetime.datetime):
            # Force use of timezone
            if o.tzinfo is None:
                o = o.replace(tzinfo=datetime.timezone.utc)
            return o.isoformat()
        return super().default(o)

class JSONTruncatedEncoder(json.JSONEncoder):
    """Used for display purposes only.
    This encoder handles dates and truncates long strings, adding ... to the end.
    This encoder should only be used when the structure rather than content
    of the json is of interest. E.g. when rendering with PlantUML's JSON viewer.
    https://plantuml.com/json
    """
    def default(self, o: typing.Any) -> typing.Any:
        if isinstance(o, datetime.datetime):
            # Force use of timezone
            if o.tzinfo is None:
                o = o.replace(tzinfo=datetime.timezone.utc)
            return o.isoformat()
        if isinstance(o, str):
            if len(o) > 20:
                return o[:20] + '...'
            return o
        return super().default(o)


class IsDataclass(typing.Protocol):
    # Used to assist with typehints to ascertain an instance is a dataclass
    __dataclass_fields__: typing.ClassVar[typing.Dict[str, typing.Any]]


def fieldUnion(cls:IsDataclass) -> typing.Set[dataclasses.Field]:
    """Rec"""
    fields = set()
    for field in dataclasses.fields(cls):
        if dataclasses.is_dataclass(field.type):
            fields |= fieldUnion(field.type)
        else:
            fields.add(field)
    return fields


def simpleFields(cls:IsDataclass) ->typing.List[dataclasses.Field]:
    fields = []
    for field in dataclasses.fields(cls):
        if not dataclasses.is_dataclass(field.type):
            fields.append(field)
    return fields


def fieldToSQLCreate(f:dataclasses.Field, primary_key_field:str="pid") -> str:
    types = {
        str: "VARCHAR",
        int: "INTEGER",
        float: "DOUBLE",
        bool: "BOOLEAN",
        datetime.datetime: "TIMESTAMPTZ",
        OptionalStr: "VARCHAR",
        OptionalInt: "INTEGER",
        OptionalFloat: "DOUBLE",
        OptionalDateTime: "TIMESTAMPTZ",
        StringList: "VARCHAR[]",
        IntegerList: "INTEGER[]",
        FloatList: "DOUBLE[]",
        DateTimeList: "TIMESTAMPTZ[]",
    }
    v = f"{f.name} {types.get(f.type, 'JSON')}"
    if f.name == primary_key_field:
        return f"{v} PRIMARY KEY"
    if f.default is not dataclasses.MISSING:
        _d = ""
        if f.default is None:
            _d = "DEFAULT NULL"
        v = f"{v} {_d}"
    return v


def fieldsToSQLCreate(fields:typing.Set[dataclasses.Field], primary_key_field:str) -> str:
    _sql = []
    for field in fields:
        _sql.append(fieldToSQLCreate(field, primary_key_field))
    return ",\n".join(_sql)


def fieldsToSQLSelect(fields:typing.Set[dataclasses.Field]) -> str:
    _sql = []
    for field in fields:
        if not dataclasses.is_dataclass(field.type):
            _sql.append(field.name)
    return ", ".join(_sql)
