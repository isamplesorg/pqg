"""
Test cases for type mapping python to property graph.
"""
import decimal
import typing
import linkml_runtime.utils.metamodelcore
import pytest

from isamples import *
import isamples.generated
from pqg.common import *

typeish_cases = (
    # testvalue, expected type, is a list, is a dataclass
    (typing.Union[dict, typing.ForwardRef('GeospatialCoordLocation'), None], None, False, True),
    (list, None, True, False),
    (None, None, False, False),
    (str, str, False, False),
    (bool, bool, False, False),
    (int, int, False, False),
    (float, float, False, False),
    (decimal.Decimal, decimal.Decimal, False, False),
    (typing.Union[bool, linkml_runtime.utils.metamodelcore.Bool, None], bool, False, False),
    (typing.Union[str, linkml_runtime.utils.metamodelcore.URIorCURIE, typing.List[typing.Union[str, linkml_runtime.utils.metamodelcore.URIorCURIE]], None], str, True, False),
    (typing.Union[dict, isamples.generated.Agent, typing.List[typing.Union[dict, isamples.generated.Agent]], None], None, True, True),
)

@pytest.mark.parametrize("given,expected,ignore1,ignore2", typeish_cases)
def test_typeish(given:type, expected:type, ignore1:bool, ignore2:bool):
    res = typeish(given)
    assert res == expected


@pytest.mark.parametrize("given,ignore1,expected,ignore2", typeish_cases)
def test_listish(given:type, ignore1: bool, expected:type, ignore2:bool):
    res = listish(given)
    assert res == expected


@pytest.mark.parametrize("given,ignore1,ignore2,expected", typeish_cases)
def test_dataclassish(given:type, ignore1: bool, ignore2:bool, expected:type):
    res = dataclassish(given)
    assert res == expected


types_cases = (
    (typing.Union[bool, linkml_runtime.utils.metamodelcore.Bool, None], 'BOOLEAN',),
    (typing.Union[str, linkml_runtime.utils.metamodelcore.URIorCURIE, typing.List[typing.Union[str, linkml_runtime.utils.metamodelcore.URIorCURIE]], None], 'VARCHAR[]'),
    (typing.Union[dict, typing.ForwardRef('GeospatialCoordLocation'), None], None),
    (typing.Union[dict, isamples.generated.Agent, typing.List[typing.Union[dict, isamples.generated.Agent]], None], None),
    (typing.Optional[typing.List[str]], 'VARCHAR[]',),
    (typing.List[int], 'INTEGER[]',),
    (typing.List[str], 'VARCHAR[]',),
    (typing.Optional[str], 'VARCHAR',),
    (str, 'VARCHAR',),
    (bool, 'BOOLEAN',),
    (int, 'INTEGER',),
    (float, 'DOUBLE',),
    (decimal.Decimal, 'DOUBLE',),
    (list, None),
    (None, None),
)

@pytest.mark.parametrize("given, expected", types_cases)
def test_type_mapping(given:dataclasses.field, expected:str):
    result = typeToSQL(given)
    assert result == expected


