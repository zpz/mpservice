import pytest


def test_deprecated():

    with pytest.warns(DeprecationWarning):

        assert True

    with pytest.warns(DeprecationWarning):

        assert True
