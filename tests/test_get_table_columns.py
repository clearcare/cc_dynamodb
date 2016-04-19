from moto import mock_dynamodb
import pytest

import cc_dynamodb


@mock_dynamodb
def test_get_dynamodb_table_unknown_table_raises_exception(fake_config):
    with pytest.raises(cc_dynamodb.UnknownTableException):
        cc_dynamodb.get_table('invalid_table')
