import cc_dynamodb

from moto import mock_dynamodb2
import pytest


@mock_dynamodb2
def test_update_table_should_raise_if_table_doesnt_exist(fake_config):
    with pytest.raises(cc_dynamodb.UnknownTableException):
        cc_dynamodb.update_table('change_in_condition')
