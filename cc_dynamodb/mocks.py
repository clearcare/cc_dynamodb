from decimal import Decimal
import operator

from boto.dynamodb2 import table as dynamodb2_table
from boto.dynamodb2.types import QUERY_OPERATORS
from mock import patch
import moto.core.models

import cc_dynamodb


__all__ = [
    'mock_query_2',
    'mock_table_with_data',
    'create_table',
]


def create_table(table_name):
    """Create table. Throws an error if table already exists."""
    config = cc_dynamodb.get_config()
    args, kwargs = cc_dynamodb._dynamodb_translator.create_table_args(table_name, config.namespace)
    table = cc_dynamodb.get_connection().create_table(*args, **kwargs)
    # moto sometimes return a dict
    if isinstance(table, dict):
        table = dynamodb2_table.Table(table['Table']['TableName'])
    return table


def mock_table_with_data(table_name, data):
    '''Create a table and populate it with array of items from data.

    Example:

    data = [{'key_1': 'value 1'}, {'key_1': 'value 2'}]
    table = mock_table_with_data('some_table', data)

    len(table.scan())  # Expect 2 results
    '''
    table = create_table(table_name)

    for item_data in data:
        table.put_item(item_data)

    return table


class TableWithQuery2(dynamodb2_table.Table):
    @staticmethod
    def _sorting_function(range_keys):
        def sorter(obj):
            return tuple([obj.get(range_key) for range_key in range_keys])
        return sorter

    @staticmethod
    def _compare_func(a, b, comparison_operator, value_type):
        operation = getattr(operator, comparison_operator.lower())
        if value_type == 'N':
            try:
                a = Decimal(a)
                b = Decimal(b)
            except TypeError:
                pass
        return operation(a, b)

    def _query_2_with_index(self, *args, **kwargs):
        table_name = cc_dynamodb.get_reverse_table_name(self.table_name)
        index = cc_dynamodb.get_table_index(table_name, kwargs.pop('index'))
        valid_keys = [key.schema()['AttributeName'] for key in index.parts if key.schema()['KeyType'] == 'HASH']
        if len(valid_keys) != 1:
            raise ValueError('Need exactly 1 HashKey for table: %s, index: %s' % (table_name, index))

        range_keys = [key.schema()['AttributeName'] for key in index.parts if key.schema()['KeyType'] == 'RANGE']
        valid_keys += range_keys

        # reverse is also not supported by moto
        reverse = kwargs.pop('reverse', False)
        key_conditions = self._build_filters(
            kwargs,
            using=QUERY_OPERATORS
        )
        if set(key_conditions.keys()) - set(valid_keys):
            raise ValueError('Query by %s, only allowed %s' % (', '.join(key_conditions.keys()),
                                                               ', '.join(valid_keys)))
        table = cc_dynamodb.get_table(table_name)
        results = []
        for obj in table.scan():
            is_matching = True
            for column, details in key_conditions.items():
                if hasattr(operator, details['ComparisonOperator'].lower()):
                    value = details['AttributeValueList'][0].values()[0]
                    value_type = details['AttributeValueList'][0].keys()[0]
                    if not self._compare_func(obj[column], value,
                                              details['ComparisonOperator'], value_type):
                        is_matching = False
                else:
                    raise NotImplementedError('Query of type: %s not supported yet' % details)
            if is_matching:
                results.append(obj)

        for obj in sorted(results, key=self._sorting_function(range_keys=range_keys), reverse=reverse):
            yield obj

    def query_2(self, *args, **kwargs):
        """Implement query_2 for custom index.

        moto does not have a working implementation of query_2 if you pass index=

        NOTE/WARNING/CAVEAT: This only works if there is only ONE index for a given name.
        """
        if 'index' in kwargs:
            return self._query_2_with_index(*args, **kwargs)
        return super(TableWithQuery2, self).query_2(*args, **kwargs)

    def query_count(self, *args, **kwargs):
        """Implement query_2 for custom index.

        moto does not have a working implementation of query_2 if you pass index=

        NOTE/WARNING/CAVEAT: This only works if there is only ONE index for a given name.
        """
        if 'index' in kwargs:
            return len(list(self._query_2_with_index(*args, **kwargs)))
        return super(TableWithQuery2, self).query_count(*args, **kwargs)


class MockQuery2(moto.core.models.MockAWS):
    nested_count = 0

    def __init__(self, *args, **kwargs):
        self.patcher = patch('boto.dynamodb2.table.Table')

    def start(self):
        self.table = self.patcher.start()
        self.table.side_effect = TableWithQuery2

    def stop(self):
        self.patcher.stop()


def mock_query_2(func=None):
    """Use this when testing query_2 with secondary index.

    Can be used as a decorator or a context manager (aka via the `with` statement).
    Similar to how you would use `moto`'s `mock_dynamodb2`.

    Example:

        with mock_query_2():
            items = table.query_2(some_column__eq='value', index='SomeColumnIndex')
    """
    if func:
        return MockQuery2()(func)
    else:
        return MockQuery2()
