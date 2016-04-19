import cc_dynamodb

import mock
from moto import mock_dynamodb2
import pytest


@mock_dynamodb2
def test_update_table_should_raise_if_table_doesnt_exist(fake_config):
    with pytest.raises(cc_dynamodb.UnknownTableException):
        cc_dynamodb.update_table('change_in_condition')


@mock_dynamodb2
def test_update_table_should_not_update_if_same_throughput(fake_config):
    table = cc_dynamodb.create_table('change_in_condition')

    original_metadata = table.describe()
    # Moto does not support GlobalSecondaryIndexes
    original_metadata['Table'].update({
        'GlobalSecondaryIndexes': [
            {'IndexSizeBytes': 111,
             'IndexName': 'SavedInRDB',
             'Projection': {'ProjectionType': 'ALL'},
             'ProvisionedThroughput': {
                 'WriteCapacityUnits': 15,
                 'ReadCapacityUnits': 15,
             },
             'IndexStatus': 'ACTIVE',
             'KeySchema': [
                 {'KeyType': 'HASH', 'AttributeName': 'saved_in_rdb'},
                 {'KeyType': 'RANGE', 'AttributeName': 'time'}],
            'ItemCount': 0}]
    })

    patcher = mock.patch('cc_dynamodb.table.Table.describe')
    mock_metadata = patcher.start()

    patcher = mock.patch('cc_dynamodb.table.Table.update_global_secondary_index')
    mock_update_gsi = patcher.start()

    patcher = mock.patch('cc_dynamodb.table.Table.create_global_secondary_index')
    mock_create_gsi = patcher.start()

    patcher = mock.patch('cc_dynamodb.table.Table.delete_global_secondary_index')
    mock_delete_gsi = patcher.start()

    mock_metadata.return_value = original_metadata
    cc_dynamodb.update_table('change_in_condition')

    mock_metadata.stop()
    mock_update_gsi.stop()
    mock_create_gsi.stop()
    mock_delete_gsi.stop()

    assert not mock_update_gsi.called


@mock_dynamodb2
def test_update_table_should_create_update_delete_gsi(fake_config):
    # NOTE: this test does too many things. Could be broken up.
    # ... but it's nice to cover a case that calls out to all index changes.
    table = cc_dynamodb.create_table('change_in_condition')

    original_metadata = table.describe()
    # Moto does not support GlobalSecondaryIndexes
    original_metadata['Table'].update({
        'GlobalSecondaryIndexes': [
            {'IndexSizeBytes': 111,
             'IndexName': 'SavedInRDB',
             'Projection': {'ProjectionType': 'ALL'},
             'ProvisionedThroughput': {
                 'WriteCapacityUnits': 10,
                 'ReadCapacityUnits': 10,
             },
             'IndexStatus': 'ACTIVE',
             'KeySchema': [
                 {'KeyType': 'HASH', 'AttributeName': 'saved_in_rdb'},
                 {'KeyType': 'RANGE', 'AttributeName': 'time'}],
            'ItemCount': 0},
            {'IndexSizeBytes': 50,
             'IndexName': 'SomeUpstreamIndex',
             'Projection': {'ProjectionType': 'ALL'},
             'ProvisionedThroughput': {
                 'WriteCapacityUnits': 10,
                 'ReadCapacityUnits': 10,
             },
             'IndexStatus': 'ACTIVE',
             'KeySchema': [
                 {'KeyType': 'HASH', 'AttributeName': 'session_id'},
                 {'KeyType': 'RANGE', 'AttributeName': 'time'}],
            'ItemCount': 0}]
    })
    original_config = cc_dynamodb.get_config()
    patcher = mock.patch('cc_dynamodb.get_config')
    mock_config = patcher.start()

    original_config.hcl['resource']['aws_dynamodb_table']['change_in_condition']['global_secondary_index'] = [
        {
            'hash_key': 'rdb_id',
            'range_key': 'session_id',
            'projection_type': 'ALL',
            'name': 'RdbID',
            'read': 15,
            'write': 15,
        },
        {
            'hash_key': 'saved_in_rdb',
            'range_key': 'time',
            'projection_type': 'ALL',
            'name': 'SavedInRDB',
        },
    ]

    original_config.hcl['resource']['aws_dynamodb_table']['change_in_condition']['attribute'].extend([
        {'name': 'rdb_id', 'type': 'N'},
        {'name': 'session_id', 'type': 'N'},
    ])

    mock_config.return_value = original_config

    patcher = mock.patch('cc_dynamodb.table.Table.describe')
    mock_metadata = patcher.start()

    patcher = mock.patch('cc_dynamodb.table.Table.update_global_secondary_index')
    mock_update_gsi = patcher.start()

    patcher = mock.patch('cc_dynamodb.table.Table.create_global_secondary_index')
    mock_create_gsi = patcher.start()

    patcher = mock.patch('cc_dynamodb.table.Table.delete_global_secondary_index')
    mock_delete_gsi = patcher.start()

    mock_metadata.return_value = original_metadata
    cc_dynamodb.update_table('change_in_condition', throughput={'read': 55, 'write': 44})

    mock_metadata.stop()
    mock_update_gsi.stop()
    mock_create_gsi.stop()
    mock_delete_gsi.stop()
    mock_config.stop()

    mock_update_gsi.assert_called_with(global_indexes={'SavedInRDB': {'read': 5, 'write': 5}})
    assert mock_create_gsi.called
    assert mock_create_gsi.call_args[0][0].name == 'RdbID'
    mock_delete_gsi.assert_called_with('SomeUpstreamIndex')

    table = cc_dynamodb.get_table('change_in_condition')
    # Ensure the throughput has been updated
    assert table.throughput == {'read': 5, 'write': 5}


@mock_dynamodb2
def test_update_table_should_update_gsi_if_no_throughput_defined(fake_config):
    table = cc_dynamodb.create_table('change_in_condition')

    original_metadata = table.describe()
    # Moto does not support GlobalSecondaryIndexes
    original_metadata['Table'].update({
        'GlobalSecondaryIndexes': [
            {'IndexSizeBytes': 111,
             'IndexName': 'SavedInRDB',
             'Projection': {'ProjectionType': 'ALL'},
             'ProvisionedThroughput': {
                 'WriteCapacityUnits': 15,
                 'ReadCapacityUnits': 15,
             },
             'IndexStatus': 'ACTIVE',
             'KeySchema': [
                 {'KeyType': 'HASH', 'AttributeName': 'saved_in_rdb'},
                 {'KeyType': 'RANGE', 'AttributeName': 'time'}],
            'ItemCount': 0}]
    })

    original_config = cc_dynamodb.get_config()
    patcher = mock.patch('cc_dynamodb.get_config')
    mock_config = patcher.start()
    del original_config.hcl['resource']['aws_dynamodb_table']['change_in_condition']['global_secondary_index'][0]['read']
    del original_config.hcl['resource']['aws_dynamodb_table']['change_in_condition']['global_secondary_index'][0]['write']

    mock_config.return_value = original_config

    patcher = mock.patch('cc_dynamodb.table.Table.describe')
    mock_metadata = patcher.start()

    patcher = mock.patch('cc_dynamodb.table.Table.update_global_secondary_index')
    mock_update_gsi = patcher.start()

    patcher = mock.patch('cc_dynamodb.table.Table.create_global_secondary_index')
    mock_create_gsi = patcher.start()

    patcher = mock.patch('cc_dynamodb.table.Table.delete_global_secondary_index')
    mock_delete_gsi = patcher.start()

    mock_metadata.return_value = original_metadata
    cc_dynamodb.update_table('change_in_condition')

    mock_metadata.stop()
    mock_update_gsi.stop()
    mock_create_gsi.stop()
    mock_delete_gsi.stop()
    mock_config.stop()

    assert mock_update_gsi.called
    mock_update_gsi.assert_called_with(global_indexes={'SavedInRDB': {'read': 5, 'write': 5}})
