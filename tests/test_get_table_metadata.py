from moto import mock_dynamodb

import cc_dynamodb


@mock_dynamodb
def test_get_table_metadata_indexes(fake_config):

    metadata = cc_dynamodb._get_table_metadata('change_in_condition')
    assert metadata['indexes'][0].definition() == [
        {
            'AttributeName': 'carelog_id',
            'AttributeType': 'N',
        }, {
            'AttributeName': 'session_id',
            'AttributeType': 'N',
        }
    ]

    assert metadata['indexes'][0].schema() == {
        'KeySchema': [
            {
                'KeyType': 'HASH',
                'AttributeName': 'carelog_id',
            },
            {
                'KeyType': 'RANGE',
                'AttributeName': 'session_id',
            }
        ],
        'IndexName': 'SessionId',
        'Projection': {
            'ProjectionType': 'ALL',
        },
    }


@mock_dynamodb
def test_get_table_metadata_global_indexes(fake_config):

    metadata = cc_dynamodb._get_table_metadata('change_in_condition')

    assert metadata['global_indexes'][0].definition() == [
        {
            'AttributeName': 'saved_in_rdb',
            'AttributeType': 'N'
        }, {
            'AttributeName': 'time',
            'AttributeType': 'N',
        }
    ]

    assert metadata['global_indexes'][0].schema() == {
        'KeySchema': [
            {
                'KeyType': 'HASH',
                'AttributeName': 'saved_in_rdb',
            },
            {
                'KeyType': 'RANGE',
                'AttributeName': 'time',
            }
        ],
        'IndexName': 'SavedInRDB',
        'Projection': {
            'ProjectionType': 'ALL',
        },
        'ProvisionedThroughput': {
            'WriteCapacityUnits': 15,
            'ReadCapacityUnits': 15,
        }
    }


@mock_dynamodb
def test_get_table_metadata_schema(fake_config):

    metadata = cc_dynamodb._get_table_metadata('change_in_condition')

    assert metadata['schema'][0].definition() == {
        'AttributeName': 'carelog_id',
        'AttributeType': 'N',
    }

    assert metadata['schema'][0].schema() == {
        'KeyType': 'HASH',
        'AttributeName': 'carelog_id',
    }
