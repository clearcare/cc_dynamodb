import copy
import os
import time
import hcl

from boto import dynamodb2
from boto.dynamodb2 import fields  # AllIndex, GlobalAllIndex, HashKey, RangeKey
from boto.dynamodb2 import table
from boto.exception import JSONResponseError
from bunch import Bunch

from .log import create_logger


logger = create_logger()
UPDATE_INDEX_RETRIES = 60

# Cache to avoid parsing YAML file repeatedly.
_cached_config = None


def set_config(dynamodb_tf, namespace=None, aws_access_key_id=False, aws_secret_access_key=False,
               host=None, port=None, is_secure=None):
    global _cached_config

    with open(dynamodb_tf) as hcl_file:
        terraform_config = hcl.load(hcl_file)

    _cached_config = Bunch({
        'hcl': terraform_config,
        'namespace': namespace or os.environ.get('CC_DYNAMODB_NAMESPACE'),
        'aws_access_key_id': aws_access_key_id or os.environ.get('CC_DYNAMODB_ACCESS_KEY_ID', False),
        'aws_secret_access_key': aws_secret_access_key or os.environ.get('CC_DYNAMODB_SECRET_ACCESS_KEY', False),
        'host': host or os.environ.get('CC_DYNAMODB_HOST'),
        'port': port or os.environ.get('CC_DYNAMODB_PORT'),
        'is_secure': is_secure or os.environ.get('CC_DYNAMODB_IS_SECURE'),
    })

    if not _cached_config.namespace:
        msg = 'Missing namespace kwarg OR environment variable CC_DYNAMODB_NAMESPACE'
        logger.error('ConfigurationError: ' + msg)
        raise ConfigurationError(msg)
    if _cached_config.aws_access_key_id is False:
        msg = 'Missing aws_access_key_id kwarg OR environment variable CC_DYNAMODB_ACCESS_KEY_ID'
        logger.error('ConfigurationError: ' + msg)
        raise ConfigurationError(msg)
    if _cached_config.aws_secret_access_key is False:
        msg = 'Missing aws_secret_access_key kwarg OR environment variable CC_DYNAMODB_SECRET_ACCESS_KEY'
        logger.error('ConfigurationError: ' + msg)
        raise ConfigurationError(msg)
    if _cached_config.port:
        try:
            _cached_config.port = int(_cached_config.port)
        except ValueError:
            msg = ('Integer value expected for port '
                   'OR environment variable CC_DYNAMODB_PORT. Got %s' % _cached_config.port)
            logger.error('ConfigurationError: ' + msg)
            raise ConfigurationError(msg)

    logger.info('cc_dynamodb.set_config', extra=dict(status='config loaded'))


def get_config(**kwargs):
    global _cached_config

    if not _cached_config:
        set_config(**kwargs)

    return Bunch(copy.deepcopy(_cached_config.toDict()))


class ConfigurationError(Exception):
    pass


def translate_projection_key(projection_key, is_global=False):
    return '{}{}Index'.format('Global' if is_global else '', projection_key.title())


def translate_field_type(field_type):
    return ''.join(c.title() for c in field_type.split('_'))


def translate_attributes(attributes):
    return {attribute['name']: attribute['type'] for attribute in attributes}


def _build_attribute(field_name, field_type, attributes):
    key_type = getattr(fields, translate_field_type(field_type))
    return key_type(field_name, **{'data_type': attributes[field_name]})


def _build_schema(key_config, attributes):
    hash_key = key_config['hash_key']
    schema = [
        _build_attribute(hash_key, 'hash_key', attributes)
    ]

    range_key = key_config.get('range_key')
    if range_key:
        schema.append(_build_attribute(range_key, 'range_key', attributes))
    return schema


def _build_index_kwargs(index_details, attributes):
    hash_key = index_details['hash_key']

    kwargs = {
        'parts': [
            _build_attribute(hash_key, 'hash_key', attributes),
        ],
    }

    range_key = index_details.get('range_key')
    if range_key:
        kwargs['parts'].append(_build_attribute(range_key, 'range_key', attributes))

    return kwargs


def _build_index(index_details, attributes, is_global=False):
    index_type = getattr(
        fields,
        translate_projection_key(index_details['projection_type'], is_global=is_global)
    )
    kwargs = _build_index_kwargs(index_details, attributes)
    return index_type(index_details['name'], **kwargs)


def _get_table_metadata(table_name):

    try:
        table = get_config().hcl['resource']['aws_dynamodb_table'][table_name]
    except KeyError:
        logger.exception('cc_dynamodb.UnknownTable', extra=dict(
            table_name=table_name,
            table=table_name,
            DTM_EVENT='cc_dynamodb.UnknownTable'),
        )
        raise UnknownTableException('Unknown table: %s' % table_name)

    attributes = translate_attributes(table['attribute'])

    metadata = {
        'schema': _build_schema(table, attributes),
    }

    global_secondary_index = table.get('global_secondary_index')
    if global_secondary_index is not None:
        if isinstance(global_secondary_index, list):
            metadata['global_indexes'] = [
                _build_index(i, attributes, is_global=True) for i in global_secondary_index
            ]
        else:
            metadata['global_indexes'] = _build_index(
                global_secondary_index, attributes, is_global=True),

    local_secondary_index = table.get('local_secondary_index')
    if local_secondary_index is not None:
        metadata['indexes'] = _build_index(local_secondary_index, attributes),

    return metadata


def get_table_name(table_name):
    '''Prefixes the table name for the different environments/settings.'''
    return get_config().namespace + table_name


def get_reverse_table_name(table_name):
    '''Prefixes the table name for the different environments/settings.'''
    prefix_length = len(get_config().namespace)
    return table_name[prefix_length:]


def get_table_index(table_name, index_name):

    for table_name, table_data in get_config().hcl['resource']['aws_dynamodb_table'].iteritems():
        for field, value in table_data.iteritems():
            if field.endswith('index'):
                index_data = table_data[field]
                if index_data['name'] == index_name:
                    attributes = translate_attributes(table_data['attribute'])
                    return _build_index(
                        index_data, attributes, False if field.startswith('local') else True)


def get_connection():
    """Returns a DynamoDBConnection even if credentials are invalid."""
    config = get_config()

    if config.host:
        from boto.dynamodb2.layer1 import DynamoDBConnection
        return DynamoDBConnection(
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            host=config.host,                           # Host where DynamoDB Local resides
            port=config.port,                           # DynamoDB Local port (8000 is the default)
            is_secure=config.is_secure or False)        # For DynamoDB Local, disable secure connections

    return dynamodb2.connect_to_region(
        'us-west-2',
        aws_access_key_id=config.aws_access_key_id,
        aws_secret_access_key=config.aws_secret_access_key,
    )


def list_table_names():
    """List known table names from configuration, without namespace."""
    return get_config().hcl['resource']['aws_dynamodb_table'].keys()


def get_table(table_name, connection=None):
    '''Returns a dict with table and preloaded schema, plus columns.

    WARNING: Does not check the table actually exists. Querying against
             a non-existent table raises boto.exception.JSONResponseError

    This function avoids additional lookups when using a table.
    The columns included are only the optional columns you may find in some of the items.
    '''
    return table.Table(
        get_table_name(table_name),
        connection=connection or get_connection(),
        **_get_table_metadata(table_name)
    )


def _get_table_init_data(table_name, connection, throughput):
    init_data = {
        'table_name': get_table_name(table_name),
        'connection': connection or get_connection(),
    }
    if throughput is not False:
        init_data['throughput'] = throughput
    init_data.update(_get_table_metadata(table_name))
    return init_data


def create_table(table_name, connection=None, throughput=False):
    """Create table. Throws an error if table already exists."""
    try:
        db_table = table.Table.create(
            **_get_table_init_data(table_name, connection, throughput)
        )
        logger.info('cc_dynamodb.create_table: %s' % table_name, extra=dict(status='created table'))
        return db_table
    except JSONResponseError as e:
        if e.status == 400 and e.error_code == 'ResourceInUseException':
            logger.warn('Called create_table("%s"), but already exists: %s' %
                        (table_name, e.body))
            raise TableAlreadyExistsException(body=e.body)
        raise e


def _validate_schema(schema, table_metadata):
    """Raise error if primary index (schema) is not the same as upstream"""
    upstream_schema = table_metadata['Table']['KeySchema']
    upstream_schema_attributes = [i['AttributeName'] for i in upstream_schema]
    upstream_attributes = [item for item in table_metadata['Table']['AttributeDefinitions']
                           if item['AttributeName'] in upstream_schema_attributes]

    local_schema = [item.schema() for item in schema]
    local_schema_attributes = [i['AttributeName'] for i in local_schema]
    local_attributes = [item.definition() for item in schema
                        if item.definition()['AttributeName'] in local_schema_attributes]

    if sorted(upstream_schema, key=lambda i: i['AttributeName']) != sorted(local_schema, key=lambda i: i['AttributeName']):
        msg = 'Mismatched schema: %s VS %s' % (upstream_schema, local_schema)
        logger.warn(msg)
        raise UpdateTableException(msg)

    if sorted(upstream_attributes, key=lambda i: i['AttributeName']) != sorted(local_attributes, key=lambda i: i['AttributeName']):
        msg = 'Mismatched attributes: %s VS %s' % (upstream_attributes, local_attributes)
        logger.warn(msg)
        raise UpdateTableException(msg)


def update_table(table_name, connection=None, throughput=None):
    """
    Update existing table.

    Handles updating primary index and global secondary indexes.
    Updates throughput and creates/deletes indexes.

    :param table_name: unprefixed table name
    :param connection: optional dynamodb connection, to avoid creating one
    :param throughput: a dict, e.g. {'read': 10, 'write': 10}
    :return: the updated boto Table
    """
    db_table = table.Table(
        **_get_table_init_data(table_name, connection, throughput)
    )
    local_global_indexes_by_name = dict((index.name, index) for index in db_table.global_indexes)
    try:
        table_metadata = db_table.describe()
    except JSONResponseError as e:
        if e.status == 400 and e.error_code == 'ResourceNotFoundException':
            raise UnknownTableException('Unknown table: %s' % table_name)

    _validate_schema(schema=db_table.schema, table_metadata=table_metadata)

    # Update existing primary index throughput
    db_table.update(throughput=throughput)

    upstream_global_indexes_by_name = dict(
        (index['IndexName'], index) for index in table_metadata['Table'].get('GlobalSecondaryIndexes', []))
    for index_name, index in local_global_indexes_by_name.items():
        if index_name not in upstream_global_indexes_by_name:
            logger.info('Creating GSI %s for %s' % (index_name, table_name))
            for i in range(UPDATE_INDEX_RETRIES + 1):
                try:
                    db_table.create_global_secondary_index(index)
                except JSONResponseError as e:
                    if 'already exists' in str(e.body):
                        break

                    if i < UPDATE_INDEX_RETRIES:
                        time.sleep(1)
                    else:
                        raise
        else:
            throughput = {
                'write': upstream_global_indexes_by_name[index_name]['ProvisionedThroughput']['WriteCapacityUnits'],
                'read': upstream_global_indexes_by_name[index_name]['ProvisionedThroughput']['ReadCapacityUnits'],
            }

            if throughput == index.throughput:
                continue
            # Update throughput
            # TODO: this could be done in a single call with multiple indexes
            db_table.update_global_secondary_index(global_indexes={
                index_name: index.throughput
            })
            logger.info('Updating GSI %s throughput for %s to %s' % (index_name, table_name, index.throughput))

    for index_name in upstream_global_indexes_by_name.keys():
        if index_name not in local_global_indexes_by_name:
            logger.info('Deleting GSI %s for %s' % (index_name, table_name))
            for i in range(UPDATE_INDEX_RETRIES + 1):
                try:
                    db_table.delete_global_secondary_index(index_name)
                except JSONResponseError as e:
                    if 'ResourceNotFoundException' in str(e.body):
                        break

                    if i < UPDATE_INDEX_RETRIES:
                        time.sleep(1)
                    else:
                        raise

    logger.info('cc_dynamodb.update_table: %s' % table_name, extra=dict(status='updated table'))
    return db_table


class UnknownTableException(Exception):
    pass


class TableAlreadyExistsException(Exception):
    def __init__(self, body):
        self.body = body


class UpdateTableException(Exception):
    pass
