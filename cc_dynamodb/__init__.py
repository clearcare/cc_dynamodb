import copy
import os
import time

from boto import dynamodb2
from boto.dynamodb2 import table
from boto.exception import JSONResponseError
from bunch import Bunch
from hcl_translator import dynamodb_translator
from hcl_translator.exceptions import UnknownTableException

from .log import create_logger


logger = create_logger()
UPDATE_INDEX_RETRIES = 60

# Cache to avoid parsing YAML file repeatedly.
_cached_config = None
_dynamodb_table_info = None


def set_config(dynamodb_tf, namespace=None, aws_access_key_id=False, aws_secret_access_key=False,
               host=None, port=None, is_secure=None):
    global _cached_config
    global _dynamodb_table_info

    _dynamodb_table_info = dynamodb_translator(dynamodb_tf, logger)

    _cached_config = Bunch({
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


def get_table_name(table_name):
    '''Prefixes the table name for the different environments/settings.'''
    return get_config().namespace + table_name


def get_reverse_table_name(table_name):
    '''Prefixes the table name for the different environments/settings.'''
    prefix_length = len(get_config().namespace)
    return table_name[prefix_length:]


def get_table_index(table_name, index_name):
    if _dynamodb_table_info is None:
        raise ConfigurationError('set_config must be called')
    return _dynamodb_table_info.get_table_index(table_name, index_name)


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
    if _dynamodb_table_info is None:
        raise ConfigurationError('set_config must be called')
    return _dynamodb_table_info.list_table_names()


def get_table(table_name, connection=None):
    '''Returns a dict with table and preloaded schema, plus columns.

    WARNING: Does not check the table actually exists. Querying against
             a non-existent table raises boto.exception.JSONResponseError

    This function avoids additional lookups when using a table.
    The columns included are only the optional columns you may find in some of the items.
    '''
    if _dynamodb_table_info is None:
        raise ConfigurationError('set_config must be called')
    return table.Table(
        get_table_name(table_name),
        connection=connection or get_connection(),
        **_dynamodb_table_info.get_schema(table_name)
    )


def _get_table_init_data(table_name, connection, throughput):
    if _dynamodb_table_info is None:
        raise ConfigurationError('set_config must be called')
    init_data = {
        'table_name': get_table_name(table_name),
        'connection': connection or get_connection(),
    }
    if throughput is not False:
        init_data['throughput'] = throughput
    init_data.update(_dynamodb_table_info.get_schema(table_name))
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


class TableAlreadyExistsException(Exception):
    def __init__(self, body):
        self.body = body


class UpdateTableException(Exception):
    pass
