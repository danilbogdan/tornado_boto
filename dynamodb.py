import botocore.session

from tornado import gen
from tornado.httpclient import HTTPError
from tornado_botocore import Botocore
from boto.dynamodb2.types import Dynamizer
from boto.dynamodb2.items import Item
from boto.dynamodb2.table import Table


class DynamoDB(object):
    SCHEMA          = [{'AttributeName': 'Id',
                        'KeyType': 'HASH'}]
    ATTRIBUTE_DEFN = [{'AttributeName': 'Id',
                        'AttributeType': 'S',}]
    PROVISIONEDTHROUGHPUT = {'ReadCapacityUnits': 5,
                             'WriteCapacityUnits': 5}

    def __init__(self, access_key='', secret_key='', session=None,
                 region_name='us-west-2', connect_timeout=None, request_timeout=None, endpoint_url=None):
        self.region_name = region_name
        self.connect_timeout = connect_timeout
        self.request_timeout = request_timeout
        self.endpoint_url = endpoint_url
        self.session = session or botocore.session.get_session()
        if not self.session.get_credentials():
            self.session.set_credentials(access_key, secret_key)

    @gen.coroutine
    def get(self, table_name, key):
        encoded_key = self._encode_item(key)
        dynamodb_get_item = self._create_dynamodb_task('GetItem')
        encoded_item = yield self._run_dynamodb_task(
            dynamodb_get_item.call,
            TableName=table_name,
            Key=encoded_key,
            ConsistentRead=False
        )
        raise gen.Return(self._decode_item(encoded_item))

    @gen.coroutine
    def put(self, table_name, item, overwrite=False):
        encoded_item = self._encode_item(item)
        dynamodb_put_item = self._create_dynamodb_task('PutItem')
        kwargs = dict(
                    TableName=table_name,
                    Item=encoded_item)
        # TODO: check why HTTPClient does not rise exception when overwrite=False
        if not overwrite:
            kwargs.update(dict(Expected=self._get_expected(item)))
        try:
            yield self._run_dynamodb_task(dynamodb_put_item.call, **kwargs)
        except HTTPError:
            raise Exception('DuplicateKeyError')


    @gen.coroutine
    def delete(self, table_name, item):
        encoded_item = self._encode_item(item)
        dynamodb_delete_item = self._create_dynamodb_task('DeleteItem')
        yield self._run_dynamodb_task(
            dynamodb_delete_item.call,
            TableName=table_name,
            Key=encoded_item
        )
        # optionally it is possyble to get some params from key before deleting

    @gen.coroutine
    def _create_table(self, **kwargs):
        dynamodb_create_table = self._create_dynamodb_task('CreateTable')
        table_name = kwargs.get('TableName', 'default')
        yield gen.Task(
            dynamodb_create_table.call,
            AttributeDefinitions=self.ATTRIBUTE_DEFN,
            KeySchema=self.SCHEMA,
            ProvisionedThroughput=self.PROVISIONEDTHROUGHPUT,
            TableName=table_name
        )

    def _create_dynamodb_task(self, task):
        return Botocore(
            service='dynamodb',
            operation=task,
            region_name=self.region_name,
            endpoint_url=self.endpoint_url,
            session=self.session,
            connect_timeout=self.connect_timeout,
            request_timeout=self.request_timeout
        )

    @gen.coroutine
    def _run_dynamodb_task(self, task, **kwargs):
        try:
            result = yield gen.Task(task, **kwargs)
        except Exception, e: # table does not exist
            yield self._create_table(**kwargs)
            result = yield gen.Task(task, **kwargs)
        raise gen.Return(result.get('Item'))


    def _encode_item(self, item):
        item_cp = item.copy()
        for key, val in item_cp.iteritems():
            item_cp.update({key: Dynamizer().encode(val)})
        return item_cp


    def _decode_item(self, encoded_item):
        encoded_item_cp = encoded_item.copy()
        # only 1-depth supported.
        # TODO: provide n-depth of dict
        for key, val in encoded_item_cp.iteritems():
            encoded_item_cp.update({key: Dynamizer().decode(val)})
        return encoded_item_cp

    def _get_expected(self, item):
        return Item(Table(''), item).build_expects()


