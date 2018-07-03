from boto.dynamodb2 import types



class Dynamizer(types.Dynamizer):

    def encode_item(self, item):
        item_cp = item.copy()
        for key, val in item_cp.iteritems():
            item_cp.update({key: self.encode(val)})
        return item_cp


    def decode_item(self, encoded_item):
        encoded_item_cp = encoded_item.copy()
        # only 1-depth supported.
        # TODO: provide n-depth of dict
        for key, val in encoded_item_cp.iteritems():
            encoded_item_cp.update({key: self.decode(val)})
        return encoded_item_cp