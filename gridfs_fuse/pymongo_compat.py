"""Internal helper to make pymongo-2.8 look like pymongo-3.0

This is for applications which did not upgrade to pymongo-3.0
"""
import pymongo


class CompatCollection(object):
    def __init__(self, database, collection_name):
        self.coll = database[collection_name]

    def create_index(self, index, unique):
        return self.coll.create_index(index, unique=True)

    def update_one(self, query, update):
        return self.coll.update(query, update, multi=False)

    def delete_one(self, query):
        return self.coll.remove(query, multi=False)

    def find_one(self, query):
        return self.coll.find_one(query)

    def insert_one(self, doc):
        return self.coll.insert(doc)

    def find_one_and_update(self, query, update):
        return self.coll.find_and_modify(query, update)


def compat_collection(database, collection_name):
    if pymongo.version_tuple[0] < 3:
        return CompatCollection(database, collection_name)
    return getattr(database, collection_name)
