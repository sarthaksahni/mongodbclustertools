#!/usr/bin/env python
import os
import sys

import pkg_resources
from pkg_resources import DistributionNotFound, VersionConflict

def get_human_size(size,precision=2):
    suffixes=['B','KB','MB','GB','TB']
    suffixIndex = 0
    while size > 1024 and suffixIndex < 4:
        suffixIndex += 1
        size = size/1024.0
    return "%.*f%s"%(precision,size,suffixes[suffixIndex])

class mongodbclustertool(object):

    conn = None

    def listdbs(self):
        if self.conn is None:
            self.conn = pymongo.MongoClient(config.get('MONGO', 'CONNECT_URI'))
        for db in self.conn.list_databases():
            print("=> DB Name:", db['name'], ", Size: ", get_human_size(db['sizeOnDisk'], 2))

    def listcollections(self, db):
        if self.conn is None:
            self.conn = pymongo.MongoClient(config.get('MONGO', 'CONNECT_URI'))
        print("Database: ", db)
        for colls in self.conn[db].list_collection_names():
            print("=> Collection:", colls)

    def findjumbos(self, db, coll):
        if self.conn is None:
            self.conn = pymongo.MongoClient(config.get('MONGO', 'CONNECT_URI'))
        count = 0
        for jumbo in self.conn.config.chunks.find({"ns": db+"."+coll, "jumbo":True}):
            count += 1;
            print(jumbo)
        if count > 0:
            print("You have",count, "jumbos!")
        else:
            print("Great! No jumbos found!")

    def chunkdist(self, db, coll):
        if self.conn is None:
            self.conn = pymongo.MongoClient(config.get('MONGO', 'CONNECT_URI'))
        shards = {}
        totalchunks = 0
        for chunk in self.conn.config.chunks.find({"ns": db+"."+coll}):
            totalchunks += 1
            if chunk['shard'] not in shards:
                shards[chunk['shard']] = {"count": 1, "percent": (1/totalchunks)*100}
            else:
                shards[chunk['shard']]['count'] += 1
                shards[chunk['shard']]['percent'] = float("{0:.2f}".format((shards[chunk['shard']]['count']/totalchunks)*100))
        for shard in shards:
            print("Shard:", shard, "| Chunks:", shards[shard]['count'], "| Distribution:", str(shards[shard]['percent'])+"%")

    def listshards(self):
        if self.conn is None:
            self.conn = pymongo.MongoClient(config.get('MONGO', 'CONNECT_URI'))
        for shard in self.conn.config.shards.find():
            print(shard)

if __name__ == '__main__':
    dependencies = [
        'pymongo==3.7.2',
        'fire==0.1.3',
    ]
    try:
        pkg_resources.require(dependencies)
    except DistributionNotFound as de:
        print(format(de))
        sys.exit(2)
    except VersionConflict as ve:
        print(format(ve))
        sys.exit(2)
    if sys.version_info[0] is not 3:
        print("Error: Python 3 required, found v" + str(sys.version_info[0]))
        sys.exit(2)

    import fire
    import pymongo
    from configparser import ConfigParser
    import logging

    total_kws = 0
    LOG_FORMAT = '%(asctime)s: [%(levelname)s] %(message)s'

    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
    logging.info('##############################')
    logging.info('## Mongo DB Cluster Tools!! ##')
    logging.info('##############################')

    logging.debug('Finding Config File.. ')
    config_file_path = os.getenv('MONGODBTOOLS_CONFIG_FILE_PATH',
                                 os.path.dirname(os.path.abspath(__file__)) + '/config.ini')
    config = ConfigParser()
    config.read(config_file_path)
    logging.getLogger().setLevel(logging.getLevelName(config.get('PROCESS', 'log_level')))

    logging.debug('Loading config: ' + str(config_file_path))
    fire.Fire(mongodbclustertool)
