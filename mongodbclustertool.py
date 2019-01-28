#!/usr/bin/env python
import os
import sys

import pkg_resources
from pkg_resources import DistributionNotFound, VersionConflict


def get_human_size(size, precision=2):
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB']
    suffixIndex = 0
    while size > 1024 and suffixIndex < 4:
        suffixIndex += 1
        size = size / 1024.0
    return "%.*f%s" % (precision, size, suffixes[suffixIndex])


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
        for jumbo in self.conn.config.chunks.find({"ns": db + "." + coll, "jumbo": True}):
            count += 1;
            print(jumbo)
        if count > 0:
            print("You have", count, "jumbos!")
        else:
            print("Great! No jumbos found!")

    def chunkdist(self, db, coll):
        if self.conn is None:
            self.conn = pymongo.MongoClient(config.get('MONGO', 'CONNECT_URI'))
        shards = {}
        totalchunks = 0
        for chunk in self.conn.config.chunks.find({"ns": db + "." + coll}):
            totalchunks += 1
            if chunk['shard'] not in shards:
                shards[chunk['shard']] = {"count": 1, "percent": (1 / totalchunks) * 100}
            else:
                shards[chunk['shard']]['count'] += 1
                shards[chunk['shard']]['percent'] = float(
                    "{0:.2f}".format((shards[chunk['shard']]['count'] / totalchunks) * 100))
        for shard in shards:
            print("Shard:", shard, "| Chunks:", shards[shard]['count'], "| Distribution:",
                  str(shards[shard]['percent']) + "%")

    def listshards(self):
        if self.conn is None:
            self.conn = pymongo.MongoClient(config.get('MONGO', 'CONNECT_URI'))
        for shard in self.conn.config.shards.find():
            print(shard)

    def exportlarge(self, db, coll, query, procs=5):
        if self.conn is None:
            self.conn = pymongo.MongoClient(config.get('MONGO', 'CONNECT_URI'))
        writable_dir = './temp'
        out_file = './output.csv'
        if not os.path.exists(writable_dir):
            os.makedirs(writable_dir)
        collection = self.conn[db][coll]
        if type(query) is not dict:
            print("The query is not well formatted (needs to look like a python dict). You passed: ", query)
            return False
        total_records = collection.count(query)

        if total_records is 0:
            print("Nothing to export. Found zero records.")
            return False

        records_per_proc = int(total_records / procs)
        print("Found", total_records, "records,", records_per_proc, "will be exported using", procs, "processes.")
        i = 0
        skip = 0
        while i < procs:
            limit = skip + records_per_proc
            if i is (procs - 1):
                limit = total_records
            print(i, "proc -> ", skip, limit)
            skip = skip + records_per_proc
            i = i + 1
        proceed = input("Do you wish to proceed? (y/n): ")
        if (proceed.lower() == "y") or (proceed.lower() == "yes"):
            print("Starting export, your response was", proceed)

            i = 0
            skip = 0
            processes = []
            limit = records_per_proc
            while i < procs:

                if i is (procs - 1):
                    limit = total_records

                print(i, "proc -> ", skip, limit)
                p = Process(target=self.exportlargeproccess, args=(i, db, coll, query, skip, limit, writable_dir))
                p.start()
                processes.append(p)
                skip = skip + records_per_proc
                i = i + 1
            i = 0
            for proc in processes:
                proc.join()
                print("=>", proc.pid, "process completed. Process:", i, "out of", len(processes))
                i += 1

            print("Combining files created at", writable_dir)

            script = "cat " + writable_dir + "/*.csv>./" + out_file
            call(script, shell=True)

        else:
            print("Aborting Exporting, your response was", proceed)
            return False

    def exportlargeproccess(self, id, db, coll, query, skip, limit, writable_dir):
        file = writable_dir + "/" + db + "_" + coll + "_" + str(id) + "." + str(skip) + "-" + str(limit) + ".csv"
        print("  => Starting Export Process for Process ID:", id, "skip:", skip, "limit:", limit, "writing:", file)
        conn = pymongo.MongoClient(config.get('MONGO', 'CONNECT_URI'))
        collection = conn[db][coll]
        file_write = open(file, "w")
        csv_writer = None
        for data in collection.find(query).skip(skip).limit(limit):
            flat_data = flattenDict(data)
            if csv_writer is None:
                keys = flat_data.keys()
                csv_writer = csv.DictWriter(file_write, keys)
            csv_writer.writerow(flat_data)
        print("  => Data export finished for Proc:", id)


def flattenDict(d, result=None):
    """
    https://gist.github.com/higarmi/6708779
    """
    if result is None:
        result = {}
    for key in d:
        value = d[key]
        if isinstance(value, dict):
            value1 = {}
            for keyIn in value:
                value1[".".join([key, keyIn])] = value[keyIn]
            flattenDict(value1, result)
        elif isinstance(value, (list, tuple)):
            for indexB, element in enumerate(value):
                if isinstance(element, dict):
                    value1 = {}
                    index = 0
                    for keyIn in element:
                        newkey = ".".join([key, keyIn])
                        value1[".".join([key, keyIn])] = value[indexB][keyIn]
                        index += 1
                    for keyA in value1:
                        flattenDict(value1, result)
        else:
            result[key] = value
    return result


# pymongo==3.7.2
# fire==0.1.3

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
    from multiprocessing import Process
    import csv
    from subprocess import call

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
