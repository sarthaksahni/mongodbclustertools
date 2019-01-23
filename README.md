# mongodbclustertools
A short python3 script for getting quick view of your MongoDB cluster!

I created this script while I was struggling with an imbalanced cluster. There are a lot of ways to get your cluster wrong, but there are few to make it right! This script is an initiative to get more understanding of cluster than just ```db.database.collection.getShardDistribution()```. If you don't know what that command is, you should probably look for it at MongoDB docs.

Installation:
- Simply copy the script or ```git pull``` this repo.
- Install dependencies by ```pip install -r requirements.txt```
- Make ```./mongodbclustertool.py``` executable by ```chmod +x mongodbclustertool.py```
- Create config file at the directory ```config.ini``` with contents from ```config.ini.sample``` provided
- Add your mongos admin credentials to the config file.

Usage:

Lists available databases

    ./mongodbclustertool.py listdbs

Lists All Shards in Cluster

    ./mongodbclustertool.py listshards
Lists available collection in ```<db>```

    ./mongodbclustertool.py listdbs <db>
or

    ./mongodbclustertool.py listdbs --db <db>

Lists chunks marked as ```jumbo``` for ```<collection>``` in ```<db>```

    ./mongodbclustertool.py listdbs <db> <collection>
or

    ./mongodbclustertool.py listdbs --db <db> --coll <collection>

List ```chunk``` distribution across shards for a ```<collection>``` in ```<db>```

    ./mongodbclustertool.py chunkdist <db> <collection>
or

    ./mongodbclustertool.py chunkdist --db <db> --coll <collection>


Todos:
- [ ] Find empty chunks across cluster for a collection
- [ ] Find possible mergeable adjacent empty chunks in a cluster
- [ ] Propose a possible merge chunk fix for better blancing
- [ ] Find size on disk for each shard
- [ ] Fetch last balancing errors
- [ ] Quick MongoDB to CSV export with progressbar2

## How to contribute
I am always open to suggestions and fixes. You can create a PR if you wish to contribute code or you can also add a ToDo in the readme for suggesting any addition.
