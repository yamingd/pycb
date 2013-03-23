from couchbase import Couchbase
import json


client = Couchbase("localhost", "Administrator", "password")
bucket = client.bucket("jobs")

data = dict(dogs="rule", cats="not so much")
dataJson = json.dumps(data, indent=4)
bucket.set("toBeDeleted", 0, 0, dataJson)
print "to be deleted:%s" % bucket.get("toBeDeleted")
print "delete result %s" % bucket.delete("toBeDeleted")
print "try to get deleted, result %s" % bucket.get("toBeDeleted")
print "set result %s" % bucket.set("piranha", 0, 0, dataJson)
print "get result %s" % bucket.get("piranha")
print "add result %s" % bucket.add("piranha", 0, 0, dataJson)
print "incr result %s" % bucket.incr("count", 1, 1, 0)
print "incr result %s" % bucket.incr("count", 1, 1, 0)
print "decr result %s" % bucket.decr("count", 1, 0, 0)
print "stats %s" % bucket.stats()
print "stats(jobs) %s" % bucket.stats("jobs")
print "flush %s" % bucket.flush()
