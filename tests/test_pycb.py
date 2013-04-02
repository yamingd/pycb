from pycb import Couchbase, PycbException
import unittest
import requests
import json
import time


def bucket_list():
    bucketList = []
    r = requests.get("http://localhost:8091/pools/default/buckets")
    for bucket in r.json():
        bucketList.append(bucket['name'])

    return bucketList


class TestPycb(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.cb = Couchbase("localhost", "Administrator", "password")
        self.testBucket = self.cb.create("test")

    @classmethod
    def tearDownClass(self):
        self.cb.delete("test")

    def test_bad_connection_params(self):
        cb = Couchbase("localhost", "Administrator", "passweird")
        with self.assertRaises(PycbException):
            self.testBucket = cb.bucket("test")

    def test_create_and_delete_buckets(self):
        bucketDefinitions = [
            dict(
                name="plain",
                shouldFail=False,
                params={}
            ),
            dict(
                name="bad",
                shouldFail=True,
                params=dict(
                    replicaNumber=100
                )
            ),
            dict(
                name="memcached-bucket",
                shouldFail=False,
                params=dict(
                    bucketType="memcached"
                )
            )
        ]

        for bucketDef in bucketDefinitions:
            name = bucketDef['name']
            params = bucketDef['params']
            if bucketDef['shouldFail'] is True:
                with self.assertRaises(PycbException):
                    self.cb.create(name, **params)
            else:
                self.cb.create(name, **params)
                self.assertIn(name, bucket_list())

        for bucketDef in bucketDefinitions:
            if bucketDef['shouldFail'] is False:
                self.cb.delete(bucketDef['name'])
                self.assertNotIn(bucketDef['name'], bucket_list())

        with self.assertRaises(PycbException):
            self.cb.delete("nonexistentBucket")

    def test_exception_string(self):
        try:
            raise PycbException(10, "it broke")
        except PycbException as e:
            self.assertEqual(e.error, 10)
            self.assertEqual(e.errMsg, "it broke")
            self.assertIn("it broke", e.__str__())

    def test_get_view(self):
        url = "http://Administrator:password@localhost:" \
              "8092/test/_design/dev_test"
        mapFunction = dict(map='function(doc, meta){if (meta.type == "json")'
                               '{emit(doc.key, doc.data);}}')
        view = dict(test=mapFunction)
        payload = dict(views=view)
        headers = {'content-type': 'application/json'}
        requests.put(
            url,
            data=json.dumps(payload, indent=4),
            headers=headers
        )
        self.testBucket.set("testViewKey1", 0, 0,
                            '{"key": "key1", "data": "somedata"}')
        self.testBucket.set("testViewKey2", 0, 0,
                            '{"key": "key2", "data": "somedata"}')
        self.testBucket.set("testViewKey3", 0, 0,
                            '{"key": "key3", "data": "somedata"}')
        params = dict(
            stale="false",
            startkey="key1",
            endkey="key3"
        )
        time.sleep(1)
        rows = self.testBucket.view("_design/dev_test/_view/test", **params)
        self.assertTrue(len(rows) >= 3)

        with self.assertRaises(PycbException):
            self.testBucket.view("not_a_design_document")

    def test_set(self):
        self.testBucket.set("setTestKey", 0, 0, '{"data": "setdata"}')
        data = self.testBucket.get("setTestKey")[2]
        self.assertEqual(data, '{"data": "setdata"}')

    def test_add(self):
        self.testBucket.add("addTestKey", 0, 0, '{"data": "adddata"}')
        data = self.testBucket.get("addTestKey")[2]
        self.assertEqual(data, '{"data": "adddata"}')

        with self.assertRaises(PycbException):
            self.testBucket.add("addTestKey", 0, 0, '{"data": "adddata"}')

    def test_replace(self):
        with self.assertRaises(PycbException):
            self.testBucket.replace("replaceTestKey", 0, 0,
                                    '{"data": "replacedata"}')

        self.testBucket.set("replaceTestKey", 0, 0,
                            '{"data": "notreplacedata"}')
        self.testBucket.replace("replaceTestKey", 0, 0,
                                '{"data": "replacedata"}')
        data = self.testBucket.get("replaceTestKey")[2]
        self.assertEqual(data, '{"data": "replacedata"}')

    def test_append(self):
        self.testBucket.set("appendTestKey", 0, 0, "not JSON")
        self.testBucket.append("appendTestKey", ", appended")
        data = self.testBucket.get("appendTestKey")[2]
        self.assertEqual(data, "not JSON, appended")

    def test_prepend(self):
        self.testBucket.set("prependTestKey", 0, 0, "not JSON")
        self.testBucket.prepend("prependTestKey", "prepended, ")
        data = self.testBucket.get("prependTestKey")[2]
        self.assertEqual(data, "prepended, not JSON")

    def test_get(self):
        self.testBucket.set("getTestKey", 0, 0, '{"data": "getData"}')
        data = self.testBucket.get("getTestKey")[2]
        self.assertEqual(data, '{"data": "getData"}')

    def test_delete(self):
        self.testBucket.set("deleteTestKey", 0, 0, '{"data": "deleteData"}')
        self.testBucket.delete("deleteTestKey")
        with self.assertRaises(PycbException):
            self.testBucket.get("deleteTestKey")[2]
        with self.assertRaises(PycbException):
            self.testBucket.delete("deleteTestKey")

    def test_increment_and_decrement(self):
        self.testBucket.incr("countKey")
        self.assertEqual(self.testBucket.get("countKey")[2], 0)
        self.testBucket.incr("countKey", amt=3)
        self.assertEqual(self.testBucket.get("countKey")[2], 3)
        self.testBucket.decr("countKey")
        self.assertEqual(self.testBucket.get("countKey")[2], 2)
        self.testBucket.set("countKey", 0, 0, "non-numeric")
        with self.assertRaises(PycbException):
            self.testBucket.incr("countKey")

    def test_stats(self):
        results = self.testBucket.stats()
        self.assertIsInstance(results, list)
        self.assertTrue(len(results) >= 1)

    def test_flush(self):
        results = self.testBucket.flush()
        self.assertIsInstance(results, list)
        self.assertTrue(len(results) >= 1)

if __name__ == '__main__':
    unittest.main()
