##v0.0.5
* Create libevent event base for our io_ops, then call it in non-blocking mode to allow timeouts when connecting to a bucket.
* Added PycbKeyNotFound and PycbKeyExists exceptions

## V0.0.4
* Added checks for libcouchbase calls that don't fail but also don't generate a callback.

## v0.0.3
* Added create and delete bucket. added tests

## V0.0.2
* Added create method to class Couchbase, which creates a new bucket

## v0.0.1:
* Initial release
