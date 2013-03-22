#include <Python.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <libcouchbase/couchbase.h>


void
lcb_instance_destructor(PyObject *capsule) {
    lcb_t *instance;

    instance = PyCapsule_GetPointer(capsule, "lcb_instance");
    free(instance);
}

static PyObject *
_set_callback(PyObject *self, PyObject *args, PyObject **callback)
{
    PyObject *temp;

    if (!PyArg_ParseTuple(args, "O", &temp))
        return NULL;

    if (!PyCallable_Check(temp)) {
        PyErr_SetString(PyExc_TypeError, "parameter must be callable");
        return NULL;
    }

    Py_XINCREF(temp);           /* Add a reference to new callback */
    Py_XDECREF(*callback);      /* Dispose of previous callback */
    *callback = temp;           /* Remember new callback */

    Py_INCREF(Py_None);
    return Py_None;
}


static void
_do_callback(PyObject *callback, PyObject *arglist)
{
    PyObject *result;

    if (!arglist)
        return;

    result = PyObject_CallObject(callback, arglist);
    Py_DECREF(arglist);
    if (result)
        Py_DECREF(result);
}

/* ----------------------------------------
     error_callback
   ---------------------------------------- */
static PyObject *py_error_callback = NULL;

static PyObject *
_set_error_callback(PyObject *self, PyObject *args)
{
    return _set_callback(self, args, &py_error_callback);
}

static void
error_callback(lcb_t instance, lcb_error_t error, const char *errinfo)
{
    PyObject *arglist;

    if (py_error_callback) {
        arglist = Py_BuildValue("is", error, errinfo);
        return _do_callback(py_error_callback, arglist);
    }
}

/* ----------------------------------------
     get_callback
   ---------------------------------------- */
static PyObject *py_get_callback = NULL;

static PyObject *
_set_get_callback(PyObject *self, PyObject *args)
{
    return _set_callback(self, args, &py_get_callback);
}
                  
static void
get_callback(lcb_t instance, const void *cookie,
             lcb_error_t error, lcb_get_resp_t *resp)
{
    PyObject *arglist;

    if (py_get_callback) {
        arglist = Py_BuildValue("Ois#s#", cookie, error,
                                resp->v.v0.key, resp->v.v0.nkey, 
                                resp->v.v0.bytes, resp->v.v0.nbytes);
        return _do_callback(py_get_callback, arglist);
    }
}

/* ----------------------------------------
     store_callback
   ---------------------------------------- */
static PyObject *py_store_callback = NULL;

static PyObject *
_set_store_callback(PyObject *self, PyObject *args)
{
    return _set_callback(self, args, &py_store_callback);
}


static void
store_callback(lcb_t instance,
               const void *cookie,
               lcb_storage_t operation,
               lcb_error_t error,
               lcb_store_resp_t *resp)
{
    PyObject *arglist;

    if (py_store_callback) {
        arglist = Py_BuildValue("Ois#", cookie, error, 
                                resp->v.v0.key, resp->v.v0.nkey);
        return _do_callback(py_store_callback, arglist);
    }
}


/* ----------------------------------------
     remove_callback
   ---------------------------------------- */
static PyObject *py_remove_callback = NULL;

static PyObject *
_set_remove_callback(PyObject *self, PyObject *args)
{
    return _set_callback(self, args, &py_remove_callback);
}


static void
remove_callback(lcb_t instance,
                const void *cookie,
                lcb_error_t error,
                lcb_remove_resp_t *resp)
{
    PyObject *arglist;

    if (py_remove_callback) {
        arglist = Py_BuildValue("Ois#", cookie, error,
                                resp->v.v0.key, resp->v.v0.nkey);
        return _do_callback(py_remove_callback, arglist);
    }
}


/* ----------------------------------------
     arithmetic_callback
   ---------------------------------------- */
static PyObject *py_arithmetic_callback = NULL;

static PyObject *
_set_arithmetic_callback(PyObject *self, PyObject *args)
{
    return _set_callback(self, args, &py_arithmetic_callback);
}


static void
arithmetic_callback(lcb_t instance,
                    const void *cookie,
                    lcb_error_t error,
                    lcb_arithmetic_resp_t *resp)
{
    PyObject *arglist;

    if (py_arithmetic_callback) {
        arglist = Py_BuildValue("Ois#l", cookie, error, resp->v.v0.key,
                                resp->v.v0.nkey, resp->v.v0.value);
        return _do_callback(py_arithmetic_callback, arglist);
    }
}


/* ----------------------------------------
     stat_callback
   ---------------------------------------- */
static PyObject *py_stat_callback = NULL;

static PyObject *
_set_stat_callback(PyObject *self, PyObject *args)
{
    return _set_callback(self, args, &py_stat_callback);
}


static void
stat_callback(lcb_t instance,
              const void *cookie,
              lcb_error_t error,
              lcb_server_stat_resp_t *resp)
{
    PyObject *arglist;

    if (py_arithmetic_callback) {
        arglist = Py_BuildValue("Oiss#s#", cookie, error,
                                resp->v.v0.server_endpoint,
                                resp->v.v0.key, resp->v.v0.nkey, 
                                resp->v.v0.bytes, resp->v.v0.nbytes);
        return _do_callback(py_stat_callback, arglist);
    }
}


/* ----------------------------------------
     flush_callback
   ---------------------------------------- */
static PyObject *py_flush_callback = NULL;

static PyObject *
_set_flush_callback(PyObject *self, PyObject *args)
{
    return _set_callback(self, args, &py_flush_callback);
}


static void
flush_callback(lcb_t instance,
               const void *cookie,
               lcb_error_t error,
               lcb_server_stat_resp_t *resp)
{
    PyObject *arglist;

    if (py_flush_callback) {
        arglist = Py_BuildValue("Ois", cookie, error,
                                resp->v.v0.server_endpoint);
        return _do_callback(py_flush_callback, arglist);
    }
}


/* ----------------------------------------
     http_complete_callback
   ---------------------------------------- */
static PyObject *py_http_complete_callback = NULL;

static PyObject *
_set_http_complete_callback(PyObject *self, PyObject *args)
{
    return _set_callback(self, args, &py_http_complete_callback);
}


static void
http_complete_callback(lcb_t instance,
                       const void *cookie,
                       lcb_error_t error,
                       lcb_http_resp_t *resp)
{
    PyObject *arglist;

    if (py_http_complete_callback) {
        arglist = Py_BuildValue("Oiis#ss#", cookie, error,
                                resp->v.v0.status,
                                resp->v.v0.path, resp->v.v0.npath,
                                resp->v.v0.headers,
                                resp->v.v0.bytes, resp->v.v0.nbytes);
        return _do_callback(py_http_complete_callback, arglist);
    }
}


static PyObject *
_connect(PyObject *self, PyObject *args) {
    lcb_error_t err;
    struct lcb_create_st create_options;
    struct lcb_create_io_ops_st io_opts;
    char errMsg[256];

    char *host = NULL;
    char *user = NULL;
    char *passwd = NULL;
    char *bucket = NULL;

    if (!PyArg_ParseTuple(args, "|ssss", &host, &user, &passwd, &bucket))
        return NULL;

    io_opts.version = 0;
    io_opts.v.v0.type = LCB_IO_OPS_DEFAULT;
    io_opts.v.v0.cookie = NULL;

    err = lcb_create_io_ops(&create_options.v.v0.io, &io_opts);
    if (err != LCB_SUCCESS) {
        snprintf(errMsg, 256, "Failed to create IO instance: %s\n",
                 lcb_strerror(NULL, err));
        PyErr_SetString(PyExc_IOError, errMsg);
        return NULL;
    }

    memset(&create_options, 0, sizeof(create_options));
    if (host) {
        create_options.v.v0.host = host;
    }
    if (user) {
        create_options.v.v0.user = user;
    }
    if (passwd) {
        create_options.v.v0.passwd = passwd;
    }
    if (bucket) {
        create_options.v.v0.bucket = bucket;
    }

    lcb_t *instance = calloc(1, sizeof(lcb_t));
    if (!instance) {
        PyErr_SetString(PyExc_MemoryError, "ran out of memory while allocating lcb_t instance");
        return NULL;
    }

    err = lcb_create(instance, &create_options);
    if (err != LCB_SUCCESS) {
        free(instance);
        snprintf(errMsg, 256, "Failed to create libcouchbase instance: %s\n",
                 lcb_strerror(NULL, err));
        PyErr_SetString(PyExc_IOError, errMsg);
    }
    
    (void)lcb_set_error_callback(*instance, (lcb_error_callback) error_callback);

    /* Initiate the connect sequence in libcouchbase */
    if ((err = lcb_connect(*instance)) != LCB_SUCCESS) {
        snprintf(errMsg, 256, "Failed to initiate connect: %s\n",
                 lcb_strerror(NULL, err));
        PyErr_SetString(PyExc_IOError, errMsg);
        lcb_destroy(*instance);
        return NULL;
    }

    lcb_set_get_callback(*instance, (lcb_get_callback) get_callback);
    lcb_set_store_callback(*instance, (lcb_store_callback) store_callback);
    lcb_set_remove_callback(*instance, (lcb_remove_callback) remove_callback);
    lcb_set_arithmetic_callback(*instance, (lcb_arithmetic_callback) arithmetic_callback);
    lcb_set_stat_callback(*instance, (lcb_stat_callback) stat_callback);
    lcb_set_flush_callback(*instance, (lcb_flush_callback) flush_callback);
    lcb_set_http_complete_callback(*instance, (lcb_http_complete_callback) http_complete_callback);

    return PyCapsule_New(instance, "lcb_instance", lcb_instance_destructor);
}


static PyObject *
_get(PyObject *self, PyObject *args) {
    PyObject *capsule;
    void *cookie;
    char *key = NULL;
    lcb_t *instance;

    lcb_get_cmd_t cmd;
    const lcb_get_cmd_t *commands[1];

    lcb_error_t err;
    char errMsg[256];

    if (!PyArg_ParseTuple(args, "OOs", &capsule, &cookie, &key)) {
        return NULL;
    }
    instance = PyCapsule_GetPointer(capsule, "lcb_instance");

    commands[0] = &cmd;
    memset(&cmd, 0, sizeof(cmd));
    cmd.v.v0.key = key;
    cmd.v.v0.nkey = strlen(key);

    err = lcb_get(*instance, cookie, 1, commands);
    if (err != LCB_SUCCESS) {
        snprintf(errMsg, 256, "Failed to get: %s\n",
                 lcb_strerror(NULL, err));
        PyErr_SetString(PyExc_IOError, errMsg);
        return NULL;
    }

    Py_INCREF(Py_None);
    return Py_None;
}


static PyObject *
_store(PyObject *self, PyObject *args) {
    PyObject *capsule;
    void *cookie;
    char *key;
    int expiration;
    int flags;
    char *value;
    int operation;
    lcb_t *instance;

    lcb_store_cmd_t cmd;
    const lcb_store_cmd_t *commands[1];

    lcb_error_t err;
    char errMsg[256];

    if (!PyArg_ParseTuple(args, "OOsiisi", &capsule, &cookie, &key, 
                          &expiration, &flags, &value, &operation)) {
        return NULL;
    }
    instance = PyCapsule_GetPointer(capsule, "lcb_instance");

    memset(&cmd, 0, sizeof(cmd));
    cmd.v.v0.key = key;
    cmd.v.v0.nkey = strlen(key);
    cmd.v.v0.bytes = value;
    cmd.v.v0.nbytes = strlen(value);
    cmd.v.v0.operation = operation;
    cmd.v.v0.exptime = expiration;
    cmd.v.v0.flags = flags;
    commands[0] = &cmd;
    
    err = lcb_store(*instance, cookie, 1, commands);
    if (err != LCB_SUCCESS) {
        snprintf(errMsg, 256, "Failed to store: %s\n",
                 lcb_strerror(NULL, err));
        PyErr_SetString(PyExc_IOError, errMsg);
        return NULL;
    }

    Py_INCREF(Py_None);
    return Py_None;
}


static PyObject *
_remove(PyObject *self, PyObject *args) {
    PyObject *capsule;
    void *cookie;
    char *key;
    lcb_t *instance;

    lcb_remove_cmd_t cmd;
    const lcb_remove_cmd_t *commands[1];

    lcb_error_t err;
    char errMsg[256];

    if (!PyArg_ParseTuple(args, "OOs", &capsule, &cookie, &key)) {
        return NULL;
    }
    instance = PyCapsule_GetPointer(capsule, "lcb_instance");

    memset(&cmd, 0, sizeof(cmd));
    cmd.v.v0.key = key;
    cmd.v.v0.nkey = strlen(key);
    commands[0] = &cmd;
    
    err = lcb_remove(*instance, cookie, 1, commands);
    if (err != LCB_SUCCESS) {
        snprintf(errMsg, 256, "Failed to remove: %s\n",
                 lcb_strerror(NULL, err));
        PyErr_SetString(PyExc_IOError, errMsg);
        return NULL;
    }

    Py_INCREF(Py_None);
    return Py_None;
}


static PyObject *
_arithmetic(PyObject *self, PyObject *args) {
    PyObject *capsule;
    void *cookie;
    char *key;
    int delta;
    int initial;
    int expiration;
    lcb_t *instance;

    lcb_arithmetic_cmd_t cmd;
    const lcb_arithmetic_cmd_t *commands[1];

    lcb_error_t err;
    char errMsg[256];

    if (!PyArg_ParseTuple(args, "OOsiii", &capsule, &cookie,
                          &key, &delta, &initial, &expiration)) {
        return NULL;
    }
    instance = PyCapsule_GetPointer(capsule, "lcb_instance");

    memset(&cmd, 0, sizeof(cmd));
    cmd.v.v0.key = key;
    cmd.v.v0.nkey = strlen(key);
    cmd.v.v0.exptime = expiration;
    cmd.v.v0.create = 1;
    cmd.v.v0.delta = delta;
    cmd.v.v0.initial = initial;
    commands[0] = &cmd;
    
    err = lcb_arithmetic(*instance, cookie, 1, commands);
    if (err != LCB_SUCCESS) {
        snprintf(errMsg, 256, "Failed to do arithmetic: %s\n",
                 lcb_strerror(NULL, err));
        PyErr_SetString(PyExc_IOError, errMsg);
        return NULL;
    }

    Py_INCREF(Py_None);
    return Py_None;
}


static PyObject *
_stats(PyObject *self, PyObject *args) {
    PyObject *capsule;
    void *cookie;
    char *name;
    lcb_t *instance;

    lcb_server_stats_cmd_t cmd;
    const lcb_server_stats_cmd_t *commands[1];

    lcb_error_t err;
    char errMsg[256];

    if (!PyArg_ParseTuple(args, "OOs", &capsule, &cookie, &name)) {
        return NULL;
    }
    instance = PyCapsule_GetPointer(capsule, "lcb_instance");

    memset(&cmd, 0, sizeof(cmd));
    cmd.v.v0.name = name;
    cmd.v.v0.nname = strlen(name);
    commands[0] = &cmd;
    
    err = lcb_server_stats(*instance, cookie, 1, commands);
    if (err != LCB_SUCCESS) {
        snprintf(errMsg, 256, "Failed to get stats: %s\n",
                 lcb_strerror(NULL, err));
        PyErr_SetString(PyExc_IOError, errMsg);
        return NULL;
    }

    Py_INCREF(Py_None);
    return Py_None;
}


static PyObject *
_flush(PyObject *self, PyObject *args) {
    PyObject *capsule;
    void *cookie;
    lcb_t *instance;

    lcb_flush_cmd_t cmd;
    const lcb_flush_cmd_t *commands[1];

    lcb_error_t err;
    char errMsg[256];

    if (!PyArg_ParseTuple(args, "OO", &capsule, &cookie)) {
        return NULL;
    }
    instance = PyCapsule_GetPointer(capsule, "lcb_instance");

    memset(&cmd, 0, sizeof(cmd));
    commands[0] = &cmd;
    
    err = lcb_flush(*instance, cookie, 1, commands);
    if (err != LCB_SUCCESS) {
        snprintf(errMsg, 256, "Failed to initiate flush: %s\n",
                 lcb_strerror(NULL, err));
        PyErr_SetString(PyExc_IOError, errMsg);
        return NULL;
    }

    Py_INCREF(Py_None);
    return Py_None;
}


static PyObject *
_strerror(PyObject *self, PyObject *args) {
    lcb_error_t error;

    if (!PyArg_ParseTuple(args, "i", &error))
        return NULL;

    return Py_BuildValue("s", lcb_strerror(NULL, error));
}


static PyObject *
_wait(PyObject *self, PyObject *args) {
    PyObject *capsule;
    lcb_t *instance;

    if (!PyArg_ParseTuple(args, "O", &capsule))
        return NULL;
    instance = PyCapsule_GetPointer(capsule, "lcb_instance");

    lcb_wait(*instance);

    Py_INCREF(Py_None);
    return Py_None;
}


static PyMethodDef
LcbMethods[] = {
    { "set_error_callback", _set_error_callback, METH_VARARGS,
      "Set callback for errors"},
    { "set_get_callback", _set_get_callback, METH_VARARGS,
      "Set callback for lcb_get"},
    { "set_store_callback", _set_store_callback, METH_VARARGS,
      "Set callback for lcb_store"},
    { "set_remove_callback", _set_remove_callback, METH_VARARGS,
      "Set callback for lcb_remove"},
    { "set_arithmetic_callback", _set_arithmetic_callback, METH_VARARGS,
      "Set callback for lcb_arithmetic"},
    { "set_stat_callback", _set_stat_callback, METH_VARARGS,
      "Set callback for lcb_server_stats"},
    { "set_flush_callback", _set_flush_callback, METH_VARARGS,
      "Set callback for lcb_flush"},
    { "set_http_complete_callback", _set_http_complete_callback, METH_VARARGS,
      "Set callback for lcb_make_http_request"},
    { "connect", _connect, METH_VARARGS,
      "Connect to a Couchbase cluster" },
    { "get", _get, METH_VARARGS,
      "Get a key" },
    { "store", _store, METH_VARARGS,
      "Store a key" },
    { "remove", _remove, METH_VARARGS,
      "Remove a key" },
    { "arithmetic", _arithmetic, METH_VARARGS,
      "Add to or subtract from a numeric key" },
    { "stats", _stats, METH_VARARGS,
      "Get stats from Couchbase cluster" },
    { "flush", _flush, METH_VARARGS,
      "Flush a bucket" },
    { "strerror", _strerror, METH_VARARGS,
      "Return the string representation of an error" },
    { "wait", _wait, METH_VARARGS,
      "wait for couchbase call to complete" },
    { NULL, NULL, 0, NULL }
};

PyMODINIT_FUNC
initpylcb(void)
{
    (void) Py_InitModule("pylcb", LcbMethods);
}


