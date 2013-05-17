#include <Python.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <libcouchbase/couchbase.h>
#include <event.h>


/* ---------------------------------------------------
    Create a libevent event base that can be passed
    to lcb_create.
    
    This should be a temporary hack until we can
    build a tornado ioloop plugin
   ---------------------------------------------------*/

static PyObject *
pylcb_create_event_base(PyObject *self, PyObject *args) {
    struct event_base *evbase;

    evbase = event_base_new();
    return PyCapsule_New(evbase, "event_base", NULL);
}

static PyObject *
pylcb_run_event_loop_nonblock(PyObject *self, PyObject *args) {
    PyObject *capsule;
    struct event_base *evbase;

    if (!PyArg_ParseTuple(args, "O", &capsule)) {
        return NULL;
    }
    evbase = PyCapsule_GetPointer(capsule, "event_base");
    event_base_loop(evbase, 2);

    Py_INCREF(Py_None);
    return Py_None;
}

/* -------------------------------------------------
    end temporary hack awaiting tornado ioloop
    plugin
   ------------------------------------------------- */


/* ----------------------------------------------------------
    Each instance has its own unique set of python callbacks.
    Implemented as a simple linked list with linear search.
   ---------------------------------------------------------- */
struct instance_callbacks {
    PyObject *arithmetic_callback;
    PyObject *configuration_callback;
    PyObject *error_callback;
    PyObject *flush_callback;
    PyObject *get_callback;
    PyObject *http_complete_callback;
    PyObject *http_data_callback;
    PyObject *observe_callback;
    PyObject *remove_callback;
    PyObject *stat_callback;
    PyObject *store_callback;
    PyObject *touch_callback;
    PyObject *unlock_callback;
    PyObject *verbosity_callback;
    PyObject *version_callback;
};

struct callbacks_node {
    lcb_t instance;
    struct instance_callbacks callbacks;
    struct callbacks_node *prev;
    struct callbacks_node *next;
};

static struct callbacks_node *callbacksRoot = NULL;


static void
print_callbacks_node_list()
{
    struct callbacks_node *node;

    node = callbacksRoot;
    while (node) {
        node = node->next;
    }
}


static struct callbacks_node *
add_callbacks_node(lcb_t instance)
{
    struct callbacks_node *newNode = calloc(1, sizeof(struct callbacks_node));
    if (!newNode) {
        PyErr_SetString(PyExc_MemoryError,
                        "ran out of memory while allocating callbacks_node");
        return NULL;
    }
    newNode->instance = instance;

    if (!callbacksRoot) {
        callbacksRoot = newNode;
    } else {
        callbacksRoot->prev = newNode;
        newNode->next = callbacksRoot;
        callbacksRoot = newNode;
    }
    print_callbacks_node_list();
    return newNode;
}


static struct callbacks_node *
find_callbacks_node(lcb_t instance)
{
    struct callbacks_node *node;

    node = callbacksRoot;
    while (node) {
        if (node->instance == instance) {
            break;
        }
        node = node->next;
    }
    return node;
}


static void
remove_callbacks_node(lcb_t instance)
{
    struct callbacks_node *node;

    node = find_callbacks_node(instance);
    if (node) {
        node->prev->next = node->next;
        node->next->prev = node->prev;

        Py_XDECREF(node->callbacks.arithmetic_callback);
        Py_XDECREF(node->callbacks.configuration_callback);
        Py_XDECREF(node->callbacks.error_callback);
        Py_XDECREF(node->callbacks.flush_callback);
        Py_XDECREF(node->callbacks.get_callback);
        Py_XDECREF(node->callbacks.http_complete_callback);
        Py_XDECREF(node->callbacks.http_data_callback);
        Py_XDECREF(node->callbacks.observe_callback);
        Py_XDECREF(node->callbacks.remove_callback);
        Py_XDECREF(node->callbacks.stat_callback);
        Py_XDECREF(node->callbacks.store_callback);
        Py_XDECREF(node->callbacks.touch_callback);
        Py_XDECREF(node->callbacks.unlock_callback);
        Py_XDECREF(node->callbacks.verbosity_callback);
        Py_XDECREF(node->callbacks.version_callback);

        free(node);
    }
}


static PyObject *
set_callback(PyObject *callback, PyObject **target)
{
    if (!PyCallable_Check(callback)) {
        PyErr_SetString(PyExc_TypeError, "parameter must be callable");
        return NULL;
    }

    Py_XINCREF(callback);   /* Add a reference to new callback */
    Py_XDECREF(*target);    /* Dispose of previous callback */
    *target = callback;     /* Remember new callback */

    Py_INCREF(Py_None);
    return Py_None;
}


static void
do_callback(PyObject *callback, PyObject *arglist)
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
     arithmetic_callback
   ---------------------------------------- */
static PyObject *
pylcb_set_arithmetic_callback(PyObject *self, PyObject *args)
{
    PyObject *capsule;
    PyObject *callback;
    lcb_t *instancePtr;
    struct callbacks_node *node;

    if (!PyArg_ParseTuple(args, "OO", &capsule, &callback)) {
        return NULL;
    }
    instancePtr = PyCapsule_GetPointer(capsule, "lcb_instance");

    node = find_callbacks_node(*instancePtr);
    if (!node) {
        return NULL;
    }

    return set_callback(callback, &(node->callbacks.arithmetic_callback));
}


static void
arithmetic_callback(lcb_t instance,
                    const void *cookie,
                    lcb_error_t error,
                    lcb_arithmetic_resp_t *resp)
{
    PyObject *arglist;
    struct callbacks_node *node;

    node = find_callbacks_node(instance);
    if (node) {
        if (node->callbacks.arithmetic_callback) {
            arglist = Py_BuildValue("Ois#l", cookie, error, resp->v.v0.key,
                                    resp->v.v0.nkey, resp->v.v0.value);
            return do_callback(node->callbacks.arithmetic_callback, arglist);
        }
    }
}


/* ----------------------------------------
     configuration callback
   ---------------------------------------- */
static PyObject *
pylcb_set_configuration_callback(PyObject *self, PyObject *args)
{
    PyObject *capsule;
    PyObject *callback;
    lcb_t *instancePtr;
    struct callbacks_node *node;

    if (!PyArg_ParseTuple(args, "OO", &capsule, &callback)) {
        return NULL;
    }
    instancePtr = PyCapsule_GetPointer(capsule, "lcb_instance");

    node = find_callbacks_node(*instancePtr);
    if (!node) {
        return NULL;
    }

    return set_callback(callback, &(node->callbacks.configuration_callback));
}


static void
configuration_callback(lcb_t instance, lcb_configuration_t config)
{
    PyObject *arglist;
    struct callbacks_node *node;

    node = find_callbacks_node(instance);
    if (node) {
        if (node->callbacks.configuration_callback) {
            arglist = Py_BuildValue("(i)", config);
            return do_callback(node->callbacks.configuration_callback, arglist);
        }
    }
}


/* ----------------------------------------
    error_callback
   ---------------------------------------- */
static PyObject *
pylcb_set_error_callback(PyObject *self, PyObject *args)
{
    PyObject *capsule;
    PyObject *callback;
    lcb_t *instancePtr;
    struct callbacks_node *node;

    if (!PyArg_ParseTuple(args, "OO", &capsule, &callback)) {
        return NULL;
    }
    instancePtr = PyCapsule_GetPointer(capsule, "lcb_instance");

    node = find_callbacks_node(*instancePtr);
    if (!node) {
        return NULL;
    }

    return set_callback(callback, &(node->callbacks.error_callback));
}


static void
error_callback(lcb_t instance, lcb_error_t error, const char *errinfo)
{
    PyObject *arglist;
    struct callbacks_node *node;

    node = find_callbacks_node(instance);
    if (node) {
        if (node->callbacks.error_callback) {
            arglist = Py_BuildValue("is", error, errinfo);
            do_callback(node->callbacks.error_callback, arglist);
        }
    }
}


/* ----------------------------------------
     flush_callback
   ---------------------------------------- */
static PyObject *
pylcb_set_flush_callback(PyObject *self, PyObject *args)
{
    PyObject *capsule;
    PyObject *callback;
    lcb_t *instancePtr;
    struct callbacks_node *node;

    if (!PyArg_ParseTuple(args, "OO", &capsule, &callback)) {
        return NULL;
    }
    instancePtr = PyCapsule_GetPointer(capsule, "lcb_instance");

    node = find_callbacks_node(*instancePtr);
    if (!node) {
        return NULL;
    }

    return set_callback(callback, &(node->callbacks.flush_callback));
}


static void
flush_callback(lcb_t instance,
               const void *cookie,
               lcb_error_t error,
               lcb_server_stat_resp_t *resp)
{
    PyObject *arglist;
    struct callbacks_node *node;

    node = find_callbacks_node(instance);
    if (node) {
        if (node->callbacks.flush_callback) {
            arglist = Py_BuildValue("Ois", cookie, error,
                                    resp->v.v0.server_endpoint);
            return do_callback(node->callbacks.flush_callback, arglist);
        }
    }
}


/* ----------------------------------------
     get_callback
   ---------------------------------------- */
static PyObject *
pylcb_set_get_callback(PyObject *self, PyObject *args)
{
    PyObject *capsule;
    PyObject *callback;
    lcb_t *instancePtr;
    struct callbacks_node *node;

    if (!PyArg_ParseTuple(args, "OO", &capsule, &callback)) {
        return NULL;
    }
    instancePtr = PyCapsule_GetPointer(capsule, "lcb_instance");

    node = find_callbacks_node(*instancePtr);
    if (!node) {
        return NULL;
    }

    return set_callback(callback, &(node->callbacks.get_callback));
}
             
     
static void
get_callback(lcb_t instance, const void *cookie,
             lcb_error_t error, lcb_get_resp_t *resp)
{
    PyObject *arglist;
    struct callbacks_node *node;

    node = find_callbacks_node(instance);
    if (node) {
        if (node->callbacks.get_callback) {
            arglist = Py_BuildValue("Ois#s#i", cookie, error,
                                    resp->v.v0.key, resp->v.v0.nkey, 
                                    resp->v.v0.bytes, resp->v.v0.nbytes,
                                    resp->v.v0.flags);
            return do_callback(node->callbacks.get_callback, arglist);
        }
    }
}


/* ----------------------------------------
     http_complete_callback
   ---------------------------------------- */
static PyObject *
pylcb_set_http_complete_callback(PyObject *self, PyObject *args)
{
    PyObject *capsule;
    PyObject *callback;
    lcb_t *instancePtr;
    struct callbacks_node *node;

    if (!PyArg_ParseTuple(args, "OO", &capsule, &callback)) {
        return NULL;
    }
    instancePtr = PyCapsule_GetPointer(capsule, "lcb_instance");

    node = find_callbacks_node(*instancePtr);
    if (!node) {
        return NULL;
    }

    return set_callback(callback, &(node->callbacks.http_complete_callback));
}


static void
http_complete_callback(lcb_http_request_t request,
                       lcb_t instance,
                       const void *cookie,
                       lcb_error_t error,
                       lcb_http_resp_t *resp)
{
    PyObject *arglist;
    struct callbacks_node *node;

    node = find_callbacks_node(instance);
    if (node) {
        if (node->callbacks.http_complete_callback) {
            arglist = Py_BuildValue("Oiis#ss#", cookie, error,
                                    resp->v.v0.status,
                                    resp->v.v0.path, resp->v.v0.npath,
                                    resp->v.v0.headers,
                                    resp->v.v0.bytes, resp->v.v0.nbytes);
            return do_callback(node->callbacks.http_complete_callback, arglist);
        }
    }
}


/* ----------------------------------------
     remove_callback
   ---------------------------------------- */
static PyObject *
pylcb_set_remove_callback(PyObject *self, PyObject *args)
{
    PyObject *capsule;
    PyObject *callback;
    lcb_t *instancePtr;
    struct callbacks_node *node;

    if (!PyArg_ParseTuple(args, "OO", &capsule, &callback)) {
        return NULL;
    }
    instancePtr = PyCapsule_GetPointer(capsule, "lcb_instance");

    node = find_callbacks_node(*instancePtr);
    if (!node) {
        return NULL;
    }

    return set_callback(callback, &(node->callbacks.remove_callback));
}


static void
remove_callback(lcb_t instance,
                const void *cookie,
                lcb_error_t error,
                lcb_remove_resp_t *resp)
{
    PyObject *arglist;
    struct callbacks_node *node;

    node = find_callbacks_node(instance);
    if (node) {
        if (node->callbacks.remove_callback) {
            arglist = Py_BuildValue("Ois#", cookie, error,
                                    resp->v.v0.key, resp->v.v0.nkey);
            return do_callback(node->callbacks.remove_callback, arglist);
        }
    }
}


/* ----------------------------------------
     stat_callback
   ---------------------------------------- */
static PyObject *
pylcb_set_stat_callback(PyObject *self, PyObject *args)
{
    PyObject *capsule;
    PyObject *callback;
    lcb_t *instancePtr;
    struct callbacks_node *node;

    if (!PyArg_ParseTuple(args, "OO", &capsule, &callback)) {
        return NULL;
    }
    instancePtr = PyCapsule_GetPointer(capsule, "lcb_instance");

    node = find_callbacks_node(*instancePtr);
    if (!node) {
        return NULL;
    }

    return set_callback(callback, &(node->callbacks.stat_callback));
}


static void
stat_callback(lcb_t instance,
              const void *cookie,
              lcb_error_t error,
              lcb_server_stat_resp_t *resp)
{
    PyObject *arglist;
    struct callbacks_node *node;

    node = find_callbacks_node(instance);
    if (node) {
        if (node->callbacks.stat_callback) {
            arglist = Py_BuildValue("Oiss#s#", cookie, error,
                                    resp->v.v0.server_endpoint,
                                    resp->v.v0.key, resp->v.v0.nkey, 
                                    resp->v.v0.bytes, resp->v.v0.nbytes);
            return do_callback(node->callbacks.stat_callback, arglist);
        }
    }
}


/* ----------------------------------------
     store_callback
   ---------------------------------------- */
static PyObject *
pylcb_set_store_callback(PyObject *self, PyObject *args)
{
    PyObject *capsule;
    PyObject *callback;
    lcb_t *instancePtr;
    struct callbacks_node *node;

    if (!PyArg_ParseTuple(args, "OO", &capsule, &callback)) {
        return NULL;
    }
    instancePtr = PyCapsule_GetPointer(capsule, "lcb_instance");

    node = find_callbacks_node(*instancePtr);
    if (!node) {
        return NULL;
    }

    return set_callback(callback, &(node->callbacks.store_callback));
}


static void
store_callback(lcb_t instance,
               const void *cookie,
               lcb_storage_t operation,
               lcb_error_t error,
               lcb_store_resp_t *resp)
{
    PyObject *arglist;
    struct callbacks_node *node;

    node = find_callbacks_node(instance);
    if (node) {
        if (node->callbacks.store_callback) {
            arglist = Py_BuildValue("Ois#", cookie, error, 
                                    resp->v.v0.key, resp->v.v0.nkey);
            return do_callback(node->callbacks.store_callback, arglist);
        }
    }
}


void
lcb_instance_destructor(PyObject *capsule) {
    lcb_t *instancePtr;

    instancePtr = PyCapsule_GetPointer(capsule, "lcb_instance");
    fprintf(stdout, "destroying instance %p\n", *instancePtr);
    remove_callbacks_node(*instancePtr);
    print_callbacks_node_list();
    lcb_destroy(*instancePtr);
    free(instancePtr);
}


static PyObject *
pylcb_create(PyObject *self, PyObject *args) {
    PyObject *capsule;
    char *host = NULL;
    char *user = NULL;
    char *passwd = NULL;
    char *bucket = NULL;
    int type = LCB_TYPE_BUCKET;

    struct event_base *evbase;
    struct lcb_create_st create_options;
    struct lcb_create_io_ops_st io_opts;

    lcb_error_t err;
    char errMsg[256];

    if (!PyArg_ParseTuple(args, "O|ssssi", &capsule, &host, &user,
                          &passwd, &bucket, &type))
        return NULL;

    evbase = PyCapsule_GetPointer(capsule, "event_base");
    io_opts.version = 0;
    io_opts.v.v0.type = LCB_IO_OPS_LIBEVENT;
    io_opts.v.v0.cookie = evbase;

    memset(&create_options, 0, sizeof(create_options));
    create_options.version = 1;
    err = lcb_create_io_ops(&create_options.v.v1.io, &io_opts);
    if (err != LCB_SUCCESS) {
        snprintf(errMsg, 256, "pylcb, failed to create IO instance: %s\n",
                 lcb_strerror(NULL, err));
        PyErr_SetString(PyExc_IOError, errMsg);
        return NULL;
    }

    create_options.v.v1.host = host;
    create_options.v.v1.user = user;
    create_options.v.v1.passwd = passwd;
    create_options.v.v1.bucket = bucket;
    create_options.v.v1.type = type;

    lcb_t *instancePtr = calloc(1, sizeof(lcb_t));
    if (!instancePtr) {
        PyErr_SetString(PyExc_MemoryError,
                        "ran out of memory while allocating lcb_t instance");
        return NULL;
    }

    err = lcb_create(instancePtr, &create_options);
    if (err != LCB_SUCCESS) {
        free(instancePtr);
        snprintf(errMsg, 256, "pylcb, failed to create libcouchbase instance: %s\n",
                 lcb_strerror(NULL, err));
        PyErr_SetString(PyExc_IOError, errMsg);
    }

    add_callbacks_node(*instancePtr);

    lcb_set_error_callback(*instancePtr, (lcb_error_callback) error_callback);
    lcb_set_arithmetic_callback(*instancePtr, (lcb_arithmetic_callback) arithmetic_callback);
    lcb_set_configuration_callback(*instancePtr, (lcb_configuration_callback) configuration_callback);
    lcb_set_get_callback(*instancePtr, (lcb_get_callback) get_callback);
    lcb_set_flush_callback(*instancePtr, (lcb_flush_callback) flush_callback);
    lcb_set_http_complete_callback(*instancePtr, (lcb_http_complete_callback) http_complete_callback);
    lcb_set_remove_callback(*instancePtr, (lcb_remove_callback) remove_callback);
    lcb_set_stat_callback(*instancePtr, (lcb_stat_callback) stat_callback);
    lcb_set_store_callback(*instancePtr, (lcb_store_callback) store_callback);

    return PyCapsule_New(instancePtr, "lcb_instance", lcb_instance_destructor);
}


static PyObject *
pylcb_connect(PyObject *self, PyObject *args) {
    PyObject *capsule;
    lcb_t *instancePtr;

    lcb_error_t err;
    char errMsg[256];

    if (!PyArg_ParseTuple(args, "O", &capsule))
        return NULL;
    instancePtr = PyCapsule_GetPointer(capsule, "lcb_instance");

    /* Initiate the connect sequence in libcouchbase */
    if ((err = lcb_connect(*instancePtr)) != LCB_SUCCESS) {
        snprintf(errMsg, 256, "pylcb, failed to initiate connect: %s\n",
                 lcb_strerror(NULL, err));
        PyErr_SetString(PyExc_IOError, errMsg);
        lcb_destroy(*instancePtr);
        return NULL;
    }

    Py_INCREF(Py_None);
    return Py_None;
}


static PyObject *
pylcb_arithmetic(PyObject *self, PyObject *args) {
    PyObject *capsule;
    void *cookie;
    char *key;
    int delta;
    int initial;
    int expiration;
    lcb_t *instancePtr;

    lcb_arithmetic_cmd_t cmd;
    const lcb_arithmetic_cmd_t *commands[1];

    lcb_error_t err;
    char errMsg[256];

    if (!PyArg_ParseTuple(args, "OOsiii", &capsule, &cookie,
                          &key, &delta, &initial, &expiration)) {
        return NULL;
    }
    instancePtr = PyCapsule_GetPointer(capsule, "lcb_instance");

    memset(&cmd, 0, sizeof(cmd));
    cmd.v.v0.key = key;
    cmd.v.v0.nkey = strlen(key);
    cmd.v.v0.exptime = expiration;
    cmd.v.v0.create = 1;
    cmd.v.v0.delta = delta;
    cmd.v.v0.initial = initial;
    commands[0] = &cmd;
    
    err = lcb_arithmetic(*instancePtr, cookie, 1, commands);
    if (err != LCB_SUCCESS) {
        snprintf(errMsg, 256, "pylcb, failed to initiate arithmetic: %s\n",
                 lcb_strerror(NULL, err));
        PyErr_SetString(PyExc_IOError, errMsg);
        return NULL;
    }

    Py_INCREF(Py_None);
    return Py_None;
}


static PyObject *
pylcb_flush(PyObject *self, PyObject *args) {
    PyObject *capsule;
    void *cookie;
    lcb_t *instancePtr;

    lcb_flush_cmd_t cmd;
    const lcb_flush_cmd_t *commands[1];

    lcb_error_t err;
    char errMsg[256];

    if (!PyArg_ParseTuple(args, "OO", &capsule, &cookie)) {
        return NULL;
    }
    instancePtr = PyCapsule_GetPointer(capsule, "lcb_instance");

    memset(&cmd, 0, sizeof(cmd));
    commands[0] = &cmd;
    
    err = lcb_flush(*instancePtr, cookie, 1, commands);
    if (err != LCB_SUCCESS) {
        snprintf(errMsg, 256, "pylcb, failed to initiate flush: %s\n",
                 lcb_strerror(NULL, err));
        PyErr_SetString(PyExc_IOError, errMsg);
        return NULL;
    }

    Py_INCREF(Py_None);
    return Py_None;
}


static PyObject *
pylcb_get(PyObject *self, PyObject *args) {
    PyObject *capsule;
    void *cookie;
    char *key = NULL;
    lcb_t *instancePtr;

    lcb_get_cmd_t cmd;
    const lcb_get_cmd_t *commands[1];

    lcb_error_t err;
    char errMsg[256];

    if (!PyArg_ParseTuple(args, "OOs", &capsule, &cookie, &key)) {
        return NULL;
    }
    instancePtr = PyCapsule_GetPointer(capsule, "lcb_instance");

    commands[0] = &cmd;
    memset(&cmd, 0, sizeof(cmd));
    cmd.v.v0.key = key;
    cmd.v.v0.nkey = strlen(key);

    err = lcb_get(*instancePtr, cookie, 1, commands);
    if (err != LCB_SUCCESS) {
        snprintf(errMsg, 256, "pylcb, failed to initiate get: %s\n",
                 lcb_strerror(NULL, err));
        PyErr_SetString(PyExc_IOError, errMsg);
        return NULL;
    }

    Py_INCREF(Py_None);
    return Py_None;
}


static PyObject *
pylcb_make_http_request(PyObject *self, PyObject *args) {
    PyObject *capsule;
    void *cookie;
    lcb_http_type_t type;
    char *path;
    char *body;
    lcb_http_method_t method;
    int chunked;
    char *content_type;
    lcb_t *instancePtr;

    lcb_http_cmd_t cmd;
    lcb_http_request_t req;

    lcb_error_t err;
    char errMsg[256];

    if (!PyArg_ParseTuple(args, "OOissiis", &capsule, &cookie, &type,
                          &path, &body, &method, &chunked, &content_type)) {
        return NULL;
    }
    instancePtr = PyCapsule_GetPointer(capsule, "lcb_instance");

    memset(&cmd, 0, sizeof(cmd));
    cmd.v.v0.path = path;
    cmd.v.v0.npath = strlen(path);
    cmd.v.v0.body = body;
    cmd.v.v0.nbody = strlen(body);
    cmd.v.v0.method = method;
    cmd.v.v0.chunked = chunked;
    cmd.v.v0.content_type = content_type;

    err = lcb_make_http_request(*instancePtr, cookie, type, &cmd, &req);
    if (err != LCB_SUCCESS) {
        snprintf(errMsg, 256, "pylcb, failed to make http request: %s\n",
                 lcb_strerror(NULL, err));
        PyErr_SetString(PyExc_IOError, errMsg);
        return NULL;
    }

    Py_INCREF(Py_None);
    return Py_None;
}


static PyObject *
pylcb_remove(PyObject *self, PyObject *args) {
    PyObject *capsule;
    void *cookie;
    char *key;
    lcb_t *instancePtr;

    lcb_remove_cmd_t cmd;
    const lcb_remove_cmd_t *commands[1];

    lcb_error_t err;
    char errMsg[256];

    if (!PyArg_ParseTuple(args, "OOs", &capsule, &cookie, &key)) {
        return NULL;
    }
    instancePtr = PyCapsule_GetPointer(capsule, "lcb_instance");

    memset(&cmd, 0, sizeof(cmd));
    cmd.v.v0.key = key;
    cmd.v.v0.nkey = strlen(key);
    commands[0] = &cmd;
    
    err = lcb_remove(*instancePtr, cookie, 1, commands);
    if (err != LCB_SUCCESS) {
        snprintf(errMsg, 256, "pylcb, failed to remove: %s\n",
                 lcb_strerror(NULL, err));
        PyErr_SetString(PyExc_IOError, errMsg);
        return NULL;
    }

    Py_INCREF(Py_None);
    return Py_None;
}


static PyObject *
pylcb_stats(PyObject *self, PyObject *args) {
    PyObject *capsule;
    void *cookie;
    char *name;
    lcb_t *instancePtr;

    lcb_server_stats_cmd_t cmd;
    const lcb_server_stats_cmd_t *commands[1];

    lcb_error_t err;
    char errMsg[256];

    if (!PyArg_ParseTuple(args, "OOs", &capsule, &cookie, &name)) {
        return NULL;
    }
    instancePtr = PyCapsule_GetPointer(capsule, "lcb_instance");

    memset(&cmd, 0, sizeof(cmd));
    cmd.v.v0.name = name;
    cmd.v.v0.nname = strlen(name);
    commands[0] = &cmd;

    err = lcb_server_stats(*instancePtr, cookie, 1, commands);
    if (err != LCB_SUCCESS) {
        snprintf(errMsg, 256, "pylcb, failed to get stats: %s\n",
                 lcb_strerror(NULL, err));
        PyErr_SetString(PyExc_IOError, errMsg);
        return NULL;
    }

    Py_INCREF(Py_None);
    return Py_None;
}


static PyObject *
pylcb_store(PyObject *self, PyObject *args) {
    PyObject *capsule;
    void *cookie;
    char *key;
    int expiration;
    int flags;
    char *value;
    int operation;
    lcb_t *instancePtr;

    lcb_store_cmd_t cmd;
    const lcb_store_cmd_t *commands[1];

    lcb_error_t err;
    char errMsg[256];

    if (!PyArg_ParseTuple(args, "OOsiisi", &capsule, &cookie, &key, 
                          &expiration, &flags, &value, &operation)) {
        return NULL;
    }
    instancePtr = PyCapsule_GetPointer(capsule, "lcb_instance");

    memset(&cmd, 0, sizeof(cmd));
    cmd.v.v0.key = key;
    cmd.v.v0.nkey = strlen(key);
    cmd.v.v0.bytes = value;
    cmd.v.v0.nbytes = strlen(value);
    cmd.v.v0.operation = operation;
    cmd.v.v0.exptime = expiration;
    cmd.v.v0.flags = flags;
    commands[0] = &cmd;
    
    err = lcb_store(*instancePtr, cookie, 1, commands);
    if (err != LCB_SUCCESS) {
        snprintf(errMsg, 256, "pylcb, failed to store: %s\n",
                 lcb_strerror(NULL, err));
        PyErr_SetString(PyExc_IOError, errMsg);
        return NULL;
    }

    Py_INCREF(Py_None);
    return Py_None;
}


static PyObject *
pylcb_strerror(PyObject *self, PyObject *args) {
    lcb_error_t error;

    if (!PyArg_ParseTuple(args, "i", &error))
        return NULL;

    return Py_BuildValue("s", lcb_strerror(NULL, error));
}


static PyObject *
pylcb_wait(PyObject *self, PyObject *args) {
    PyObject *capsule;
    lcb_t *instancePtr;

    if (!PyArg_ParseTuple(args, "O", &capsule))
        return NULL;
    instancePtr = PyCapsule_GetPointer(capsule, "lcb_instance");

    lcb_wait(*instancePtr);

    Py_INCREF(Py_None);
    return Py_None;
}


static PyObject *
pylcb_get_timeout(PyObject *self, PyObject *args) {
    PyObject *capsule;
    lcb_t *instancePtr;

    if (!PyArg_ParseTuple(args, "O", &capsule))
        return NULL;
    instancePtr = PyCapsule_GetPointer(capsule, "lcb_instance");

    return Py_BuildValue("i", lcb_get_timeout(*instancePtr));
}


static PyObject *
pylcb_set_timeout(PyObject *self, PyObject *args) {
    PyObject *capsule;
    int timeout;
    lcb_t *instancePtr;

    if (!PyArg_ParseTuple(args, "Oi", &capsule, &timeout))
        return NULL;
    instancePtr = PyCapsule_GetPointer(capsule, "lcb_instance");

    lcb_set_timeout(*instancePtr, (lcb_uint32_t) timeout);

    Py_INCREF(Py_None);
    return Py_None;
}

static PyMethodDef
LcbMethods[] = {
    { "create", pylcb_create, METH_VARARGS,
      "Create an instance used to connect to Couchbase" },
    { "connect", pylcb_connect, METH_VARARGS,
      "Connect to Couchbase" },
    { "arithmetic", pylcb_arithmetic, METH_VARARGS,
      "Add to or subtract from a numeric key" },
    { "flush", pylcb_flush, METH_VARARGS,
      "Flush a bucket" },
    { "get", pylcb_get, METH_VARARGS,
      "Get a key" },
    { "make_http_request", pylcb_make_http_request, METH_VARARGS,
      "make an http request" },
    { "remove", pylcb_remove, METH_VARARGS,
      "Remove a key" },
    { "stats", pylcb_stats, METH_VARARGS,
      "Get stats from Couchbase cluster" },
    { "store", pylcb_store, METH_VARARGS,
      "Store a key" },
    { "strerror", pylcb_strerror, METH_VARARGS,
      "Return the string representation of an error" },
    { "wait", pylcb_wait, METH_VARARGS,
      "wait for couchbase call to complete" },
    { "create_event_base", pylcb_create_event_base, METH_VARARGS,
      "creates a libevent event base" },
    { "run_event_loop_nonblock", pylcb_run_event_loop_nonblock, METH_VARARGS,
      "run libevent event loop nonblocking" },
    { "get_timeout", pylcb_get_timeout, METH_VARARGS,
      "get libcouchbase operation timeout" },
    { "set_timeout", pylcb_set_timeout, METH_VARARGS,
      "set libcouchbase operation timeout" },
    { "set_arithmetic_callback", pylcb_set_arithmetic_callback, METH_VARARGS,
      "Set callback for lcb_arithmetic"},
    { "set_configuration_callback", pylcb_set_configuration_callback, METH_VARARGS,
      "Set callback for lcb_configuration"},
    { "set_error_callback", pylcb_set_error_callback, METH_VARARGS,
      "Set callback for errors"},
    { "set_flush_callback", pylcb_set_flush_callback, METH_VARARGS,
      "Set callback for lcb_flush"},
    { "set_get_callback", pylcb_set_get_callback, METH_VARARGS,
      "Set callback for lcb_get"},
    { "set_http_complete_callback", pylcb_set_http_complete_callback, METH_VARARGS,
      "Set callback for lcb_make_http_request"},
    { "set_remove_callback", pylcb_set_remove_callback, METH_VARARGS,
      "Set callback for lcb_remove"},
    { "set_stat_callback", pylcb_set_stat_callback, METH_VARARGS,
      "Set callback for lcb_server_stats"},
    { "set_store_callback", pylcb_set_store_callback, METH_VARARGS,
      "Set callback for lcb_store"},
    { NULL, NULL, 0, NULL }
};


PyMODINIT_FUNC
initpylcb(void)
{
    (void) Py_InitModule("pylcb", LcbMethods);
}


