#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <jansson.h>
#include <curl/curl.h>
#include <assert.h>


#define BUFSIZE (10 * 1024) /* Should not go above 10K unless we have massive values */
#define DEFAULT_HOSTNAME "127.0.0.1"
#define DEFAULT_PORT 4001
#define HTTP_SUCCESS 200
#define HTTP_BAD_REQ 400
#define ETCD_URL_FORMAT "http://%s:%hd/v1/%s%s"

static const char *hostname = NULL;
static short port = 0;

typedef enum {
    ETCD_GET,
    ETCD_SET,
    ETCD_DEL
} etcd_method;

typedef enum {
    ETCD_SUCCESS,
    ETCD_FAILURE
} etcd_response;

struct etcd_data {
    etcd_response response;
    char *value;
    char *errmsg;
    int index;
};

/* API */
etcd_response etcd_set(const char *key, const char *value, unsigned ttl);
const struct etcd_data *etcd_get(const char *key);
etcd_response etcd_delete(const char *key);
etcd_response etcd_test_and_set(const char *key, const char *value, const char *oldValue, unsigned ttl);
