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


/*
 * Set a new value for a key.
 *
 * @param key:   A non-empty string that should serve as a key.
 * @param value: A non-empty string that should act as a value for this key.
 * @param ttl:   Set an expiry for the key in seconds. The key is deleted 
 *               after the given duration.
 *               Set it to 0 for unlimited expiry time.
 * @return:      ETCD_SUCCESS if no errors occur, ETCD_FAILURE otherwise.
 */
etcd_response etcd_set(const char *key, const char *value, unsigned ttl);


/*
 * Get the value for the given key.
 *
 * @param key:   A non-empty string that should serve as a key.
 * @return:      A pointer to `etcd_data` struct that contains the value
 *               and the response result.
 *               If `.response` is failure, then `errmsg` will contain the
 *               error string.
 */
const struct etcd_data *etcd_get(const char *key);


/*
 * Delete a given key from the distributed store.
 *
 * @param key:   A non-empty string that should serve as a key.
 * @return:      ETCD_SUCCESS if no errors occur, or responds
 *               with ETCD_FAILURE otherwise.
 */
etcd_response etcd_delete(const char *key);


/*
 * Distributed Test-and-set atomically.
 *
 * @param key:   A non-empty string that should serve as a key.
 * @param value: A non-empty string that should serve as a new 
 *               replacement value if the old value matches.
 * @param oldValue:
 *               A non-empty string serving as old value that is
 *               compared against. If the old value matches with
 *               the value present under the above key, it's re-
 *               placed with the new value.
 * @param ttl:   Expiry time for the key, similar to a set.
 * @return:      ETCD_SUCCESS if new value is successfully, or
 *               ETCD_FAILURE otherwise.
 */
etcd_response etcd_test_and_set(const char *key, const char *value, const char *oldValue, unsigned ttl);
