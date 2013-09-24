/**
 * @author Vaibhav Bhembre
 * @version 2013/09/19
 * gcc -g -DDEBUG -Wall -Werror -Wshadow -Wwrite-strings -pedantic c-etcd.c -o etcd -lcurl -ljansson -std=c99
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <jansson.h>
#include <curl/curl.h>
#include <assert.h>
#include "c-etcd.h"

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

/* Internal */
static char *etcd_url(const char *key, const char *prefix);
static const char *etcd_host();
static short etcd_port();

static const struct etcd_data *http_request(const char *url, etcd_method method, const char *post_data);
static size_t http_write_callback(void *ptr, size_t size, size_t nmemb, void *userdata);

static void init_etcd_data(struct etcd_data **data);

static bool is_valid_key(const char *);
static bool is_valid_value(const char *);
static void exit_debug(const char *msg);
static void exit_debug_status(const char *msg, int status);
static void debug(const char *msg, ...);

int main(int argc, char *argv[]) {
    char *h;
    etcd_response response;
    const struct etcd_data *val;
    const char *key = "/key1", *value = "value1";

    if (argc <= 1) {
        hostname = DEFAULT_HOSTNAME;
        port = DEFAULT_PORT;
    } else {
        /* Input in format 'host:port' */
        hostname = h = strdup(argv[1]);
        h = strchr(hostname, ':');
        *h++ = '\0';
        port = atoi(h);
    }


    response = etcd_set(key, value, 0);
    assert(response == ETCD_SUCCESS);

    val = etcd_get(key);
    assert(val->response == ETCD_SUCCESS);
    assert(strcmp(val->value, value) == 0);
    free((struct etcd_data *)val);

    response = etcd_delete(key);
    assert(response == ETCD_SUCCESS);

    val = etcd_get(key);
    assert(val->response == ETCD_FAILURE);
    free((struct etcd_data *)val);

    response = etcd_set(key, value, 5);
    assert(response == ETCD_SUCCESS);

    response = etcd_test_and_set(key, "value2", value, 0);
    assert(response == ETCD_SUCCESS);

    response = etcd_test_and_set(key, "value2", value, 0);
    assert(response == ETCD_FAILURE);

    return 0;
}

/* Function definitions */
etcd_response etcd_set(const char *key, const char *value, unsigned ttl) {
    char *url, *data, *tmpdata;
    const struct etcd_data *retdata;
    int ret;
    etcd_response response;

    if (!is_valid_key(key) || !is_valid_value(value)) {
        return ETCD_FAILURE;
    }

    url = etcd_url(key, NULL);
    ret = asprintf(&data, "value=%s", value);
    assert(ret >= 0);

    if (ttl > 0) {
        ret = asprintf(&tmpdata, "%s&ttl=%u", data, ttl);
        assert(ret >= 0);

        free(data);
        data = tmpdata;
    }

    retdata = http_request(url, ETCD_SET, data);
    assert(retdata != NULL);

    response = retdata->response;

    if (response == ETCD_FAILURE) {
        debug(retdata->errmsg);
    }

    free(url);
    free(data);
    free((struct etcd_data *)retdata);
    return response;
}

const struct etcd_data *etcd_get(const char *key) {
    char *url;
    const struct etcd_data *retdata;

    if (!is_valid_key(key)) {
        return NULL;
    }

    url = etcd_url(key, NULL);
    retdata = http_request(url, ETCD_GET, NULL);

    free(url);
    return retdata;
}

etcd_response etcd_delete(const char *key) {
    char *url;
    const struct etcd_data *retdata;
    etcd_response response;

    if (!is_valid_key(key)) {
        return ETCD_FAILURE;
    }

    url = etcd_url(key, NULL);
    retdata = http_request(url, ETCD_DEL, NULL);
    assert(retdata != NULL);

    response = retdata->response;

    if (response == ETCD_FAILURE) {
        debug(retdata->errmsg);
    }

    free(url);
    free((struct etcd_data *)retdata);
    return response;
}

etcd_response etcd_test_and_set(const char *key, const char *value, const char *oldValue, unsigned ttl) {
    char *url, *data, *tmpdata;
    const struct etcd_data *retdata;
    etcd_response response;
    int ret;

    if (!is_valid_key(key) || !is_valid_value(value)) {
        return ETCD_FAILURE;
    }

    if (!is_valid_value(oldValue)) {
        /* If the old value is NULL, then this should act like etcd_set() */
        if (oldValue == NULL) {
            return etcd_set(key, value, ttl);
        }
        return ETCD_FAILURE;
    }

    url = etcd_url(key, NULL);
    ret = asprintf(&data, "value=%s&prevValue=%s", value, oldValue);
    assert(ret >= 0);

    if (ttl > 0) {
        ret = asprintf(&tmpdata, "%s&ttl=%u", data, ttl);
        assert(ret >= 0);

        free(data);
        data = tmpdata;
    }

    retdata = http_request(url, ETCD_SET, data);
    assert(retdata != NULL);

    response = retdata->response;

    if (response == ETCD_FAILURE) {
        debug(retdata->errmsg);
    }

    free(url);
    free(data);
    free((struct etcd_data *)retdata);
    return response;
}

static char *etcd_url(const char *key, const char *prefix) {
    char *url;
    int ret, prefix_allocated = 0;

    if (prefix == NULL) {
        prefix = strdup("keys");
        prefix_allocated = 1;
    }

    ret = asprintf(&url, ETCD_URL_FORMAT, etcd_host(), etcd_port(), prefix, key);
    assert(ret >= 0);

    if (prefix_allocated) {
        free((char *)prefix);
    }
    return url;
}

static const char *etcd_host() {
    if (!hostname) {
        exit_debug("Incorrect hostname provided.");
    }
    return hostname;
}

static short etcd_port() {
    if (port == 0) {
        exit_debug("Incorrect port provided as 0.");
    }
    return port;
}

static bool is_valid_key(const char *key) {
    if (key == NULL || !strlen(key)) {
        debug("Invalid key provided.");
        return false;
    }
    return true;
}

static bool is_valid_value(const char *value) {
    if (value == NULL || !strlen(value)) {
        debug("Invalid value provided.");
        return false;
    }
    return true;
}

static const struct etcd_data *http_request(const char *url, etcd_method method, const char *post_data) {
    CURL *curl;
    static CURLcode curl_code;
    char *errmsg;
    struct etcd_data *data;
    long httpcode;

    if (!url) {
        debug("Incorrect 'url' provided.");
        return NULL;
    }

    curl = curl_easy_init();
    if (!curl) {
        errmsg = strdup("'curl_easy_init()' failed.");
        goto error;
    }

    init_etcd_data(&data);
    if (!data) {
        errmsg = strdup("init failed for etc_data.");
        goto error_cleanup_curl;
    }

    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, http_write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, data);

    if (method == ETCD_SET) {
        if (!post_data) {
            debug("No data provided to method: POST");
            return NULL;
        }

        curl_easy_setopt(curl, CURLOPT_POST, 1);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, post_data);

    } else if (method == ETCD_DEL) {
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
    }

    curl_code = curl_easy_perform(curl);
    if (curl_code != 0) {
        if (curl_code == CURLE_COULDNT_CONNECT) {
            debug("Failed to recieve a response for request: %s. Aborting.", url);
            exit(curl_code);
        }

        errmsg = strdup("'curl_easy_perform' failed.");
        goto error_cleanup_curl;
    }

    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpcode);
    if (httpcode != HTTP_SUCCESS && httpcode != HTTP_BAD_REQ) {
        errmsg = strdup("Server responded with status code:    ");
        sprintf(&errmsg[strlen(errmsg) - 3], "%ld", httpcode);
        goto error_cleanup_curl;
    }

    curl_easy_cleanup(curl);
    return data;

error_cleanup_curl:
    curl_easy_cleanup(curl);
error:
    debug(errmsg);
    free(errmsg);
    return NULL;
}

static size_t http_write_callback(void *ptr, size_t size, size_t nmemb, void *userdata) {
    json_t *response, *value, *in;
    json_error_t error;
    const char *etcd_value;
    const char *errmsg;
    struct etcd_data *data;
    int val;

    data = userdata;

    response = json_loads(ptr, 0, &error);
    if (!json_is_object(response)) {
        strcpy(data->errmsg, "'response' returned is not a json object");
        goto error;
    }

    in = json_object_get(response, "index");
    if (json_is_integer(in)) {
        data->index = json_integer_value(in);
    }
    json_decref(in);

    value = json_object_get(response, "value");
    if (!json_is_string(value)) {
        value = json_object_get(response, "action");
        etcd_value = "";
        if (json_is_string(value)) {
            etcd_value = json_string_value(value);
        }
        if (strcmp(etcd_value, "DELETE") == 0) {
            value = json_object_get(response, "key");
            etcd_value = json_string_value(value);
            strcpy(data->value, ++etcd_value);
            data->response = ETCD_SUCCESS;

            json_decref(value);
            /* json_decref(response); */
            return size * nmemb;

        } else {
            value = json_object_get(response, "errorCode");
            if (!json_is_integer(value)) {
                strcpy(data->errmsg, "Invalid error message.");
                goto error_value;
            }
            val = json_integer_value(value);
            json_decref(value);

            value = json_object_get(response, "message");
            if (!json_is_string(value)) {
                strcpy(data->errmsg, "Invalid error message.");
                goto error_value;
            }

            errmsg = json_string_value(value);
            sprintf(data->errmsg, "%d:%s", val, errmsg);
            goto error_value;
        }
    }
    etcd_value = json_string_value(value);
    memcpy(data->value, etcd_value, strlen(etcd_value) + 1);
    data->response = ETCD_SUCCESS;

    json_decref(value);
    /* json_decref(response); */
    return size * nmemb;

error_value:
    json_decref(value);
error:
    json_decref(response);
    data->value = NULL;
    data->response = ETCD_FAILURE;
    return size * nmemb;
}

static void init_etcd_data(struct etcd_data **data) {
    struct etcd_data *d;

    d = calloc(1, sizeof(*d) + BUFSIZE);
    if (!d) return;

    d->index = -1;
    d->value =  &((char *)d)[sizeof(*d) + 1]; /* Trick to ensure only 1 malloc is required */
    d->errmsg = d->value + (BUFSIZE / 2);
    *data = d;
}

static void exit_debug(const char *msg) {
    exit_debug_status(msg, EXIT_FAILURE);
}

static void exit_debug_status(const char *msg, int status) {
    if (strlen(msg) > 0) {
        fprintf(stderr, "Error: %s\n", msg);
    }
    exit(status);
}

static void debug(const char *msg, ...) {
#ifdef DEBUG
    va_list args;
    va_start(args, msg);

    vfprintf(stdout, msg, args);
    fprintf(stdout, "\n");

    va_end(args);
#endif
}
