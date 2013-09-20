/**
 * @author Vaibhav Bhembre
 * @version 2013/09/19
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <jansson.h>
#include <curl/curl.h>
#include <assert.h>
#include "c-etcd.h"

#define BUFSIZE (10 * 1024) /* Should not go above 10K unless we have massive values */
#define DEFAULT_HOSTNAME "127.0.0.1"
#define DEFAULT_PORT 4001
#define HTTP_SUCCESS 200
#define ETCD_URL_FORMAT "http://%s:%hd/v1/keys/%s"

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
};

/* API */
etcd_response etcd_set(const char *key, const char *value);
const char *etcd_get(const char *key);

/* Internal */
static char *etcd_url(const char *key);
static const char *etcd_host();
static short etcd_port();

static const struct etcd_data *http_request(const char *url, etcd_method method, const char *post_data);
static size_t http_write_callback(void *ptr, size_t size, size_t nmemb, void *userdata);

static void init_etcd_data(struct etcd_data **data);
static void exit_debug(const char *msg);
static void exit_debug_status(const char *msg, int status);
static void debug(const char *msg);

int main(int argc, char *argv[]) {
    char *h;
    etcd_response response;
    const char *val;
    const char *key = "key1", *value = "value1";

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


    response = etcd_set(key, value);
    assert(response == ETCD_SUCCESS);

    val = etcd_get(key);
    assert(strcmp(val, value) == 0);

    return 0;
}

/* Function definitions */
etcd_response etcd_set(const char *key, const char *value) {
    char *url, *data;
    const struct etcd_data *retdata;
    int ret;
    etcd_response response;

    if (!strlen(key)) {
        debug("Key provided cannot be empty.");
        return ETCD_FAILURE;
    }

    if (!strlen(value)) {
        debug("Value must be a non-empty string.");
        return ETCD_FAILURE;
    }

    url = etcd_url(key);
    ret = asprintf(&data, "value=%s", value);
    assert(ret >= 0);

    retdata = http_request(url, ETCD_SET, data);
    assert(retdata != NULL);

    response = retdata->response;

    if (response == ETCD_FAILURE) {
        debug(retdata->errmsg);
    }

    free(url);
    free((struct etcd_data *)retdata);
    return response;
}

const char *etcd_get(const char *key) {
    char *url;
    const struct etcd_data *retdata;
    const char *value;

    if (key == NULL || !strlen(key)) {
        debug("Key cannot be an empty string.");
        return NULL;
    }

    url = etcd_url(key);
    retdata = http_request(url, ETCD_GET, NULL);
    assert(retdata != NULL);

    free(url);

    if (retdata->response == ETCD_FAILURE) {
        debug(retdata->errmsg);
        goto retn;
    }

    if (retdata->response == ETCD_SUCCESS) {
        value = strdup(retdata->value);
        free((struct retdata *)retdata);
        return value;
    }

retn:
    free((struct retdata *)retdata);
    return NULL;

}

__attribute__ ((unused))
static etcd_response etcd_delete(const char *key) {
    /* To be defined */
    return ETCD_FAILURE;
}

static char *etcd_url(const char *key) {
    char *url;
    int ret;

    ret = asprintf(&url, ETCD_URL_FORMAT, etcd_host(), etcd_port(), key);
    assert(ret >= 0);

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
        /* Add logic for delete */
    }

    curl_code = curl_easy_perform(curl);
    if (curl_code != 0) {
        errmsg = strdup("'curl_easy_perform' failed.");
        goto error_cleanup_curl;
    }

    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpcode);
    if (httpcode != HTTP_SUCCESS) {
        errmsg = strdup("Server responded with status code:    ");
        sprintf(&errmsg[strlen(errmsg) - 3], "%ld", httpcode);
        goto error_cleanup_curl;
    }

    curl_easy_cleanup(curl);
    return data;

error_cleanup_curl:
    curl_easy_cleanup(curl);
error:
    exit_debug(errmsg);
    return NULL;
}

static size_t
http_write_callback(void *ptr, size_t size, size_t nmemb, void *userdata) {
    json_t *response, *value;
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

    value = json_object_get(response, "value");
    if (!json_is_string(value)) {
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

    etcd_value = json_string_value(value);
    memcpy(data->value, etcd_value, strlen(etcd_value) + 1);
    data->response = ETCD_SUCCESS;

    json_decref(value);
    json_decref(response);
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

    d = malloc(sizeof(*d) + BUFSIZE);
    if (!d) return;
    memset(d, 0x00, sizeof(*d) + BUFSIZE);

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

static void debug(const char *msg) {
    fprintf(stdout, "%s\n", msg);
}
