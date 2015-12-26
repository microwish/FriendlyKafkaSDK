/*
g++ -g -W -Wall -c -fPIC -I/home/c/opensource/librdkafka-debian-0.8.6-1/src -o PyKafkaClient.o PyKafkaClient.cpp
g++ -shared -o pykafkaclient.so PyKafkaClient.o -lrdkafka -lz -lpthread -lrt
*/

#include "PyKafkaClient.h"

#include <string>
#include <map>
#include <vector>
#include <stdio.h>
#include <ctype.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/time.h>
#include <stdarg.h>
#include <signal.h>
#include <syslog.h>
#include <errno.h>

static std::map<std::string, std::string> raw_conf;

#define LOG_PATH "./kafkaclient.log"

/* Log levels */
#define LL_DEBUG 0
#define LL_VERBOSE 1
#define LL_NOTICE 2
#define LL_WARNING 3
#define LL_RAW (1<<10) /* Modifier to log without timestamp */
#define CONFIG_DEFAULT_VERBOSITY LL_NOTICE

/* Low level logging. To use only for very big messages, otherwise
 * write_log() is to prefer. */
static void write_log_raw(const char *path, int level, const char *msg)
{
    static FILE *fp = NULL;
    char buf[64];
    int rawmode = (level & LL_RAW);

    if (path == NULL || path[0] == '\0') path = LOG_PATH;

    if (fp == NULL) {
        if ((fp = fopen(path, "a")) == NULL) return;
    }

    level &= 0xff; /* clear flags */

    if (rawmode) {
        fprintf(fp, "%s", msg);
    } else {
        int off;
        struct timeval tv;
        struct tm tm;
        pid_t pid = getpid();
        const char *levelstr;

        switch (level) {
        case LOG_ERR:
            levelstr = "ERROR";
            break;
        case LOG_WARNING:
            levelstr = "WARNING";
            break;
        case LOG_INFO:
            levelstr = "INFO";
            break;
        default:
            levelstr = "UNKNOWN";
        }

        if (gettimeofday(&tv, NULL) != 0) return;
        if (localtime_r(&tv.tv_sec, &tm) == NULL) return;
        off = strftime(buf, sizeof(buf), "%d %b %H:%M:%S.", &tm);
        snprintf(buf + off, sizeof(buf) - off, "%03d", (int)tv.tv_usec / 1000);
        fprintf(fp, "%d: %s %s %s\n", (int)pid, buf, levelstr, msg);
    }
    fflush(fp);

    //fclose(fp);
}

#define LOG_MAX_LEN 1024 /* Default maximum length of syslog messages */

/* Like write_log_raw() but with printf-alike support. This is the function that
 * is used across the code. The raw version is only used in order to dump
 * the INFO output on crash. */
void write_log(const char *path, int level, const char *fmt, ...)
{
    va_list ap;
    char msg[LOG_MAX_LEN];

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    write_log_raw(path, level, msg);
}

static bool parse_conf_file(const char *conf_path)
{
    if (raw_conf.size() > 0) return true;

    FILE *fp;
    if ((fp = fopen(conf_path, "r")) == NULL) {
        write_log(NULL, LOG_ERR, "fopen[%s] failed with errno[%d]",
                  conf_path, errno);
        return false;
    }

    char line[512], *p1, *p2, *p3;

    while (fgets(line, sizeof(line), fp) != NULL) {
        if ((p1 = strchr(line, '=')) == NULL) {
            write_log(NULL, LOG_WARNING, "invalid conf line[%s]", line);
            continue;
        }

        p2 = line;
        while (isspace(*p2) && p2 < p1) p2++;
        if (*p2 == '#') continue;
        if (p2 == p1) {
            write_log(NULL, LOG_WARNING, "invalid conf line[%s]", line);
            continue;
        }
        p3 = p1 - 1;
        while (isspace(*p3)) p3--;
        std::string name(p2, p3 - p2 + 1);

        p2 = p1 + 1;
        while (isspace(*p2) && *p2 != '\0') p2++;
        if (*p2 == '\0') {
            write_log(NULL, LOG_WARNING, "invalid conf line[%s]", line);
            continue;
        }
        p3 = line + strlen(line) - 1;
        while (isspace(*p3)) p3--;
        std::string value(p2, p3 - p2 + 1);

        raw_conf.insert(std::pair<std::string, std::string>(name, value));
    }

    if (ferror(fp)) {
        write_log(NULL, LOG_ERR, "ferror[%s] occurred", conf_path);
        raw_conf.clear();
    }

    fclose(fp);

#if 1
for (std::map<std::string, std::string>::iterator it = raw_conf.begin();
     it != raw_conf.end(); it++) {
    fprintf(stderr, "key[%s] value[%s]\n",
            it->first.c_str(), it->second.c_str());
}
#endif

    return raw_conf.size() > 0;
}

static kafka_client_conf_t *create_conf(const char *conf_path = NULL)
{
    if (conf_path == NULL) {
        if (raw_conf.size() == 0) {
            write_log(NULL, LOG_ERR, "invalid conf path to create_conf");
            return NULL;
        }
    } else {
        if (!parse_conf_file(conf_path)) return NULL;
    }

    kafka_client_conf_t *conf;
    char errstr[512];

    if ((conf = rd_kafka_conf_new()) == NULL) {
        write_log(NULL, LOG_ERR, "rd_kafka_conf_new failed");
        return NULL;
    }

    for (std::map<std::string, std::string>::iterator it = raw_conf.begin();
         it != raw_conf.end(); ++it) {
        if (rd_kafka_conf_set(conf, it->first.c_str(), it->second.c_str(),
                              errstr, sizeof(errstr))
            == RD_KAFKA_CONF_INVALID) {
            write_log(NULL, LOG_ERR,
                      "rd_kafka_conf_set failed with errstr[%s]", errstr);
            rd_kafka_conf_destroy(conf);
            return NULL;
        }
    }

    return conf;
}

static void destroy_conf(kafka_client_conf_t *conf)
{
    if (conf != NULL) {
        rd_kafka_conf_destroy(conf);
    }
}

static void msg_delivery_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage,
                            void *opaque)
{
    if (rkmessage->err == 0) {
        //write_log(NULL, LOG_INFO,
        //          "delivered bytes[%lu] offset[%ld] partition[%d]",
        //          rkmessage->len, rkmessage->offset, rkmessage->partition);
    } else {
        write_log(NULL, LOG_ERR, "err2str[%s] message_errstr[%s]",
                  rd_kafka_err2str(rkmessage->err),
                  rd_kafka_message_errstr(rkmessage));
    }
}

static void err_cb(rd_kafka_t *rk, int err, const char *reason, void *opaque)
{
    write_log(NULL, LOG_ERR, "rdkafka error cb: %s: %s: %s", rd_kafka_name(rk),
              rd_kafka_err2str((rd_kafka_resp_err_t)err), reason);
}

kafka_producer_t *create_kafka_producer(const char *conf_path,
                                        const char *brokers)
{
    if (conf_path == NULL || conf_path[0] == '\0') {
        write_log(NULL, LOG_WARNING, "invalid conf path, "
                  "default instead used for producer");
        conf_path = DEFAULT_CONF_PATH;
    }

    if (!parse_conf_file(conf_path)) return NULL;

    if (brokers == NULL || brokers[0] == '\0') {
        std::map<std::string, std::string>::iterator it =
            raw_conf.find("metadata.broker.list");
        if (it == raw_conf.end()) {
            write_log(NULL, LOG_ERR, "no available meta brokers for producer");
            return NULL;
        }
        brokers = it->second.c_str();
    }

    kafka_client_conf_t *conf;
    kafka_producer_t *producer;
    char errstr[512], temp[16];

    if ((conf = create_conf()) == NULL) return NULL;

    // Quick termination
    snprintf(temp, sizeof(temp), "%i", SIGIO);
    rd_kafka_conf_set(conf, "internal.termination.signal", temp, NULL, 0);

    // Producer config
    // TODO
    //rd_kafka_conf_set(conf, "queue.buffering.max.messages", "500000", NULL, 0);
    //rd_kafka_conf_set(conf, "batch.num.messages", "100", NULL, 0);
    //rd_kafka_conf_set(conf, "queue.buffering.max.ms", "5", NULL, 0);
    //rd_kafka_conf_set(conf, "socket.max.fails", "0", NULL, 0);
    //rd_kafka_conf_set(conf, "message.send.max.retries", "3", NULL, 0);
    //rd_kafka_conf_set(conf, "retry.backoff.ms", "500", NULL, 0);
    //rd_kafka_conf_set(conf, "delivery.report.only.error", "true", NULL, 0);

    rd_kafka_conf_set_error_cb(conf, err_cb);

    /* Set up a message delivery report callback.
     ** It will be called once for each message, either on successful
     ** delivery to broker, or upon failure to deliver to broker. */
    rd_kafka_conf_set_dr_msg_cb(conf, msg_delivery_cb);

    if ((producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf,
                                 errstr, sizeof(errstr))) == NULL) {
        write_log(NULL, LOG_ERR, "rd_kafka_new failed with errstr[%s]", errstr);
        return NULL;
    }

    // TODO
    //rd_kafka_set_logger()
    //rd_kafka_set_log_level()

    if (rd_kafka_brokers_add(producer, brokers) < 1) {
        write_log(NULL, LOG_ERR, "rd_kafka_brokers_add failed for producer");
        destroy_kafka_producer(producer);
        return NULL;
    }

    return producer;
}

void destroy_kafka_producer(kafka_producer_t *producer)
{
    if (producer != NULL) {
        while (rd_kafka_outq_len(producer) > 0)
            rd_kafka_poll(producer, 100);
        rd_kafka_destroy(producer);
        rd_kafka_wait_destroyed(2000);
    }
}

kafka_client_topic_t *set_producer_topic(kafka_producer_t *producer,
                                         const char *topic,
                                         int partitioner_type)
{
    if (topic == NULL || topic[0] == '\0') {
        return NULL;
    }

    rd_kafka_topic_conf_t *topic_conf = rd_kafka_topic_conf_new();
    if (topic_conf == NULL) {
        write_log(NULL, LOG_ERR, "rd_kafka_topic_conf_new failed"
                  " with topic[%s]", topic);
        return NULL;
    }

    char errstr[512];

    // TODO
    //rd_kafka_topic_conf_set(topic_conf, request.required.acks);

    if (partitioner_type == KAFKA_PARTITIONER_CONSISTENT) {
        rd_kafka_topic_conf_set_partitioner_cb(topic_conf,
                                           rd_kafka_msg_partitioner_consistent);
    }

    for (std::map<std::string, std::string>::iterator it = raw_conf.begin();
         it != raw_conf.end(); ++it) {
        if (rd_kafka_topic_conf_set(topic_conf, it->first.c_str(),
                                    it->second.c_str(), errstr, sizeof(errstr))
            == RD_KAFKA_CONF_INVALID) {
            write_log(NULL, LOG_ERR,
                      "rd_kafka_topic_conf_set failed with errstr[%s]", errstr);
            rd_kafka_topic_conf_destroy(topic_conf);
            return NULL;
        }
    }

    kafka_client_topic_t *kct = rd_kafka_topic_new(producer, topic, topic_conf);
    if (kct == NULL) {
        write_log(NULL, LOG_ERR, "rd_kafka_topic_new failed with errno[%d]",
                  errno);
        return NULL;
    }

    return kct;
}

void del_topic(kafka_client_topic_t *kct)
{
    if (kct != NULL) {
        rd_kafka_topic_destroy(kct);
    }
}

// use the default partitioners provided by rdkafka
int produce_messages(kafka_producer_t *producer,
                     kafka_client_topic_t *kct,
                     const std::vector<std::string>& payloads,
                     const std::vector<std::string>& keys)
{
    size_t n = payloads.size(), kn = keys.size();

    if (kn > 0 && n != kn) {
        write_log(NULL, LOG_ERR, "numbers of payloads and keys mismatch");
        return -1;
    }

    size_t num = 0;

    for (size_t i = 0; i < n; i++) {
        int ret;
        void *p = const_cast<char *>(payloads[i].c_str());
        if (kn == 0 || keys[i].length() == 0) {
            ret = rd_kafka_produce(kct, KAFKA_PARTITION_UA,
                                   RD_KAFKA_MSG_F_COPY, p, payloads[i].length(),
                                   NULL, 0, NULL);
        } else {
            ret = rd_kafka_produce(kct, KAFKA_PARTITION_UA,
                                   RD_KAFKA_MSG_F_COPY, p, payloads[i].length(),
                                   keys[i].c_str(), keys[i].length(), NULL);
        }
        if (ret == -1) {
            write_log(NULL, LOG_ERR, "rd_kafka_produce failed with errno[%d]",
                      errno);
            rd_kafka_poll(producer, 50);
            continue;
        }
        num++;
    }

    return num;
}
#if 0
int produce_messages(kafka_producer_t *producer,
                     kafka_client_topic_t *kct,
                     const std::vector<std::string>& payloads,
                     const std::vector<std::string>& keys)
{
    size_t n = payloads.size(), kn = keys.size();
    if (kn > 0 && n != kn) {
        write_log(NULL, LOG_ERR, "numbers of payloads and keys mismatch");
        return -1;
    }

    rd_kafka_message_t *messages =
        (rd_kafka_message_t *)calloc(n, sizeof(rd_kafka_message_t));
    if (messages == NULL) {
        write_log(NULL, LOG_ERR, "calloc for set of rd_kafka_message_t failed");
        return -1;
    }

    for (size_t i = 0; i < n; i++) {
        messages[i].payload = const_cast<char *>(payloads[i].c_str());
        messages[i].len = payloads[i].length();
        if (kn > 0 && keys[i].length() > 0) {
            messages[i].key = const_cast<char *>(keys[i].c_str());
            messages[i].key_len = keys[i].length();
        }
    }

    int r = rd_kafka_produce_batch(kct, KAFKA_PARTITION_UA,
                                   RD_KAFKA_MSG_F_COPY, messages, n);
    if (r != n) {
        for (size_t i = 0; i < n; i++) {
            if (messages[i].err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                write_log(NULL, LOG_ERR, "a message failed to be produced "
                          " with err[%s]", rd_kafka_err2str(messages[i].err));
            }
        }
    }

    free(messages);

    return r;
}
#endif

int poll_producer(kafka_producer_t *producer, int milli, int how, int qs)
{
    int n = 0;

    switch (how) {
    case 2:
        while (rd_kafka_outq_len(producer) > qs)
            n += rd_kafka_poll(producer, milli);
    case 1:
        n += rd_kafka_poll(producer, milli);
        break;
    }

    return n;
}

kafka_consumer_t *create_kafka_consumer(const char *conf_path,
                                        const char *brokers)
{
    if (conf_path == NULL || conf_path[0] == '\0') {
        write_log(NULL, LOG_WARNING, "invalid conf path, "
                  "default instead used for consumer");
        conf_path = DEFAULT_CONF_PATH;
    }

    if (!parse_conf_file(conf_path)) return NULL;

    if (brokers == NULL || brokers[0] == '\0') {
        std::map<std::string, std::string>::iterator it =
            raw_conf.find("metadata.broker.list");
        if (it == raw_conf.end()) {
            write_log(NULL, LOG_ERR, "no available meta brokers for consumer");
            return NULL;
        }
        brokers = it->second.c_str();
    }

    kafka_client_conf_t *conf;
    kafka_consumer_t *consumer;
    char errstr[512], temp[16];

    if ((conf = create_conf()) == NULL) return NULL;

    // Quick termination
    snprintf(temp, sizeof(temp), "%i", SIGIO);
    rd_kafka_conf_set(conf, "internal.termination.signal", temp, NULL, 0);

    // TODO
    //rd_kafka_conf_set(conf, "queued.min.messages", "1000000", NULL, 0);

    rd_kafka_conf_set_error_cb(conf, err_cb);

#if 0
    if (rd_kafka_conf_set(conf, "offset.store.method", "broker",
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        write_log(NULL, LOG_ERR, "rd_kafka_conf_set failed with errstr[%s]",
                  errstr);
        destroy_conf(conf);
        return NULL;
    }
#endif

    if ((consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf,
                                 errstr, sizeof(errstr))) == NULL) {
        write_log(NULL, LOG_ERR, "rd_kafka_new failed with errstr[%s]", errstr);
        return NULL;
    }

    // TODO
    //rd_kafka_set_logger()
    //rd_kafka_set_log_level()

    if (rd_kafka_brokers_add(consumer, brokers) < 1) {
        write_log(NULL, LOG_ERR, "rd_kafka_brokers_add failed for consumer");
        destroy_kafka_consumer(consumer);
        return NULL;
    }

    return consumer;
}

void destroy_kafka_consumer(kafka_consumer_t *consumer)
{
    if (consumer != NULL) {
        rd_kafka_poll(consumer, 2000);
        rd_kafka_destroy(consumer);
        rd_kafka_wait_destroyed(2000);
    }
}

kafka_client_topic_t *set_consumer_topic(kafka_consumer_t *consumer,
                                         const char *topic)
{
    if (topic == NULL || topic[0] == '\0') {
        write_log(NULL, LOG_ERR, "invalid topic for consumer");
        return NULL;
    }

    rd_kafka_topic_conf_t *topic_conf = rd_kafka_topic_conf_new();
    if (topic_conf == NULL) {
        write_log(NULL, LOG_ERR, "rd_kafka_topic_conf_new failed for consumer");
        return NULL;
    }

    char errstr[512];

    for (std::map<std::string, std::string>::iterator it = raw_conf.begin();
         it != raw_conf.end(); ++it) {
        if (rd_kafka_topic_conf_set(topic_conf, it->first.c_str(),
                                    it->second.c_str(), errstr, sizeof(errstr))
            == RD_KAFKA_CONF_INVALID) {
            write_log(NULL, LOG_ERR,
                      "rd_kafka_topic_conf_set failed with errstr[%s]", errstr);
            rd_kafka_topic_conf_destroy(topic_conf);
            return NULL;
        }
    }

    kafka_client_topic_t *kct = rd_kafka_topic_new(consumer, topic, topic_conf);
    if (kct == NULL) {
        write_log(NULL, LOG_ERR, "rd_kafka_topic_new failed"
                  " with errno[%d]", errno);
        return NULL;
    }

    return kct;
}

static void consume_message(kafka_message_t *message, void *opaque)
{
    //int *p = static_cast<int *>(opaque);
}

#define CONSUME_BATCH_SIZE 1000
#define CONSUME_BATCH_TIMEOUT_MS 1000

// TODO offset committing
int consume_messages(kafka_consumer_t *consumer, kafka_client_topic_t *kct,
                     int partition, int64_t offset,
                     void (*consume_cb)(kafka_message_t *message, void *opaque))
{
    kafka_message_t **messages = (kafka_message_t **)malloc(
        CONSUME_BATCH_SIZE * sizeof(kafka_message_t *));
    if (messages == NULL) {
        write_log(NULL, LOG_ERR, "calloc failed for messages to be consumed");
        return -1;
    }

    if (rd_kafka_consume_start(kct, partition, offset) == -1) {
        write_log(NULL, LOG_ERR, "rd_kafka_consume_start failed with errno[%d]",
                  errno);
        free(messages);
        return -1;
    }

    const char *topic = get_topic(kct);
    if (topic == NULL) {
        write_log(NULL, LOG_ERR, "invalid null topic");
        free(messages);
        return -1;
    }

    int num = 0;
    bool continued = true;

    do {
        int n = rd_kafka_consume_batch(kct, partition,
                                       CONSUME_BATCH_TIMEOUT_MS,
                                       messages, CONSUME_BATCH_SIZE);
        if (n == -1) {
            write_log(NULL, LOG_ERR, "rd_kafka_consume_batch failed with"
                      " errno[%d] errstr[%s]",
                      errno, rd_kafka_err2str(rd_kafka_errno2err(errno)));
            continue;
        } else if (n == 0) {
            write_log(NULL, LOG_INFO, "topic[%s]-partition[%d] no new message",
                      topic, partition);
            sleep(15);
            continue;
        }

        int ret = 0;
        for (int i = 0; i < n; i++) {
            switch (messages[i]->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                if (ret == 0) {
                    consume_cb(messages[i], static_cast<void *>(&ret));
#if 0
                    int ec = rd_kafka_offset_store(messages[i]->rkt,
                                                   messages[i]->partition,
                                                   messages[i]->offset);
                    if (ec != RD_KAFKA_RESP_ERR_NO_ERROR) {
                        write_log(NULL, LOG_ERR, "rd_kafka_store_offset failed"
                                  " with errcode[%d]", ec);
                    }
#endif
                    num++;
                } else if (ret == 1) {
                    write_log(NULL, LOG_INFO, "topic[%s]-partition[%d] "
                              "consuming is stopping", topic, partition);
                    continued = false;
                    ret = 2;
                }
                break;
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                write_log(NULL, LOG_INFO, "topic[%s]-partition[%d] end",
                          topic, partition);
                poll_consumer(consumer, 5000);
                break;
            default:
                write_log(NULL, LOG_WARNING,
                          "topic[%s]-partition[%d] error[%d][%s]",
                          topic, partition, messages[i]->err,
                          (char *)messages[i]->payload);
            }
            rd_kafka_message_destroy(messages[i]);
        }

        poll_consumer(consumer, 0);
    } while (continued);

    if (rd_kafka_consume_stop(kct, partition) == -1) {
        write_log(NULL, LOG_ERR, "rd_kafka_consume_stop topic[%s]-partition[%d]"
                  " failed with errno[%d]", topic, partition, errno);
    }

    poll_consumer(consumer, 100);
    free(messages);

    return num;
}

int poll_consumer(kafka_consumer_t *consumer, int milli)
{
    return rd_kafka_poll(consumer, milli);
}

const char *get_topic(const kafka_client_topic_t *kct)
{
    return kct == NULL ? NULL : rd_kafka_topic_name(kct);
}

const char *get_topic_by_message(const kafka_message_t *message)
{
    if (message == NULL || message->rkt == NULL) return NULL;
    return rd_kafka_topic_name(message->rkt);
}
