#ifndef _PyKafkaClient_H
#define _PyKafkaClient_H

#ifdef __cplusplus
extern "C" {
#endif
#include "rdkafka.h"
#ifdef __cplusplus
}
#endif

#include <string>
#include <vector>

typedef rd_kafka_conf_t kafka_client_conf_t;
typedef rd_kafka_t kafka_producer_t;
typedef rd_kafka_t kafka_consumer_t;
typedef rd_kafka_topic_t kafka_client_topic_t;
typedef rd_kafka_message_t kafka_message_t;

#define DEFAULT_CONF_PATH ""
#define KAFKA_PARTITION_UA RD_KAFKA_PARTITION_UA

enum {
    KAFKA_OFFSET_BEGINNING = RD_KAFKA_OFFSET_BEGINNING,
    KAFKA_OFFSET_END = RD_KAFKA_OFFSET_END,
    KAFKA_OFFSET_STORED = RD_KAFKA_OFFSET_STORED,
    KAFKA_OFFSET_TAIL_BASE = RD_KAFKA_OFFSET_TAIL_BASE
};

#define KAFKA_OFFSET_TAIL(n) (KAFKA_OFFSET_TAIL_BASE - (n))
#define KAFKA_OFFSET_HEAD(n) (n)

enum {
    KAFKA_PARTITIONER_RANDOM = 1,
    KAFKA_PARTITIONER_CONSISTENT
};

void write_log(const char *path, int level, const char *fmt, ...);

kafka_producer_t *create_kafka_producer(const char *conf_path,
                                        const char *brokers = NULL);

void destroy_kafka_producer(kafka_producer_t *producer);

kafka_client_topic_t *set_producer_topic(kafka_producer_t *producer,
                                         const char *topic,
                                         int partitioner_type = 1);

int poll_producer(kafka_producer_t *producer, int milli,
                  int how, int qs = 10000);

int produce_messages(kafka_producer_t *producer,
                     kafka_client_topic_t *kct,
                     const std::vector<std::string>& payloads,
                     const std::vector<std::string>& keys);

kafka_consumer_t *create_kafka_consumer(const char *conf_path,
                                        const char *brokers = NULL);

void destroy_kafka_consumer(kafka_consumer_t *consumer);

kafka_client_topic_t *set_consumer_topic(kafka_consumer_t *consumer,
                                         const char *topic);

int consume_messages(kafka_consumer_t *consumer, kafka_client_topic_t *kct,
                     int partition, int64_t offset,
                    void (*consume_cb)(kafka_message_t *message, void *opaque));

int poll_consumer(kafka_consumer_t *consumer, int milli);

void del_topic(kafka_client_topic_t *kct);

const char *get_topic(const kafka_client_topic_t *kct);
const char *get_topic_by_message(const kafka_message_t *message);

#endif
