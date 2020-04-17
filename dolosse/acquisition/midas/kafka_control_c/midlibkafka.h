#ifndef __MIDLIBKAFKA_H__
#define __MIDLIBKAFKA_H__

#include <stdio.h>
#include <stdlib.h>
#include <librdkafka/rdkafka.h>
#include <string.h>
#include <json-c/json.h>

char* json_build_feedback(char **run_state, double *evt_rate, double *kB_rate, double *events, char **feedback);
char* json_build_errors(char **error);
void produce_kafka_msg(rd_kafka_t *rk_p, rd_kafka_topic_t *rkt, char * msg);
void kafka_shutdown(rd_kafka_t *rk, rd_kafka_t *rk_p, rd_kafka_topic_t *rkt_e, rd_kafka_topic_t *rkt_f);

#endif