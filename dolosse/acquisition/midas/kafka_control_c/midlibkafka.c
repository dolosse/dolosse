#include <midlibkafka.h>

char* json_build_feedback(char **run_state, double *evt_rate, double *kB_rate, double *events, char **feedback)
{
    /*Creating a json object*/
    json_object * jobj = json_object_new_object();
    json_object * jobj_2 = json_object_new_object();

    json_object *jstring_cat = json_object_new_string("feedback");
    json_object *jstring_os = json_object_new_string("test feedback");

    json_object *jstring_state = json_object_new_string(*run_state);
    json_object *jdouble_evt_rate = json_object_new_double(*evt_rate);
    json_object *jdouble_kB_rate = json_object_new_double(*kB_rate);
    json_object *jdouble_events = json_object_new_double(*events);

    /*Form the json object*/
    json_object_object_add(jobj_2,"run_state", jstring_state);
    json_object_object_add(jobj_2,"evt_rate", jdouble_evt_rate);
    json_object_object_add(jobj_2,"kB_rate", jdouble_kB_rate);
    json_object_object_add(jobj_2,"events", jdouble_events);


    json_object_object_add(jobj,"category", jstring_cat);
    json_object_object_add(jobj,"daq", jobj_2);
    json_object_object_add(jobj,"os", jstring_os);

    return json_object_to_json_string(jobj);
}

char* json_build_errors(char **error)
{
    /*Creating a json object*/
    json_object * jobj = json_object_new_object();

    json_object *jstring_cat = json_object_new_string("errors");
    json_object *jstring_err = json_object_new_string(*error);

    /*Form the json object*/
    json_object_object_add(jobj,"category", jstring_cat);
    json_object_object_add(jobj,"msg", jstring_err);

    return json_object_to_json_string(jobj);
}

void produce_kafka_msg(rd_kafka_t *rk_p, rd_kafka_topic_t *rkt, char *msg)
{
    if(rd_kafka_produce(rkt, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,  msg, strlen(msg), NULL, 0, NULL) == -1 )
            {
                fprintf(stderr, "%%Failed to produce top topic %s: %s\n", rd_kafka_topic_name, rd_kafka_err2str(rd_kafka_last_error()));
                if (rd_kafka_last_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL)
                rd_kafka_poll(rk_p, 1000);
            }
            else
            {
                /* fprintf(stderr, "%%Enqueued message (%zd bytes""for topic %s\n", strlen(msg), rd_kafka_topic_name(rkt)); */
            }
            rd_kafka_poll(rk_p, 0);   /* non-blocking */
}

void kafka_shutdown(rd_kafka_t *rk, rd_kafka_t *rk_p, rd_kafka_topic_t *rkt_e, rd_kafka_topic_t *rkt_f)
{
    /* Close the consumer: commit final offsets and leave the group. */
    fprintf(stderr, "%% Closing consumer\n");
    rd_kafka_consumer_close(rk);

    /* Destroy the consumer */
    rd_kafka_destroy(rk);

    /* Wait for final messages to be delivered or fail*/

    fprintf(stderr, "%% Flushing final messages..\n");

    rd_kafka_flush(rk_p, 10*1000 /* wait for max 10 seconds */);

    /* Destroy topic objects */
    rd_kafka_topic_destroy(rkt_e);
    rd_kafka_topic_destroy(rkt_f);

    /* Destroy the producer instance */
    rd_kafka_destroy(rk_p);
}