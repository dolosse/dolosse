/********************************************************************\

Name:         kafka_control.c
Author:       Caswell Pieters

Contents:     A control process that reads out kafka topics (control) and executes
actions based on the message

Created:        17 March 2020


\********************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <librdkafka/rdkafka.h>
#include <string.h>
#include <signal.h>
#include "midas.h"
#include "mcstd.h"
#include "experim.h"
#include <json-c/json.h>
#include <yaml.h>
#include <assert.h>
#include "midlibkafka.h"

#ifdef __cplusplus
extern "C" {
#endif


const int true = 1;
const int false = 0;

/* function declarations */
INT control_init(void);
static int is_printable(const char *buf, size_t size);
INT command_execute(const char *command, int new_run_number);
INT kafka_consumer_setup();
INT kafka_producer_setup();
INT midas_info(char *mid_equip, char **run_state, double *evt_rate, double *kB_rate,
                double *events, char **error, char **feedback);
INT yaml_config_read();

/* experiment database handle*/
HNDLE hDB;

static volatile sig_atomic_t run = 1;
/*brief Signal termination of program*/

static void stop_prog (int sig) {
    run = 0;
}
/* json objects*/
struct json_object *parsed_json;
struct json_object *category;
struct json_object *run_struct;
struct json_object *action;
struct json_object *run_number;
struct json_object *date;


/* Kafka variables */
rd_kafka_t *rk;                                 /* Consumer instance handle */
rd_kafka_t *rk_p;                               /* Producer instance handle */
rd_kafka_topic_t *rkt_e;                        /*Error Topic object */
rd_kafka_topic_t *rkt_f;                        /*FeedbackTopic object */
rd_kafka_conf_t *conf;                          /* Temporary configuration object */
rd_kafka_resp_err_t err;                        /* librdkafka API error code */
char errstr[512];                               /* librdkafka API error reporting buffer */
const char *brokers;                            /* Argument: broker list */
const char *groupid;                            /* Argument: Consumer group id */
char **topics;                                  /* Argument: list of topics to subscribe to */
int topic_cnt;                                  /* Number of topics to subscribe to */
rd_kafka_topic_partition_list_t *subscription;  /* Subscribed topics */
char *expt_name;                                /* Experiment name */
char *expt_host;                                /* Experiment host name */
char *topic_control;                            /* Control topic */
char *topic_manage;                             /* Management topic */
const char *topic_errors;                       /* Errors topic */
const char *topic_feedback;                     /* Feedback topic */

/* kafka per message callback */
static void dr_msg_cb (rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) 
{
    if (rkmessage->err)
        fprintf(stderr, "%% Message delivery failed: %s\n", rd_kafka_err2str(rkmessage->err));
        else
        {
        fprintf(stderr, "%% Message delivered (%zd bytes, ""partition %"PRId32")\n", rkmessage->len, rkmessage->partition);
        }
}

int main()
{
    /*odb information variables*/
    double evt_rate, kB_rate, events ;
    char *run_state, *error, *feedback;
    char * mid_equip = "CAMACTrigger0";

    /* Signal handler for clean shutdown */
    signal(SIGINT, stop_prog);

    if (control_init() == SUCCESS)
    {
        while (run) 
        {
            /*get some odb status updates*/
            if (midas_info(mid_equip, &run_state, &evt_rate, &kB_rate, &events, &error, &feedback) != CM_SUCCESS)
            {
                cm_msg(MERROR, "main" , "Didn't get odb info" );
                return 1;
            }

            char * feedback_msg = json_build_feedback(&run_state, &evt_rate, &kB_rate, &events, &feedback);
            char *error_msg = json_build_errors(&error);

            /* send messages to kafka */
            produce_kafka_msg(rk_p, rkt_e, error_msg);
            produce_kafka_msg(rk_p, rkt_f, feedback_msg);

            /* Now for the consumer */
            rd_kafka_message_t *rkm;

            rkm = rd_kafka_consumer_poll(rk, 100);
            if (!rkm)
                continue; 
            if (rkm->err) {
                fprintf(stderr, "%% Consumer error: %s\n", rd_kafka_message_errstr(rkm));
                rd_kafka_message_destroy(rkm);
                continue;
            }

            /* Proper message. */
            printf("Message on %s [%"PRId32"] at offset %"PRId64":\n", rd_kafka_topic_name(rkm->rkt), rkm->partition, rkm->offset);

            /* Print the message key. */
            if (rkm->key && is_printable(rkm->key, rkm->key_len))
                printf(" Key: %.*s\n", (int)rkm->key_len, (const char *)rkm->key);
            else if (rkm->key)
                printf(" Key: (%d bytes)\n", (int)rkm->key_len);

            /* Print the message value/payload. */
            if (rkm->payload && is_printable(rkm->payload, rkm->len))
            {
                /* parse json */
                parsed_json = json_tokener_parse((const char*)rkm->payload);
                json_object_object_get_ex(parsed_json, "category", &category);
                json_object_object_get_ex(parsed_json, "run", &run_struct);
                json_object_object_get_ex(run_struct, "action", &action);
                json_object_object_get_ex(run_struct, "run_number", &run_number);

                printf("Category: %s\n", json_object_get_string(category));
                printf("Run: %s\n", json_object_get_string(run_struct));
                printf("Action: %s\n", json_object_get_string(action));
                printf("Run number: %d\n", json_object_get_int(run_number));

                command_execute(json_object_get_string(action), json_object_get_int(run_number));
            }
            else if (rkm->key)
            {
                printf(" Value: (%d bytes)\n", (int)rkm->len);
            }

            rd_kafka_message_destroy(rkm);

            sleep(1);
        }

        kafka_shutdown(rk, rk_p, rkt_e, rkt_f);
        cm_disconnect_experiment();

        return 0;
    }
    else
    {
        cm_msg(MERROR, "main" , "Init failed" );
        return 1;
    }
}

/*-- Frontend Init -------------------------------------------------*/

INT control_init()
{
    /* read yaml config file */
    if (yaml_config_read() != CM_SUCCESS)
    {
        cm_msg(MERROR, "kafka_consumer_setup" , "Could not read yaml config" );
        return 1;
    }

    /* connect to experiment */
    int status = cm_connect_experiment(expt_host, expt_name, "Kafka_Control", NULL);
    if (status != CM_SUCCESS)
        return 1;

    /* get handle to the ODB of the currently connected experiment */
    if (cm_get_experiment_database(&hDB,NULL) != CM_SUCCESS)
    {
        cm_msg(MERROR, "control_init" , "Didn't get database handle" );
        return 1;
    }

    /* set MIDAS watchdog parameters */
    if (cm_set_watchdog_params (true,  0) != CM_SUCCESS)
    {
        cm_msg(MERROR, "control_init" , "Could not set watchdog parameters" );
        return 1;
    }

    /* --------------Kafka consumer setup --------------------*/
    if (kafka_consumer_setup () != CM_SUCCESS)
    {
        cm_msg(MERROR, "control_init" , "Could not create consumer" );
        return 1;
    }

    /* --------------Kafka producer setup --------------------*/
    if (kafka_producer_setup () != CM_SUCCESS)
    {
        cm_msg(MERROR, "control_init" , "Could not create producer" );
        return 1;
    }

    cm_msg(MINFO,"control_init", "Control init run successfully");

    return SUCCESS;
}

/*returns 1 if all bytes are printable, else 0. */
static int is_printable (const char *buf, size_t size) 
{
    size_t i;

    for (i = 0 ; i < size ; i++)
        if (!isprint((int)buf[i]))
            return 0;
    return 1;
}

/* Execute the command from the topic message */
INT command_execute(const char* command, int new_run_number)
{
    int  midas_command, debug_flag = 1;
    char *str;

    if (strcmp (command, "Start") == 0)
        midas_command = TR_START;
    if (strcmp (command, "Stop") == 0)
        midas_command = TR_STOP;
    if (strcmp (command, "Pause") == 0)
        midas_command = TR_PAUSE;
    if (strcmp (command, "Resume") == 0)
        midas_command = TR_RESUME;

    printf("command%s\n:", command);

    if ( cm_transition(midas_command, new_run_number, str, sizeof(str), 0, debug_flag) != CM_SUCCESS)
    {
            // in case of error
            printf("Error: %s\n", str);
    }
    return SUCCESS;
}

INT kafka_consumer_setup()
{
    /* specify kafka config options */
    groupid = "rbs_control";
    topics = &topic_control;
    topic_cnt = 1;
    conf = rd_kafka_conf_new();
    int i;


    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return 1;
    }

    if (rd_kafka_conf_set(conf, "group.id", groupid, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return 1;
    }

    if (rd_kafka_conf_set(conf, "auto.offset.reset", "latest", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return 1;
    }

    rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!rk)
    {
        fprintf(stderr, "%% Failed to create new consumer: %s\n", errstr);
        return 1;
    }

    conf = NULL;
    /* Configuration object is now owned, and freed by the rd_kafka_t instance. */

    /* Convert the list of topics to a format suitable for librdkafka */
    subscription = rd_kafka_topic_partition_list_new(topic_cnt);
    for (i = 0 ; i < topic_cnt ; i++)
        rd_kafka_topic_partition_list_add(subscription, topics[i], RD_KAFKA_PARTITION_UA);

    /* Subscribe to the list of topics */
    err = rd_kafka_subscribe(rk, subscription);
    if (err) {
        fprintf(stderr, "%% Failed to subscribe to %d topics: %s\n",subscription->cnt, rd_kafka_err2str(err));
        rd_kafka_topic_partition_list_destroy(subscription);
        rd_kafka_destroy(rk);
        return 1;
    }

    fprintf(stderr, "%% Subscribed to %d topic(s), ""waiting for rebalance and messages...\n", subscription->cnt);

    rd_kafka_topic_partition_list_destroy(subscription);

    return SUCCESS;
}

INT kafka_producer_setup()
{
    groupid = "rbs_feedback";

    /* Create Kafka client configuration place-holder*/
    conf = rd_kafka_conf_new();

    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        return 1;
    }

    /* set the group id of the client*/
    if (rd_kafka_conf_set(conf, "group.id", groupid, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        return 1;
    }

    /* Set the delivery report callback */
    rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

    /* Create producer instance*/

    rk_p = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk_p)
    {
        fprintf(stderr, "%% Failed to create new producer: %s\n", errstr);
        return 1;
    }

    /* Create topic objects that will be reused for each message*/
    /*errors*/
    rkt_e = rd_kafka_topic_new(rk_p, topic_errors, NULL);
    if (!rkt_e)
    {
        fprintf(stderr, "%% Failed to create topic object: %s\n", rd_kafka_err2str(rd_kafka_last_error()));
        rd_kafka_destroy(rk_p);
        return 1;
    }

    /*feedback*/
    rkt_f = rd_kafka_topic_new(rk_p, topic_feedback, NULL);
    if (!rkt_f)
    {
        fprintf(stderr, "%% Failed to create topic object: %s\n", rd_kafka_err2str(rd_kafka_last_error()));
        rd_kafka_destroy(rk_p);
        return 1;
    }

    return SUCCESS;
}

INT midas_info(char *mid_equip, char **run_state, double *evt_rate, double *kB_rate,
                 double *events, char **error, char **feedback)
{
    INT size, odb_run_state, odb_run_number;
    double odb_events, odb_evt_rate, odb_kB_rate;
    char event_rate_path[60], kB_rate_path[60], events_path[60];
    
    size=sizeof(odb_run_state);
    db_get_value(hDB,0,"/Runinfo/State",
                &odb_run_state,&size,TID_INT,0);
    
    if (odb_run_state == 1)
        *run_state = "Stopped";
    else if (odb_run_state == 2)
        *run_state = "Paused";
    else if (odb_run_state == 3)
        *run_state = "Running";
    else
        *run_state = "Unknown";

    size=sizeof(odb_run_number);
    db_get_value(hDB,0,"/Runinfo/Run number",
                &odb_run_number,&size,TID_INT,0);

    sprintf(event_rate_path, "/Equipment/%s/Statistics/Events per sec.", mid_equip);
    sprintf(kB_rate_path, "/Equipment/%s/Statistics/kBytes per sec.", mid_equip);
    sprintf(events_path, "/Equipment/%s/Events sent", mid_equip);


    size=sizeof(odb_events);
    db_get_value(hDB,0,events_path,
                &odb_events,&size,TID_DOUBLE,0);
    *events = odb_events;

    size=sizeof(odb_evt_rate);
    db_get_value(hDB,0,event_rate_path,
                &odb_evt_rate,&size,TID_DOUBLE,0);
    *evt_rate = odb_evt_rate;

    size=sizeof(odb_kB_rate);
    db_get_value(hDB,0,kB_rate_path,
                &odb_kB_rate,&size,TID_DOUBLE,0);
    *kB_rate = odb_kB_rate;

    *error = "OBD test error";
    *feedback = "ODB test feedback";

    return SUCCESS;
}

INT yaml_config_read()
{
    FILE *file;
    yaml_parser_t parser;
    yaml_event_t event;
    int done = 0;
    int error = 0;
    char value[25];
    int bootstrap_found = 0, host_found = 0, expt_found =0, control_found = 0, manage_found = 0,
        errors_found = 0, feedback_found = 0;

    file = fopen("midas_kafka_control.yaml", "rb");
    assert(file);

    assert(yaml_parser_initialize(&parser));

    yaml_parser_set_input_file(&parser, file);
    
    while (!done)
    {
        if (!yaml_parser_parse(&parser, &event)) {
            error = 1;
            break;
        }

        if (event.type == YAML_SCALAR_EVENT)
        {
            sprintf(value, "%s", event.data.scalar.value);
            if (bootstrap_found == 1)
            {
                brokers = (char *)malloc(strlen(value)+1);
                strcpy(brokers,value);
                printf("broker value is: %s\n", brokers);
                bootstrap_found++;
            }
            if (host_found == 1)
            {
                expt_host = (char *)malloc(strlen(value)+1);
                strcpy(expt_host,value);
                printf("expt_host value is: %s\n", expt_host);
                host_found++;
            }
            if (expt_found == 1)
            {
                expt_name = (char *)malloc(strlen(value)+1);
                strcpy(expt_name,value);
                printf("expt_name value is: %s\n", expt_name);
                expt_found++;
            }
            if (control_found == 1)
            {
                topic_control = (char *)malloc(strlen(value)+1);
                strcpy(topic_control,value);
                printf("control value is: %s\n", topic_control);
                control_found++;
            }
            if (manage_found == 1)
            {
                topic_manage = (char *)malloc(strlen(value)+1);
                strcpy(topic_manage,value);
                printf("manage value is: %s\n", topic_manage);
                manage_found++;
            }
            if (errors_found == 1)
            {
                topic_errors = (char *)malloc(strlen(value)+1);
                strcpy(topic_errors,value);
                printf("error value is: %s\n", topic_errors);
                errors_found++;
            }
            if (feedback_found == 1)
            {
                topic_feedback = (char *)malloc(strlen(value)+1);
                strcpy(topic_feedback,value);
                printf("feedback value is: %s\n", topic_feedback);
                feedback_found++;
            }

            /* find kafka broker and topics, experiment name, and host name */
            if (strcmp(value,"bootstrap_servers") == 0)
                bootstrap_found++;
            if (strcmp(value,"expt_host") == 0)
                host_found++;
            if (strcmp(value,"expt_name") == 0)
                expt_found++;
            if (strcmp(value,"control") == 0)
                control_found++;
            if (strcmp(value,"manage") == 0)
                manage_found++;
            if (strcmp(value,"errors") == 0)
                errors_found++;
            if (strcmp(value,"feedback") == 0)
                feedback_found++;
        }

        done = (event.type == YAML_STREAM_END_EVENT);

        yaml_event_delete(&event);
    }

    yaml_parser_delete(&parser);

    assert(!fclose(file));

    return SUCCESS;
}