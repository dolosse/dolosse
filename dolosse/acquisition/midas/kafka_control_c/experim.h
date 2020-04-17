/********************************************************************\

  Name:         experim.h
  Created by:   ODBedit program

  Contents:     This file contains C structures for the "Experiment"
                tree in the ODB and the "/Analyzer/Parameters" tree.

                Additionally, it contains the "Settings" subtree for
                all items listed under "/Equipment" as well as their
                event definition.

                It can be used by the frontend and analyzer to work
                with these information.

                All C structures are accompanied with a string represen-
                tation which can be used in the db_create_record function
                to setup an ODB structure which matches the C structure.

  Created on:   Mon Apr 14 15:27:19 2003

\********************************************************************/

#define EXP_PARAM_DEFINED

typedef struct {
   char comment[80];
} EXP_PARAM;

#define EXP_PARAM_STR(_name) char *_name[] = {\
"[.]",\
"Comment = STRING : [80] Test",\
"",\
NULL }

#ifndef EXCL_ADC_CALIBRATION

#define ADC_CALIBRATION_PARAM_DEFINED

typedef struct {
   INT pedestal[8];
   float software_gain[8];
   double histo_threshold;
} ADC_CALIBRATION_PARAM;

#define ADC_CALIBRATION_PARAM_STR(_name) char *_name[] = {\
"[.]",\
"Pedestal = INT[8] :",\
"[0] 174",\
"[1] 194",\
"[2] 176",\
"[3] 182",\
"[4] 185",\
"[5] 215",\
"[6] 202",\
"[7] 202",\
"Software Gain = FLOAT[8] :",\
"[0] 1",\
"[1] 1",\
"[2] 1",\
"[3] 1",\
"[4] 1",\
"[5] 1",\
"[6] 1",\
"[7] 1",\
"Histo threshold = DOUBLE : 20",\
"",\
NULL }

#endif

#ifndef EXCL_ADC_SUMMING

#define ADC_SUMMING_PARAM_DEFINED

typedef struct {
   float adc_threshold;
} ADC_SUMMING_PARAM;

#define ADC_SUMMING_PARAM_STR(_name) char *_name[] = {\
"[.]",\
"ADC threshold = FLOAT : 5",\
"",\
NULL }

#endif

#ifndef EXCL_GLOBAL

#define GLOBAL_PARAM_DEFINED

typedef struct {
   float adc_threshold;
} GLOBAL_PARAM;

#define GLOBAL_PARAM_STR(_name) char *_name[] = {\
"[.]",\
"ADC Threshold = FLOAT : 5",\
"",\
NULL }

#endif

#ifndef EXCL_TRIGGER

#define ASUM_BANK_DEFINED

typedef struct {
   float sum;
   float average;
} ASUM_BANK;

#define ASUM_BANK_STR(_name) char *_name[] = {\
"[.]",\
"Sum = FLOAT : 0",\
"Average = FLOAT : 0",\
"",\
NULL }

#define TRIGGER_COMMON_DEFINED

typedef struct {
   WORD event_id;
   WORD trigger_mask;
   char buffer[32];
   INT type;
   INT source;
   char format[8];
   BOOL enabled;
   INT read_on;
   INT period;
   double event_limit;
   DWORD num_subevents;
   INT log_history;
   char frontend_host[32];
   char frontend_name[32];
   char frontend_file_name[256];
} TRIGGER_COMMON;

#define TRIGGER_COMMON_STR(_name) char *_name[] = {\
"[.]",\
"Event ID = WORD : 1",\
"Trigger mask = WORD : 0",\
"Buffer = STRING : [32] SYSTEM",\
"Type = INT : 2",\
"Source = INT : 16777215",\
"Format = STRING : [8] MIDAS",\
"Enabled = BOOL : y",\
"Read on = INT : 257",\
"Period = INT : 500",\
"Event limit = DOUBLE : 0",\
"Num subevents = DWORD : 0",\
"Log history = INT : 0",\
"Frontend host = STRING : [32] pc810",\
"Frontend name = STRING : [32] Sample Frontend",\
"Frontend file name = STRING : [256] C:\Midas\examples\experiment\frontend.c",\
"",\
NULL }

#define TRIGGER_SETTINGS_DEFINED

typedef struct {
   BYTE io506;
} TRIGGER_SETTINGS;

#define TRIGGER_SETTINGS_STR(_name) char *_name[] = {\
"[.]",\
"IO506 = BYTE : 7",\
"",\
NULL }

#endif

#ifndef EXCL_SCALER

#define SCALER_COMMON_DEFINED

typedef struct {
   WORD event_id;
   WORD trigger_mask;
   char buffer[32];
   INT type;
   INT source;
   char format[8];
   BOOL enabled;
   INT read_on;
   INT period;
   double event_limit;
   DWORD num_subevents;
   INT log_history;
   char frontend_host[32];
   char frontend_name[32];
   char frontend_file_name[256];
} SCALER_COMMON;

#define SCALER_COMMON_STR(_name) char *_name[] = {\
"[.]",\
"Event ID = WORD : 2",\
"Trigger mask = WORD : 0",\
"Buffer = STRING : [32] SYSTEM",\
"Type = INT : 17",\
"Source = INT : 0",\
"Format = STRING : [8] MIDAS",\
"Enabled = BOOL : y",\
"Read on = INT : 377",\
"Period = INT : 10000",\
"Event limit = DOUBLE : 0",\
"Num subevents = DWORD : 0",\
"Log history = INT : 0",\
"Frontend host = STRING : [32] pc810",\
"Frontend name = STRING : [32] Sample Frontend",\
"Frontend file name = STRING : [256] C:\Midas\examples\experiment\frontend.c",\
"",\
NULL }

#endif
