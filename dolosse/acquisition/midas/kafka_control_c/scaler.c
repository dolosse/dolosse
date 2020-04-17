/********************************************************************\

  Name:         scaler.c
  Created by:   Stefan Ritt

  Contents:     Example scaler analyzer module. This module looks
                for a SCLR banks and accumulates scalers into an
                ACUM bank.

  $Id:$

\********************************************************************/

/*-- Include files -------------------------------------------------*/

/* standard includes */
#include <stdio.h>
#include <string.h>
#include <time.h>

/* midas includes */
#include "midas.h"
#include "experim.h"
#include "analyzer.h"

/*-- Module declaration --------------------------------------------*/

INT scaler_accum(EVENT_HEADER *, void *);
INT scaler_clear(INT run_number);
INT scaler_eor(INT run_number);

ANA_MODULE scaler_accum_module = {
   "Scaler accumulation",       /* module name           */
   "Stefan Ritt",               /* author                */
   scaler_accum,                /* event routine         */
   scaler_clear,                /* BOR routine           */
   scaler_eor,                  /* EOR routine           */
   NULL,                        /* init routine          */
   NULL,                        /* exit routine          */
   NULL,                        /* parameter structure   */
   0,                           /* structure size        */
   NULL,                        /* initial parameters    */
};

/*-- accumulated scalers -------------------------------------------*/

double scaler[32];

/*-- BOR routine ---------------------------------------------------*/

INT scaler_clear(INT run_number)
{
   memset(scaler, 0, sizeof(scaler));
   return SUCCESS;
}

/*-- EOR routine ---------------------------------------------------*/

INT scaler_eor(INT run_number)
{
   return SUCCESS;
}

/*-- event routine -------------------------------------------------*/

INT scaler_accum(EVENT_HEADER * pheader, void *pevent)
{
   INT n, i;
   DWORD *psclr;
   double *pacum;

   /* look for SCLR bank */
   n = bk_locate(pevent, "SCLR", &psclr);
   if (n == 0)
      return 1;

   /* create acummulated scaler bank */
   bk_create(pevent, "ACUM", TID_DOUBLE, (void**)&pacum);

   /* accumulate scalers */
   for (i = 0; i < n; i++) {
      scaler[i] += psclr[i];
      pacum[i] = scaler[i];
   }

   /* close bank */
   bk_close(pevent, pacum + n);

   return SUCCESS;
}
