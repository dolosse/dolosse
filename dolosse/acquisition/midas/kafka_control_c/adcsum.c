/********************************************************************\

  Name:         adcsum.c
  Created by:   Stefan Ritt

  Contents:     Example analyzer module for ADC summing. This module
                looks for a bank named CADC and produces an ASUM
                bank which contains the sum of all ADC values. The
                ASUM bank is a "structured" bank. It has been defined
                in the ODB and transferred to experim.h.

  $Id:$

\********************************************************************/

/*-- Include files -------------------------------------------------*/

/* standard includes */
#include <stdio.h>
#include <math.h>

#define DEFINE_TESTS // must be defined prior to midas.h

/* midas includes */
#include "midas.h"
#include "rmidas.h"
#include "experim.h"
#include "analyzer.h"

/* root includes */
#include <TH1D.h>

#ifndef PI
#define PI 3.14159265359
#endif

/*-- Parameters ----------------------------------------------------*/

ADC_SUMMING_PARAM adc_summing_param;

/*-- Tests ---------------------------------------------------------*/

DEF_TEST(low_sum);
DEF_TEST(high_sum);

/*-- Module declaration --------------------------------------------*/

INT adc_summing(EVENT_HEADER *, void *);
INT adc_summing_init(void);
INT adc_summing_bor(INT run_number);

ADC_SUMMING_PARAM_STR(adc_summing_param_str);

ANA_MODULE adc_summing_module = {
   "ADC summing",               /* module name           */
   "Stefan Ritt",               /* author                */
   adc_summing,                 /* event routine         */
   NULL,                        /* BOR routine           */
   NULL,                        /* EOR routine           */
   adc_summing_init,            /* init routine          */
   NULL,                        /* exit routine          */
   &adc_summing_param,          /* parameter structure   */
   sizeof(adc_summing_param),   /* structure size        */
   adc_summing_param_str,       /* initial parameters    */
};

/*-- Module-local variables-----------------------------------------*/

static TH1D *hAdcSum, *hAdcAvg;

/*-- init routine --------------------------------------------------*/

INT adc_summing_init(void)
{
   /* book ADC sum histo */
   hAdcSum = h1_book<TH1D>("ADCSUM", "ADC sum", 500, 0, 10000);

   /* book ADC average in separate subfolder */
   open_subfolder("Average");
   hAdcAvg = h1_book<TH1D>("ADCAVG", "ADC average", 500000, 0, 10000);
   close_subfolder();

   return SUCCESS;
}

/*-- event routine -------------------------------------------------*/

INT adc_summing(EVENT_HEADER * pheader, void *pevent)
{
   INT i, j, n_adc;
   float *cadc;
   ASUM_BANK *asum;

   /* look for CADC bank, return if not present */
   n_adc = bk_locate(pevent, "CADC", &cadc);
   if (n_adc == 0)
      return 1;

   /* create ADC sum bank */
   bk_create(pevent, "ASUM", TID_STRUCT, (void**)&asum);

   /* sum all channels above threashold */
   asum->sum = 0.f;
   for (i = j = 0; i < n_adc; i++)
      if (cadc[i] > adc_summing_param.adc_threshold) {
         asum->sum += cadc[i];
         j++;
      }

   /* calculate ADC average */
   asum->average = j > 0 ? asum->sum / j : 0;

   /* evaluate tests */
   SET_TEST(low_sum, asum->sum < 1000);
   SET_TEST(high_sum, asum->sum > 1000);

   /* fill sum histo */
   hAdcSum->Fill(asum->sum, 1);

   /* fill average histo */
   hAdcAvg->Fill(asum->average);

   /* close calculated bank */
   bk_close(pevent, asum + 1);

   return SUCCESS;
}
