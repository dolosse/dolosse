/*

This is a template file for a standard MIDAS - PAW analyzer module.

Replace all <template> tags with the name of the module (like 
"adccalib" or "scaler") and all <TEMPLATE> tags with the uppercase
name of them.

It is assumed that the module uses parameters <template>_param, which
appear in the ODB under /Analyzer/Parameters/<template>, a init 
routine which books histos and an event routine. The event routine
looks for a bank "XXXX" and creates a result bank "YYYY" (have to be 
replaced with meaningful names).

It is assumed that the result bank is a structured bank. Before it
can be used, it has to be defined in the ODB with

> cd /Equipment/Trigger/Variables
> mkdir <YYYY>
> cd <YYYY>
> create float <item a>
> create int <item b>
> ....
> make

The "make" command generates <YYYY>_BANK in the file "experim.h", 
which is included in all module files.

The same has to be done with the module parameters:

> cd /Analyzer/Parameters
> mkdir <template>
> cd <template>
> create float <parameter a>
> create int <parameter b>
> ....
> make

The success of the operation can be checked by looking at the
"experim.h" file.

*/

/********************************************************************\

  Name:         <template>.c
  Created by:   Stefan Ritt

  Contents:     PiBeta Analyzer module for <template>

  Revision history
  ------------------------------------------------------------------
  date         by    modification
  ---------    ---   ------------------------------------------------
  <date>       <XX>  created

\********************************************************************/

/*-- Include files -------------------------------------------------*/

/* standard includes */
#include <stdio.h>

/* midas includes */
#include "midas.h"
#include "exprim.h"

/*-- Parameters ----------------------------------------------------*/

< TEMPLATE > _PARAM < template > _param;

/*-- Static parameters ---------------------------------------------*/


/*-- Module declaration --------------------------------------------*/

INT < template > (EVENT_HEADER *, void *);
INT < template > _init(void);

<TEMPLATE > _PARAM_STR(<template > _param_str);

ANA_MODULE < template > _module = {
   "<template>",                /* module name           */
       "<Your Name>",           /* author                */
       <template >,             /* event routine         */
       NULL,                    /* BOR routine           */
       NULL,                    /* EOR routine           */
       <template > _init,       /* init routine          */
       NULL,                    /* exit routine          */
       &<template > _param,     /* parameter structure   */
       sizeof(<template > _param),      /* structure size        */
       <template > _param_str,  /* initial parameters    */
};

/*-- init routine --------------------------------------------------*/

INT < template > _init(void)
{
   /* book histos */

   return SUCCESS;
}

/*-- event routine -------------------------------------------------*/

INT < template > (EVENT_HEADER * pheader, void *pevent) {
   int n;
   XXXX_BANK *xxxx;
   YYYY_BANK *yyyy;

   /* look for <XXXX> bank, return if not present */
   n = bk_locate(pevent, "XXXX", &xxxx);
   if (n == 0)
      return SUCCESS;

   /* create calculated bank */
   bk_create(pevent, "YYYY", TID_STRUCT, &yyyy);

  /*---- perform calculations --------------------------------------*/

   yyyy->x = xxxx->x * 2;

  /*---- close calculated bank -------------------------------------*/
   bk_close(pevent, yyyy + 1);

   return SUCCESS;
}
