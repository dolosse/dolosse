Sample MIDAS experiment
=======================

This directory contains an example of a MIDAS experiment. It
contains an even definition for a trigger event (ID 1) and a
scaler event (ID 2).

The frontend program generates both types of events and fills
them with random data. The data is stored in the events in
MIDAS bank format which is very similar to BOS/YBOS banks.

The analyzer uses different "modules" to recover these banks
from the events and analyzes them. For a detailed description
of the analyzer concept please read

http://pibeta.psi.ch/handbook/analyzer

To adapt the sample experiment to a real experiment, following
tasks have to be performed:

1) Run the frontend on a computer which has direct access to
the experiment hardware. Most likely this is a VxWorks machine
or a PC running MS-DOS or Windows NT.

2) Modify your event definition. If you want to send new banks
of variable size from the frontend, just create them in the
frontend readout code. If you want to send new structured
banks (banks with fixed length containing data which can be
described in a C-structure), create these banks with ODBEdit. 
The definition is stored under

  /equipment/trigger/variables
  /equipment/scaler/variables

To create a bank named "ANGL" which contains two values phi
and theta, enter:

[local]/> cd /equipment/trigger/variables
[local]/Equipment/Trigger/Variables> cr key ANGL
[local]/Equipment/Trigger/Variables> cd ANGL
[local]/Equipment/Trigger/Variables/ANGL> cr double phi
[local]/Equipment/Trigger/Variables/ANGL> cr double theta
[local]/Equipment/Trigger/Variables/ANGL> event

The last command propagates the changes to the file names
event.h which then contains the C-structure for the ANGL
bank.

The bank can then be created in the frontend with following
C-statements:

ANGL_BANK *angl;

  bk_create(pevent, "ANGL", TID_STRUCT, &angl);
  angl->phi = 0;
  angl->theta = 0;
  bk_close(pevent, angl+1);

3) Modify the analyzer to suit your needs. Read the analyzer
description (see above) to learn how to add new modules, banks
and parameters.

To run a complete experiment, follow these steps:

1) Run the frontend on your frontend computer.

2) Run the analyzer and the mlogger on the backend computer.

3) Set the data directory in the ODB under /logger/data dir.

4) Set the logging channels under /logger/channels/... If tape
writing is desired, set the logging channel type to "tape" and
the filename to something like /dev/nrmt0h under UNIX or
\\.\tape0 under Windows NT. When writing to disk, files with
the extension .mid are created.

5) Start/stop runs with the ODBEdit "start"/"stop" commands.

6) Check if run00001.mid etc. is created when logging to disk
is turned on.

7) You can offline-analyze the .mid files with the same
analyzer executable:

  analyzer -i run00001.mid -o run00001.rz

to produce directly a N-tuple file which can be loaded inside
PAW with

  PAW> hi/file 1 run00001.rz 8190

