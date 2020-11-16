# [Dolosse](https://dolosse.org) - A Kafka based Data Acquisition System
[![master](https://travis-ci.org/dolosse/dolosse.svg?branch=master)](https://travis-ci.org/dolosse/dolosse)
[![codecov](https://codecov.io/gh/dolosse/dolosse/branch/master/graph/badge.svg)](https://codecov.io/gh/dolosse/dolosse)

Copyright (c) 2018 - 2020, [S. V. Paulauskas](https://projectscience.tech), 
[Dolosse Collaboration](https://dolosse.org).

Dolosse is a modern scientific data acquisition and analysis framework. We use Kafka as a messaging backend that allows 
synchronization between acquisition systems. Using Kafka's messaging interface we can create a feedback loops between 
analysis and acquisition systems. We're still working on developing the framework

# Features
* Digitizer interfaces (coming soon)
   * XIA 
       * Pixie-16
       * XIA Pixie-Net
* Decode digitizer data formats
   * XIA
       * Pixie-16 (released)
       * Pixie-4 and Pixie-Net (coming soon)
* Runs on Linux and Windows Systems. 
* Unpacks binary data formats:
    * PLD (UTK)
    * LDF (ORNL) - coming soon

# Documentation
Our documentation is sparse right now since we're still getting off the ground. You can read about 
the project motivation in [this presentation](https://dolosse.org/modernizing-nuclear-physics-data-processing/).
You can also checkout the [wiki](https://github.com/dolosse/dolosse/wiki). 

## Support
Checkout the [support](https://github.com/dolosse/dolosse/blob/master/.github/SUPPORT.md) page for more information. 

# Installation
There are a couple of ways to use the software. They depend on the goal that you're after. 

## For use in custom software 
```bash
pip install dolosse
```
Now you'll have access to the packages and modules we use to build our components. Checkout the 
modules documentation to see what you can do out of the box. 
 
*NOTE:* There's nothing that's ready for experiments just yet.

## For development
Developers should read the [development workflow](https://github.com/dolosse/dolosse/wiki/Development-Workflow).
```bash
git clone https://github.com/dolosse/dolosse.git
cd dolosse
python3 -m venv venv
pip install -r requirements.txt
```

# Licensing
[Apache License 2.0.](https://www.apache.org/licenses/LICENSE-2.0.html) 

