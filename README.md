# [Dolosse](https://dolosse.org) - A Kafka based Data Acquisition System
[![master](https://travis-ci.org/dolosse/dolosse.svg?branch=master)](https://travis-ci.org/dolosse/dolosse)

Copyright (c) 2018 - 2019, [S. V. Paulauskas](https://projectscience.tech), 
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
    * LDF (ORNL)
    * PLD (UTK)
    * EVT (coming soon) 

# Documentation
Our documentation is sparse right now since we're still getting off the ground. You can read about 
the project motivation in [this presentation](https://dolosse.org/modernizing-nuclear-physics-data-processing/).
You can also checkout the [wiki](https://github.com/dolosse/dolosse/wiki). 

## Support
Checkout the [support] (https://github.com/dolosse/dolosse/blob/master/.github/SUPPORT.md) page for more information. 

# Installation
There are a couple of ways to use the software. They depend on the goal that you're after. 
## For development
Developers should read the [development workflow](https://github.com/dolosse/dolosse/wiki/Development-Workflow).
```bash
git clone https://github.com/dolosse/dolosse.git
cd dolosse
python3 -m venv venv
pip install -r requirements.txt
```
## For use in experiments
Similar to the workflow for developers, but we'll be using a specific release. This also gives 
users access to the frontend 
```bash
git clone --branch <version number> https://github.com/dolosse/dolosse.git
cd dolosse
python3 -m venv venv
pip install -r requirements.txt
```
*NOTE:* There's nothing that's ready for prime time just yet.
## For use in custom software
This one is easy. Just install via pip: 
```bash
pip install dolosse
```
Now you'll have access to the packages and modules we use to build our components. 

# Licensing
[Apache License 2.0.](https://www.apache.org/licenses/LICENSE-2.0.html) 

