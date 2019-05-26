# [Dolosse](https://github.com/dolosse/dolosse) - A Kafka based Data Acquisition System
Copyright (c) 2018 - 2019, [S. V. Paulauskas](https://projectscience.tech)

Dolosse uses modern tools such as Kafka and PostgreSQL to provide a scientific data acquisition 
framework. The project is python based, which allows us to take advantage of a vast array of analysis
libraries.

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

# Documentation
Our documentation is sparse right now since we're still getting off the ground. You can read about 
the project motivation in [this presentation](https://docs.google.com/presentation/d/111I5qzSBoEeqXWFkZ2Doz_PpvOkBn-HIbGrVmhtsExg/edit?usp=sharing).

## Support
Checkout the [support] () page for more information. 

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
We license the software under the Apache License 2.0. 

