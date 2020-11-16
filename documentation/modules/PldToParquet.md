# pld_to_parquet
The Low-energy Nuclear Physics Group at The University of Tennessee at Knoxville developed the 
Pixie List Data (PLD) data format. **This is an experimental format.** They are efficient in  
its data packaging. [PAASS](https://github.com/pixie16/paass) and 
[PAASS-LC](https://github.com/spaulaus/paass-lc) can produce data taken from XIA LLC's Pixie-16 
modules in this format. 

This is not a standard data format used industry wide. We choose to convert this format into 
Apache Parquet. The Parquet format is an industry standard format. It's easily processed using 
Pandas and Spark. This makes the experimental data much more readily accessible and it does not
need to be processed out of the binary format provided by the hardware.   

## Usage
1. `python3 -m dolosse.analysis.PldToParquet /path/to/config.yaml`

## Sample Configuration File
```
# Required node
dry_run: False

#required node
hardware:
  pixie:
    firmware: 30474
    frequency: 250

# List of files that we're going to process.
#required node
input_files:
  - /path/to/file.pld

# Directory containing output files.
#required node
output_directory: data


# ---------- DO NOT MODIFY ANYTHING BELOW THIS LINE -----------
logging:
  version: 1
  disable_existing_loggers: true
  formatters:
    simple:
      format: '%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s'
  handlers:
    console:
      class: logging.StreamHandler
      level: INFO
      formatter: simple
      stream: ext://sys.stdout
    file:
      class: logging.FileHandler
      level: INFO
      formatter: simple
      filename: pld_to_parquet.log
  loggers:
    file:
      level: INFO
      handlers: [ console, file ]
      propagate: no
  root:
    level: DEBUG
    handlers: [ console, file ]
```