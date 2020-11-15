"""
file: __main__.py
brief: Defines the main module for pld_to_parquet
author: S. V. Paulauskas
date: November 15, 2020
"""
from dolosse.analysis.pld_to_parquet import pld_to_parquet

try:
    pld_to_parquet()
except KeyboardInterrupt:
    print("Exiting the program now.")