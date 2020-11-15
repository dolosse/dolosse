"""
file: __main__.py
brief: Defines the main module for PldToParquet
author: S. V. Paulauskas
date: November 15, 2020
"""
from argparse import ArgumentParser
from dolosse.analysis.PldToParquet.pld_to_parquet import convert_pld

if __name__ == '__main__':
    try:
        parser = ArgumentParser(description='Converts PLD files into Apache Parquet format.')
        parser.add_argument('cfg', type=str, default='config.yaml',
                            help='The YAML configuration file')
        args = parser.parse_args()

        convert_pld(args.cfg)
    except KeyboardInterrupt:
        print("Exiting the program now.")
