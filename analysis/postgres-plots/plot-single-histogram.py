"""
file: plot-single-histogram.py
brief: Simple script that tests how to plot a single histogram from data in postgres
author: S. V. Paulauskas
date: February 22, 2019
"""
import keyring
import psycopg2
import pandas as pd
import yaml
import sys

if __name__ == "__main__":
    with open(sys.argv[1]) as f:
        cfg = yaml.safe_load(f)

    try:
        connection = psycopg2.connect(user=cfg['username'],
                                      password=keyring.get_password(cfg['host'], cfg['username']),
                                      host=cfg['host'], port=cfg['port'], database=cfg['db'])
        cursor = connection.cursor()
        cursor.execute('select energy from hyper_test where channel = 0 and slot = 2 and energy > 0 and energy < 10000;')
        df = pd.DataFrame.from_dict(cursor.fetchall())

        ax = df.plot.hist(bins=5000)
        ax.set_xlim([0, 5000])
        ax.set_ylim([0, 8000])
        ax.get_figure().savefig('energy-id10.png')
    except psycopg2.Error as ex:
        print("Error connecting to db.", ex)
