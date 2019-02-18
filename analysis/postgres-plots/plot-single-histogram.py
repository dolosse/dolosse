import keyring
import psycopg2
import pandas as pd
import yaml

if __name__ == "__main__":
    with open('cfg.yaml') as f:
        # use safe_load instead load
        cfg = yaml.safe_load(f)

    try:
        connection = psycopg2.connect(user=cfg['username'], password=keyring.get_password(cfg['host'], cfg['username']),
                                      host=cfg['host'], port=cfg['port'], database=cfg['db'])
        cursor = connection.cursor()
        cursor.execute('select energy from id10 where energy > 0 and energy < 10000;')
        df = pd.DataFrame.from_dict(cursor.fetchall())

        ax = df.plot.hist(bins=5000)
        ax.set_xlim([0, 5000])
        ax.set_ylim([0, 8000])
        ax.get_figure().savefig('energy-id10.png')
    except psycopg2.Error as ex:
        print("Error connecting to db.", ex)
