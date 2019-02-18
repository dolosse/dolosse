import keyring
import psycopg2
import pandas as pd
import threading
import time
import yaml


def chunk_data_frame(frame, n):
    df_len = len(frame)
    chunk_size = int(len(frame) / n)
    count = 0
    dfs = []

    while True:
        if count > df_len - 1:
            break
        start = count
        count += chunk_size
        dfs.append(frame.iloc[start: count])

    return dfs


def run(conn, frame):
    cursor = connection.cursor()
    start = time.time()
    print(threading.current_thread().name, '- Generating statement list.')
    statement = ''
    for row in frame.itertuples():
        statement += cursor.mogrify(
            "insert into gated select id, energy, time from data where time between %s and %s and id>3;",
            (str(row[1] - 126), str(row[1] - 63))).decode()
    print(threading.currentThread().name, 'Finished generating statement list in ', time.time() - start, 'seconds.')
    start = time.time()
    print(threading.current_thread().name, '- Executing many.')
    cursor.execute(statement)
    print(threading.current_thread().name, '- Finished executing many in ', time.time() - start, 'seconds.')


if __name__ == '__main__':
    with open('cfg.yaml') as f:
        cfg = yaml.safe_load(f)

    try:
        connection = psycopg2.connect(user=cfg['username'], password=keyring.get_password(cfg['host'], cfg['username']),
                                      host=cfg['host'], port=cfg['port'], database=cfg['db'])
        cursor = connection.cursor()
        cursor.execute('select time from id00;')
        triggers = pd.DataFrame.from_dict(cursor.fetchall())
        threads = []
        num_threads = cfg['numThreads']
        framechunks = chunk_data_frame(triggers, num_threads)
        start = time.time()
        for i in range(0, num_threads):
            t = threading.Thread(target=run, args=(connection, framechunks[i]))
            threads.append(t)
            t.start()
        for thread in threads:
            thread.join()
        print('Main Thread - Executed in ', (time.time() - start)/60, " minutes.")
    except psycopg2.Error as ex:
        print("Error connecting to db.", ex)
