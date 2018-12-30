import psycopg2
import pandas as pd
import threading
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
    for idx, trigger in frame.iterrows():
        coin_low = int(trigger) - 126
        coin_high = int(trigger) - 63
        statement = 'select * from id10 where time between ' + str(coin_low) + ' and ' + str(coin_high)
        cursor.execute(statement)
        df = pd.DataFrame.from_dict(cursor.fetchall())
        if not df.empty:
            df['trigger'] = trigger
            df.columns = ['energy', 'time', 'trigger']
            df['diff'] = trigger - df['time']
            print(threading.currentThread().getName(), ':\n', df)


if __name__ == '__main__':
    with open('cfg.yaml') as f:
        # use safe_load instead load
        cfg = yaml.safe_load(f)

    try:
        connection = psycopg2.connect(user=cfg['username'], password=cfg['password'],
                                      host=cfg['host'], port=cfg['port'], database=cfg['db'])
        cursor = connection.cursor()
        cursor.execute('select time from id00;')
        triggers = pd.DataFrame.from_dict(cursor.fetchall())
        threads = []
        num_threads = 10
        framechunks = chunk_data_frame(triggers, num_threads)
        for i in range(0, num_threads):
            t = threading.Thread(target=run, args=(connection, framechunks[i]))
            threads.append(t)
            t.start()
        for thread in threads:
            thread.join()
    except psycopg2.Error as ex:
        print("Error connecting to db.", ex)

# ax = df.plot.hist(bins=5000)
# ax.set_xlim([0, 5000])
# ax.set_ylim([0, 8000])
# ax.get_figure().savefig('time-difference-id10.png')
