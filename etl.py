import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    songs_df = pd.read_json(filepath, lines=True)

    columns = ["song_id", "title", "artist_id", "year", "duration"]
    song_data = songs_df[columns].copy()
    song_data["title"] = song_data["title"]
    cur.execute(song_table_insert, song_data)
    
    columns = [
        "artist_id",
        "artist_name",
        "artist_location",
        "artist_latitude",
        "artist_longitude"
    ]
    artist_data = songs_df[columns].copy()
    artist_data["artist_name"] = artist_data["artist_name"].map(
        single_quote_converter, na_action='ignore'
    )
    artist_data["artist_location"] = artist_data["artist_location"].map(
        single_quote_converter, na_action='ignore'
    )
    cur.execute(artist_table_insert, artist_data)
    


def process_log_file(cur, filepath):
    df = pd.read_json(filepath, lines=True)

    df = df[df["page"] == 'NextSong']

    df["ts"] = pd.to_datetime(df["ts"], unit='ms')
     
    time_data = list(
        map(
            lambda item: [
                item.strftime('%Y-%m-%dT%H:%M:%S'),
                item.hour,
                item.day,
                item.week,
                item.month,
                item.year,
                item.weekday() + 1
            ],
            df["ts"].copy()
        )
    )
    column_labels = [
        'start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'
    ]
    time_list_dict = [dict(zip(column_labels, row)) for row in time_data]
    time_df = pd.DataFrame(time_list_dict)
    time_df = time_df.drop_duplicates()
    time_df = time_df.reset_index(drop=True)
    time_df = time_df[column_labels]

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    columns = ["userId", "firstName", "lastName", "gender", "level"]
    user_df = logs_df[columns].copy()
    user_df["firstName"] = user_df["firstName"].map(
        single_quote_converter, na_action='ignore'
    )
    user_df["lastName"] = user_df["lastName"].map(
        single_quote_converter, na_action='ignore'
    )
    user_df["gender"] = user_df["gender"].str.upper()
    user_df["userId"] = user_df["userId"].astype(str)
    user_df = user_df.drop_duplicates()
    user_df = user_df.drop_duplicates(subset='userId', keep='last')
    user_df = user_df.reset_index(drop=True)

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    for index, row in df.iterrows():
        
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

    start_index = 0
    end_index = songplays_df.shape[0]
    batch_size = 5_000
    for index in range(start_index, end_index, batch_size):
        print(f"Send 'songplays' records batch from idx '{index}'...")
        query = sql.songplay_table_insert(
            dataframe=songplays_df.iloc[index:index + batch_size]
        )
        cur.execute(query)
        conn.commit()



def process_data(cur, conn, filepath, func):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()