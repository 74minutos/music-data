import argparse
import csv
import sqlite3
import unittest

def init_db(conn):
    conn.execute("""
        create table albums (
            band text,
            album text,
            date text,
            genre text,
            country text,
            real_date text,
            count integer)
        """)

def load_csv(f, conn):
    reader = csv.DictReader(f)
    cur = conn.cursor()

    with conn:
        for row in reader:
            # in this case we're mapping rows directly, but this would be a good place
            # to perform any needed preprocessing
            cur.execute("""
                insert into
                    albums(band, album, date, genre, country, real_date, count)
                values
                    (:band, :album, :date, :genre, :country, :real_date, :count)
            """, row)


class Test(unittest.TestCase):
    def test_load_data(self):
        conn = sqlite3.connect(':memory:')
        init_db(conn)
        with open('sample_metallum.csv', 'r') as f:
            load_csv(f, conn)

        cur = conn.cursor()
        cur.execute("select count(*) from albums")
        count = cur.fetchone()[0]
        self.assertTrue(count > 0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv_file", required=True, help="path to csv file to load")
    parser.add_argument("--output_file", required=True, help="path to output sqlite file")
    args = parser.parse_args()

    conn = sqlite3.connect(args.output_file)
    init_db(conn)

    with open(args.csv_file) as f:
        load_csv(f, conn)
