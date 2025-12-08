import time
import psycopg2
from psycopg2 import sql
from lingua import Language, LanguageDetectorBuilder
from config import VSM_CREDENTIALS

def main():
    label_en()
    
def count_en_non_en():
    conn = psycopg2.connect(**VSM_CREDENTIALS)
    cur = conn.cursor()
    cur.execute('''SELECT COUNT(*) FROM tweet WHERE is_en IS TRUE''')
    count = cur.fetchone()[0]
    print('en labeled tweets', count)
    cur.execute('''SELECT COUNT(*) FROM tweet WHERE is_en IS FALSE''')
    count = cur.fetchone()[0]
    print('non-en labeled tweets', count)
    cur.close()
    conn.close()
    
def label_en():
    languages = [Language.ENGLISH, Language.FRENCH, Language.GERMAN, Language.SPANISH]
    detector = LanguageDetectorBuilder.from_languages(*languages).build()
    conn = psycopg2.connect(**VSM_CREDENTIALS)
    cur = conn.cursor()
    for table_name, id_column, text_column, id_columntype in [("tweet", "id", "tweet_text", "BIGINT"), ("reddit_submission", "id", "title", "TEXT",), ("reddit_comment", "id", "body", "TEXT")]:
        chunk_size = 100_000
        while True:
            cur.execute(sql.SQL("""SELECT {}, {} FROM {}
                                   WHERE is_en IS NULL
                                   LIMIT %s """).format(
                                       sql.Identifier(id_column),
                                       sql.Identifier(text_column),
                                       sql.Identifier(table_name)
                                   ), (chunk_size,))
            rows = cur.fetchall()
            if len(rows) == 0:
                break
            rows_labeled = [(row[0], is_en(detector, row[1])) for row in rows]
            label_en_in_db(conn, rows_labeled, table_name, id_columntype)
    cur.close()
    conn.close()
    
def is_en(detector, text):
    return detector.detect_language_of(text)==Language.ENGLISH

def label_en_in_db(conn, labeled_rows, table_name, id_columntype):
    """
    Takes a list of pairs (id, is_en) and updates those labels in the database.
    """
    cur = conn.cursor()
    try:
        cur.execute(sql.SQL(
            """CREATE TEMP TABLE temp_labels (id {}, is_en BOOLEAN)
               ON COMMIT DROP"""
        ).format(sql.SQL(id_columntype)))
        cur.executemany("INSERT INTO temp_labels (id, is_en) VALUES (%s, %s)", labeled_rows)
        cur.execute(f"""
            UPDATE {table_name}
            SET is_en = temp_labels.is_en
            FROM temp_labels
            WHERE {table_name}.id = temp_labels.id
        """)
        conn.commit()
        print(f"{table_name}: {cur.rowcount} rows updated.")
    except Exception as e:
        conn.rollback()
        print(f"Error updating rows: {e}")
    finally:
        cur.close()