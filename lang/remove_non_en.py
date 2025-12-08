from config import VSM_CREDENTIALS
import psycopg2
from lingua import Language, LanguageDetectorBuilder

# TODO update this so it runs on all tweets (tweet_text), reddit submission (title), and reddit comment (body)

def main():
    # remove_non_en_from_db()
    pass
    
    
def remove_non_en_from_db():
    languages = [Language.ENGLISH, Language.FRENCH, Language.GERMAN, Language.SPANISH]
    detector = LanguageDetectorBuilder.from_languages(*languages).build()
    # detector = LanguageDetectorBuilder.from_all_languages().with_low_accuracy_mode().build()
    
    conn = psycopg2.connect(**VSM_CREDENTIALS)
    cur = conn.cursor()
    chunk_size = 10_000
    max_id = 0
    # tweets
    while True:
        cur.execute("""SELECT id, tweet_text FROM tweet ORDER BY id ASC LIMIT %s WHERE id > %s""", (chunk_size, max_id))
        tweets = cur.fetchall()
        if len(tweets) == 0:
            break
        non_en_tweet_ids = [tweet[0] for tweet in tweets if not is_en(tweet[1])]
        delete_tweets_by_id(conn, non_en_tweet_ids):
    # reddit submissions
    while True:
        cur.execute("""SELECT id, title FROM reddit_submission ORDER BY id ASC LIMIT %s WHERE id > %s""", (chunk_size, max_id))
        reddit_submissions = cur.fetchall()
        if len(reddit_submissions) == 0:
            break
        non_en_reddit_submission_ids = [reddit_submission[0] for reddit_submission in reddit_submissions if not is_en(reddit_submission[1])]
        delete_reddit_submissions_by_id(conn, non_en_reddit_submission_ids):
    # reddit comments
    while True:
        cur.execute("""SELECT id, body FROM reddit_comment ORDER BY id ASC LIMIT %s WHERE id > %s""", (chunk_size, max_id))
        reddit_comments = cur.fetchall()
        if len(reddit_comments) == 0:
            break
        non_en_reddit_comment_ids = [reddit_comment[0] for reddit_comment in reddit_comments if not is_en(reddit_comment[1])]
        delete_reddit_comments_by_id(conn, non_en_reddit_comment_ids):
    conn.close()

def is_en(detector, text):
    return detector.detect_language_of(text)==Language.ENGLISH
    
def delete_tweets_by_id(conn, tweet_ids):
    # TODO compare time for WHERE id = ANY(%s) vs creating a temp table and using WHERE IN SELECT
    cur = conn.cursor()
    cur.execute("""DELETE FROM tweet WHERE id = ANY(%s)""",(tweet_ids,))
    conn.commit()
    print(f"{cur.rowcount} rows deleted.")
    
    # cur.execute("""SELECT COUNT(*) FROM search_term_match""")
    # search_term_match_rows = cur.fetchone()[0]
    # print(f"{search_term_match_rows} search term match rows before deleting any vaccine_tweets")
    
    # cur.execute("""CREATE TEMP TABLE temp_ids (id BIGINT)""")
    # cur.executemany("""INSERT INTO temp_ids (id) VALUES (%s)""", [(id,) for id in non_en_ids])
    # # cur.execute("""SELECT COUNT(*) FROM vaccine_tweet WHERE id IN (SELECT id FROM temp_ids)""") # preview row deletion count
    # cur.execute("""DELETE FROM vaccine_tweet WHERE id IN (SELECT id FROM temp_ids)""")
    # conn.commit()
    
    # cur.execute("""SELECT COUNT(*) FROM search_term_match""")
    # search_term_match_rows = cur.fetchone()[0]
    # print(f"{search_term_match_rows} search term match rows after")
    
    # cur.close()
    
def delete_reddit_submissions_by_id(conn, reddit_submission_ids):
    cur = conn.cursor()
    cur.execute("""DELETE FROM reddit_subimssion WHERE id = ANY(%s)""",(reddit_submission_ids,))
    conn.commit()
    print(f"{cur.rowcount} rows deleted.")
    
def delete_reddit_comments_by_id(conn, reddit_comment_ids):
    cur = conn.cursor()
    cur.execute("""DELETE FROM reddit_comment WHERE id = ANY(%s)""",(reddit_comment_ids,))
    conn.commit()
    print(f"{cur.rowcount} rows deleted.")


    
    
if __name__ == "__main__":
    main()