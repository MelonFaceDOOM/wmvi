import math 
from config import VSM_CREDENTIALS
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from lingua import Language, LanguageDetectorBuilder

def remove_non_en_from_db_using_file():
    df = pd.read_csv('data/en_labeled.csv') # this is a file that contains all tweets or reddit posts and labels from the language detector
    non_en_ids = df[df['en']==False]['id'].to_list()
    delete_non_en(non_en_ids)
    
def remove_non_en_from_db():
    non_en_ids = find_non_en_ids()
    delete_non_en(non_en_ids)
    
def find_non_en_ids():
    non_en_ids = []
    for chunk in get_tweets_in_chunks():
        df = pd.DataFrame(chunk)
        label_en(df) # adds en col
        non_en_ids.extend(df[df['en']==False]['id'].to_list())
        break
    return non_en_ids
    
def delete_non_en(non_en_ids):
    conn = psycopg2.connect(**NETWORK_TWITTER_VACCINE_DB_CREDENTIALS)
    cur = conn.cursor()
    
    cur.execute("""SELECT COUNT(*) FROM search_term_match""")
    search_term_match_rows = cur.fetchone()[0]
    print(f"{search_term_match_rows} search term match rows before deleting any vaccine_tweets")
    
    cur.execute("""CREATE TEMP TABLE temp_ids (id BIGINT)""")
    cur.executemany("""INSERT INTO temp_ids (id) VALUES (%s)""", [(id,) for id in non_en_ids])
    # cur.execute("""SELECT COUNT(*) FROM vaccine_tweet WHERE id IN (SELECT id FROM temp_ids)""") # preview row deletion count
    cur.execute("""DELETE FROM vaccine_tweet WHERE id IN (SELECT id FROM temp_ids)""")
    conn.commit()
    
    cur.execute("""SELECT COUNT(*) FROM search_term_match""")
    search_term_match_rows = cur.fetchone()[0]
    print(f"{search_term_match_rows} search term match rows after")
    
    cur.close()
    conn.close()
    
def get_tweets_in_chunks(chunk_size=100_000):
    # 100k just seems like a reasonable number but maybe a bigger number would actually be faster. who knows.
    conn = psycopg2.connect(**NETWORK_TWITTER_VACCINE_DB_CREDENTIALS)
    cur = conn.cursor(cursor_factory = RealDictCursor)
    cur.execute(f'''SELECT id, tweet_text
                    FROM vaccine_tweet
                    ORDER BY id ASC
                    LIMIT %s;''',(chunk_size,))
    results = cur.fetchall()
    yield results
    if len(results) == chunk_size:
        while len(results) > 0:
            max_id = results[-1]['id']
            cur.execute(f'''SELECT id, tweet_text
                            FROM vaccine_tweet
                            WHERE id > %s
                            ORDER BY id ASC
                            LIMIT %s;''',(max_id, chunk_size))
            results = cur.fetchall()
            yield results
    cur.close()
    
    
def label_all():
    df = get_vaccine_tweets()
    label_en(df) # adds en col
    df.to_csv("data/en_labeled.csv", index=False)
    
def read_labeled_sample():
    df = pd.read_csv("data/en_labeled_sample.csv")
    print(len(df))
    correct_trues = ((df['en']==True) & (df['en_actual']=="t")).sum()
    incorrect_trues = ((df['en']==True) & (df['en_actual']=="f")).sum()
    correct_falses = ((df['en']==False) & (df['en_actual']=="f")).sum()
    incorrect_falses = ((df['en']==False) & (df['en_actual']=="t")).sum()
    
    sensitivity = correct_trues / (df['en_actual']=="t").sum()
    specificity = correct_falses / (df['en_actual']=="f").sum()
    false_positive = incorrect_trues / (df['en_actual']=="t").sum()
    false_negative = incorrect_falses / (df['en_actual']=="f").sum()
    
    print(f"Sensitivity: {'{:.1%}'.format(sensitivity)}")
    print(f"Specificity: {'{:.1%}'.format(specificity)}")
    print(f"False Positive: {'{:.1%}'.format(false_positive)}")
    print(f"False Negative: {'{:.1%}'.format(false_negative)}")
    
def label_en(df):
    languages = [Language.ENGLISH, Language.FRENCH, Language.GERMAN, Language.SPANISH]
    detector = LanguageDetectorBuilder.from_languages(*languages).build()
    # detector = LanguageDetectorBuilder.from_all_languages().with_low_accuracy_mode().build()
    df['en'] = df.apply(lambda x: detector.detect_language_of(x['tweet_text'])==Language.ENGLISH, axis=1)
    return df
        
def get_vaccine_tweets(n=0):
    conn = psycopg2.connect(**NETWORK_TWITTER_VACCINE_DB_CREDENTIALS)
    cur = conn.cursor(cursor_factory = RealDictCursor)
    if n:
        cur.execute("""SELECT id, tweet_text FROM vaccine_tweet limit %s""", (n,))
    else:
        cur.execute("""SELECT id, tweet_text FROM vaccine_tweet""")
    r = cur.fetchall()
    df = pd.DataFrame(r)
    cur.close()
    conn.close()
    return df
    
def get_sample_and_label_en():
    conn = psycopg2.connect(**NETWORK_TWITTER_VACCINE_DB_CREDENTIALS)
    cur = conn.cursor()
    cur.execute("""SELECT COUNT(*) FROM vaccine_tweet""")
    n_total = cur.fetchone()[0]
    cur.close()
    conn.close()
    n_sample = sample_size(n_total)
    df = get_vaccine_tweets(n=n_sample)
    label_en(df) # adds en col
    df.to_csv("data/en_labeled_sample.csv", index=False)
    

def sample_size(population_size, confidence_level=0.95, margin_of_error=0.05):
    # Z-values for common confidence levels
    z_values = {
        0.90: 1.645,
        0.95: 1.96,
        0.99: 2.576
    }
    z = z_values.get(confidence_level)
    if z is None:
        raise ValueError("Unsupported confidence level. Supported levels are 0.90, 0.95, and 0.99.")
    # Calculate the initial sample size without considering finite population
    p = 0.5
    n_0 = (z**2 * p * (1 - p)) / (margin_of_error**2)
    # Adjust for finite population
    n = n_0 / (1 + (n_0 - 1) / population_size)
    return math.ceil(n)    