from db.db import init_pool, close_pool, getcursor
init_pool()
with getcursor(commit=True) as cur:
    cur.execute('''SELECT COUNT(*) FROM matches.post_term_match''')
    r = cur.fetchall()
    print(r)
    cur.execute('''DELETE FROM matches.post_term_match''')
    cur.execute('''SELECT COUNT(*) FROM matches.post_term_match''')
    r = cur.fetchall()
    print(r)
close_pool()