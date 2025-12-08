from psycopg2.extras import RealDictCursor
from db.db import init_pool, getcursor, close_pool
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

init_pool(prefix="OLD", minconn=1, maxconn=4, force_tunnel=False)

with getcursor(cursor_factory=RealDictCursor) as cur:
    cur.execute("SELECT * FROM episodes")
    r = cur.fetchall()

print(f"Total rows: {len(r)}")

df = pd.DataFrame(r)
value_counts = df["guid"].value_counts()
total_dupe = [b for a,b in value_counts.items() if b > 1]
total_dupe_count = sum(total_dupe) - len(total_dupe) 
print("total episodes predicted to be skipped for dupe guid:", total_dupe_count)

dup_guid_counts = value_counts[value_counts > 1]
dup_guids = dup_guid_counts.index

dups_df = df[df["guid"].isin(dup_guids)]

# How many dup GUIDs span multiple podcast_ids?
podcast_counts = dups_df.groupby("guid")["podcast_id"].nunique()

print("Total GUIDs with duplicates:", len(dup_guid_counts))
print("GUIDs spanning > 1 podcast_id:", (podcast_counts > 1).sum())
print("All dup GUIDs cross podcasts?:", bool((podcast_counts > 1).all()))

id_counts = df["id"].value_counts()
dup_id_counts = id_counts[id_counts > 1]
dup_id_rows = dup_id_counts.sum() - len(dup_id_counts)
print("rows involved in duplicate ids:", dup_id_rows)
# # Inspect top 3 duplicate GUIDs
# for guid in dup_guid_counts.head(3).index:
    # g = dups_df[dups_df["guid"] == guid]
    # print("\nGUID:", guid)
    # print("  num episodes:", len(g))
    # print("  podcast_ids:", sorted(g["podcast_id"].unique()))
    # for i, (_, row) in enumerate(g.head(3).iterrows(), start=1):
        # print(f"  Episode #{i}:")
        # print("    id         :", row["id"])
        # print("    title      :", row["title"])
        # print("    podcast_id :", row["podcast_id"])
        # print("    pub_date   :", row.get("pub_date"))
