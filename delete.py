#!/usr/bin/env python3
from google.cloud import datastore

def delete_all_posts(batch_size=500):
    client = datastore.Client()

    print("Suppression de tous les Post...")

    while True:
        query = client.query(kind="Post")
        query.keys_only()

        posts = list(query.fetch(limit=batch_size))

        if not posts:
            break

        with client.transaction():
            for entity in posts:
                client.delete(entity.key)

        print(f"Supprimé {len(posts)} posts...")

    print(" Tous les posts ont été supprimés.")

if __name__ == "__main__":
    delete_all_posts()