import os
import time
import random
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

class LastFMclient:
    def __init__(self):
        self.api_key = os.getenv('API_KEY')
        self.base_url = "http://ws.audioscrobbler.com/2.0/"
        if not self.api_key:
            raise ValueError("No API key founded in .env")

    def _get(self, method, params, retries=5):
        params = {
            'method' : method,
            'api_key' : self.api_key,
            'format': 'json',
            **params
        }
        for i in range(retries):
            try:
                response = requests.get(self.base_url, params=params, timeout=10)

                # rate limit handler (error code 29 / 429)
                if response.status_code in [29, 429]:
                    wait = (2 ** i) + random.random()
                    print(f"Rate limit reached. Retry in {wait:.2f}s...")
                    time.sleep(wait)
                    continue
                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as e:
                if i == retries:
                    print(f"####| API ERROR AFTER {retries - 1} RETRIES|#### : {e}")
                    return None
                time.sleep(1)
        return None

    def get_top_tracks_yearly(self, year, limit=1000):
        tracks = []
        page = 1
        requests_per_page = 100

        while len(tracks) < limit:
            params = {'tag': str(year), 'page': page, 'limit': requests_per_page}
            data = self._get('tag.getTopTracks', params)
            print(data)
            root_key = 'tracks' if 'tracks' in data else 'toptracks'
            if not data or root_key not in data:
                print(f"KEY {root_key} missing")
                break
            batch = data[root_key].get('track', [])

            if not batch: break
            for t in batch:
                rank = t.get('@attr', {}).get('rank', 0)
                tracks.append({
                    'track_name': t.get('name'),
                    'artist_name': t.get('artist', {}).get('name'),
                    'track_popularity': int(rank),
                    'year': year
                })
            attr = data[root_key].get('@attr', {})
            total_pages = int(attr.get('totalPages', 1))

            if page >= total_pages:
                break
            page += 1
        return tracks[:limit]

    def get_artist_stats(self, artist_name):
        data = self._get('artist.getInfo', {'artist': artist_name})
        if not data or 'artist' not in data:
            return None

        artist = data['artist']
        stats = artist.get('stats', {})
        return {
            'name': artist['name'],
            'playcount': int(stats.get('playcount', 0)),
            'listeners': int(stats.get('listeners', 0)),
            'tags': [t['name'] for t in artist.get('tags', {}).get('tag', [])]
        }



