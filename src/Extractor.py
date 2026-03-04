import os
import time
import random
import requests
from dotenv import load_dotenv

load_dotenv()
"""
    HTTP client for LastFM API.
    Authentication, rate-limiting,
    methods for track / artist data.
"""
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
        for attempt in range(retries):
            try:
                response = requests.get(self.base_url, params=params, timeout=10)

                # back off exponentially then retry
                if response.status_code in (29, 429):
                    wait = (2 ** attempt) + random.random()
                    print(f"[RATE LIMIT] Retrying in {wait:.2f}s... (attempt {attempt + 1}/{retries})")
                    time.sleep(wait)
                    continue

                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as e:
                if attempt == retries - 1:
                    print(f"[API ERROR] Failed after {retries} attempts: {e}")
                    return None
                time.sleep(0.05)  # pause before retries

        return None


    """
        Get the top 200 tracks / year using LastFM tag.
        Paginates automatically until limit tracks are collected or
        there are no more pages.
    """
    def get_top_tracks_yearly(self, year, limit=200):
        tracks = []
        page = 1
        requests_per_page = 100

        while len(tracks) < limit:
            params = {'tag': str(year), 'page': page, 'limit': requests_per_page}
            data = self._get('tag.getTopTracks', params)
            #print(data)
            root_key = 'tracks' if 'tracks' in data else 'toptracks'

            if not data or root_key not in data:
                print(f"KEY {root_key} missing")
                break
            batch = data[root_key].get('track', [])
            if not batch:
                break

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



    """
            Fetch statistics and genre tags for a given artist.
            artist_name (str): The artist's name as known by Last.fm.
            Returns: dict | None: 
                Artist stats with keys:
                name, playcount, listeners, genres, artist_popularity.
                Returns None if the artist is not found.
    """
    def get_artist_stats(self, artist_name):
        data = self._get('artist.getInfo', {'artist': artist_name})

        if not data or 'artist' not in data:
            return None
        artist = data['artist']
        stats = artist.get('stats', {})
        artist_stats = {
            'name': artist['name'],
            'playcount': int(stats.get('playcount', 0)),
            'listeners': int(stats.get('listeners', 0)),
            'genres': [t['name'] for t in artist.get('tags', {}).get('tag', [])],
            'artist_popularity': int(artist.get('stats', {}).get('playcount', 0))
        }
        return artist_stats

    """
            Fetch album and audio metadata for a specific track.
            Returns: dict 
            Track details with keys:
                      album_name, release_date, duration_ms, track_listeners.
                      Returns an empty dict if the track is not found.
            """
    def get_track_details(self, artist_name, track_name):
        params = {
            'artist': artist_name,
            'track': track_name
        }
        data = self._get('track.getInfo', params)
        if not data or 'track' not in data:
            return {}

        track = data.get('track', {})
        album = track.get('album', {})
        track_detail = {
            'album_name': album.get('title', 'N/A'),
            'release_date': album.get('release_date', 'N/A'),
            'duration_ms': int(track.get('duration', 0)),
            'track_listeners': int(track.get('listeners', 0))
        }
        return track_detail



