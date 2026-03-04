import os
import time
import random
import requests
from dotenv import load_dotenv

load_dotenv()

class LastFMclient:
    """HTTP client for the Last.fm API.

    Handles authentication, rate-limiting, and exposes methods
    to fetch track and artist data.
    """
    def __init__(self):
        self.api_key = os.getenv('API_KEY')
        self.base_url = "http://ws.audioscrobbler.com/2.0/"
        if not self.api_key:
            raise ValueError("No API key founded in .env")


    def _get(self, method, params, retries=5):
        """Send a GET request to the LastFM API with exponential backoff on rate limits.

                Args:
                    method:  Last.fm API method name ( e.g. 'tag.getTopTracks').
                    params:  Query string parameters specific to the method.
                    retries: Maximum number of attempts before giving up.
                Returns:
                    Parsed JSON response as a dict, or None on failure.
                """
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


    def get_top_tracks_yearly(self, year, limit=200):
        """Return the top tracks for a given year using the LastFM tag endpoint.

        Paginates automatically until limit -tracks are collected or
        there are no more pages.

        Args:
            year:  The year to query (used as a Last.fm tag).
            limit: Maximum number of tracks to return.
        Returns:
            List of dicts with keys: track_name, artist_name, track_popularity, year.
        """
        tracks = []
        page = 1
        requests_per_page = 100

        while len(tracks) < limit:
            params = {'tag': str(year), 'page': page, 'limit': requests_per_page}
            data = self._get('tag.getTopTracks', params)

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


    def get_artist_stats(self, artist_name):
        """Fetch statistics and genre tags for a given artist.

        Args:
            artist_name: The artist's known by LastFM.
        Returns:
            Dict with keys: name, playcount, listeners, genres, artist_popularity.
            Returns None if the artist is not found.
        """
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


    def get_track_details(self, artist_name, track_name):
        """Fetch album and audio metadata for a specific track.

        Args:
            artist_name: The artist's name as known by Last.fm.
            track_name:  The track's name as known by Last.fm.
        Returns:
            Dict with keys: album_name, release_date, duration_ms, track_listeners.
            Returns an empty dict if the track is not found.
        """
        data = self._get('track.getInfo', {"artist": artist_name, "track": track_name})

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



