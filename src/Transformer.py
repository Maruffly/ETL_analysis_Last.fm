import threading
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

class DataEnricher:
    """Enriches raw track data with artist statistics and track details.

        Fetches additional metadata from the Last.fm API and merges it into
        each track record. Uses a thread-safe local cache to avoid redundant
        API ca
     """
    def __init__(self, client):
        self.client = client
        self.artist_cache: dict = {}
        self._lock = threading.Lock()


    def enrich_tracks(self, tracks):
        """Enrich a list of raw track dicts with artist and track metadata.

        Fetches are async with a thread pool. Artist data is cached
        to avoid double API calls.

        Args:
            tracks: List of raw track dicts (must contain 'artist_name'
                    and 'track_name' keys).
        Returns:
            List of enriched track dicts combining the original fields with
            artist stats and track details.
        """
        enriched_data = []

        def process_track(track):
            artist_name = track['artist_name']
            track_name = track['track_name']

            with self._lock:
                if artist_name not in self.artist_cache:
                    # temporarily store a sentinel to avoid duplicate fetches
                    # while the lock is released during the API call.
                    self.artist_cache[artist_name] = None

            # fetch outside the lock so other threads are not blocked.
            artist_stats = self.client.get_artist_stats(artist_name) or {}

            with self._lock:
                # only write if fetch succeeded or if still unset.
                if not self.artist_cache.get(artist_name):
                    self.artist_cache[artist_name] = artist_stats

            cached_artist = self.artist_cache[artist_name] or {}
            track_info = self.client.get_track_details(artist_name, track_name) or {}

            return {**track, **cached_artist, **track_info}

        # dict fusion
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(process_track, t) for t in tracks]
            for future in tqdm(as_completed(futures), total=len(tracks), unit="track"):
                enriched_data.append(future.result())

        return enriched_data

