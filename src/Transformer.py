import threading
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

"""
       Enriches raw track data with artist statistics and track details
       fetched from the LastFM API.
       Uses a thread-safe local cache to avoid redundant API calls for
       artists that appear multiple times across the dataset.
"""
class DataEnricher:
    def __init__(self, client):
        self.client = client
        self.artist_cache = {}

    def enrich_tracks(self, tracks):
        enriched_data = []

        def process_track(track):
            artist_name = track['artist_name']
            track_name = track['track_name']

            # cache handling
            if artist_name not in self.artist_cache:
                self.artist_cache[artist_name] = self.client.get_artist_stats(artist_name) or {}
            track_info = self.client.get_track_details(artist_name, track_name) or {}
            return {**track, **self.artist_cache[artist_name], **track_info}
        # print(f"Fetching details for: {track_name}")

        # dict fusion
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(process_track, t) for t in tracks]
            for future in tqdm(as_completed(futures), total=len(tracks), unit="track"):
                enriched_data.append(future.result())

        return enriched_data

