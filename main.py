"""
Extract - Transform - Load - Analys
"""

import ast
from typing import Counter

import pandas as pd
from src.Extractor import LastFMclient
from src.Transformer import DataEnricher

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

YEARS = [2020, 2021, 2022, 2023, 2024]
LIMIT = 200
NUMERIC_COLS = ["track_listeners", "listeners", "playcount", "duration_ms"]
OUTPUT_FILE = f"top_{LIMIT}_tracks_{YEARS[0]}_{YEARS[-1]}.csv"


# ─────────────────────────────────────────────
# ETL STEPS
# ─────────────────────────────────────────────

def extract_and_transform(client: LastFMclient, enricher: DataEnricher) -> pd.DataFrame:
    """Extract the top tracks/year and enrich them with metadata.

        Args:
            client:  Authenticated LastFM API client.
            enricher: DataEnricher that merges artist/track details into raw records.
        Returns:
            Concatenated DataFrame with one row per track and a 'ranking_year' column.
        Raises:
            ValueError: If no data could be collected for any of the configured years.
    """
    frames = []

    for year in YEARS:
        raw_tracks = client.get_top_tracks_yearly(year, limit=LIMIT)

        if not raw_tracks:
            print(f"[WARN] No data for {year}, skipping.")
            continue

        enriched = enricher.enrich_tracks(raw_tracks)
        df_year = pd.DataFrame(enriched)
        df_year["ranking_year"] = year
        frames.append(df_year)
        print(f"[OK] {year} — {len(df_year)} tracks extracted.")

    if not frames:
        raise ValueError("No data collected across all years.")

    return pd.concat(frames, ignore_index=True)


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Cast numeric columns and drop unused ones.

        Args:
            df: Raw enriched DataFrame.
        Returns:
            Cleaned DataFrame with numeric columns coerced and irrelevant
            columns removed.
    """
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df = df.drop(columns=["track_popularity"], errors="ignore")
    return df


def compute_insights(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived metrics and a true rank per year.

        Computes:
            - track_weight_in_artist: share of track listeners vs. total artist listeners (%).
            - true_rank: listener-based rank of each track within its ranking year.
        Args:
            df: Cleaned DataFrame.
        Returns:
            DataFrame with the two new columns added and sorted by year then listeners.
    """
    df["track_weight_in_artist"] = (df["track_listeners"] / df["listeners"]) * 100

    df = df.sort_values(
        by=["ranking_year", "track_listeners"],
        ascending=[True, False])

    df["true_rank"] = (
        df.groupby("ranking_year")["track_listeners"]
        .rank(ascending=False, method="first")
        .astype(int))

    return df


def save(df: pd.DataFrame, path: str) -> None:
    """Export the dataset to a csv file.

        Args:
            df:   DataFrame to export.
            path: Destination file path.
    """
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"\n[SAVED] {path} ({len(df)} rows)")


def analyze_genres_yearly(df: pd.DataFrame) -> None:
    """Analyze and display the evolution of the 5 dominant genres per year.

    Args:
        df: Final enriched DataFrame.
    """

    tmp_df = df.copy()

    # transform genre column to list
    def parse_genres(x):
        # if x is already a list / tuple / null return list / empty list
        if isinstance(x, (list, tuple)):
            return list(x)
        if pd.isna(x):
            return []
            
        # handle N/A parsing
        if isinstance(x, str):
            if x == "N/A" or x.strip() == "":
                return []
            try:
                return ast.literal_eval(x)
            except (ValueError, SyntaxError):
                # if it's a simple string but not a formatted list
                return [x]
        
        return []

    tmp_df['genres_list'] = tmp_df['genres'].apply(parse_genres)

    excluded_tags = ["female vocalists", "male vocalists", "seen live", "korean"]

    years = sorted(tmp_df['ranking_year'].unique())
    
    for year in years:
        year_data = tmp_df[tmp_df['ranking_year'] == year]

        all_tags = []
        for sublist in year_data['genres_list']:
            for tag in sublist:
                tag_clean = tag.lower().strip()
                
                # group kpop variants
                if tag_clean in ["kpop", "k-pop"]:
                    tag_clean = "k-pop"
                
                # filter generics
                if tag_clean not in excluded_tags:
                    all_tags.append(tag_clean)
        
        counts = Counter(all_tags)
        top_5 = counts.most_common(5)
        
        genres_display = ", ".join([f"{genre} ({count})" for genre, count in top_5])
        print(f"[{year}] : {genres_display}")



def analyse(df: pd.DataFrame) -> None:
    """Print analysis summaries to stdout.

        Displays:
            - Global correlation matrix across numeric columns.
            - Top 10 tracks of all time by listener count.
            - Most recurring artists across all annual top lists.
        Args:
            df: Final enriched and ranked dataFrame.
        """
    print("\n====| Global correlation matrix (2020–2024) |====")
    print(df[NUMERIC_COLS].corr())

    print("\n====| Most recurring artists in annual tops |====")
    print(df["artist_name"].value_counts().head(5).to_string())

    print("\n====| Genre evolution by year|====")
    analyze_genres_yearly(df)



def main() -> None:
    """Analyze and print the top 5 most frequent genres per year.
    
    Args:
        df: Final enriched DataFrame containing 'genres' and 'ranking_year'.
    """
    print(f"\n{'='*16}| ETL starting for {YEARS} |{'='*16}\n")

    client = LastFMclient()
    enricher = DataEnricher(client)

    df = extract_and_transform(client, enricher)
    df = clean(df)
    df = compute_insights(df)
    save(df, OUTPUT_FILE)
    analyse(df)


if __name__ == "__main__":
    main()