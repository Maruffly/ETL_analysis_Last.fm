"""
Extract - Transform - Load - Analys
"""

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
    # extract top tracks per year and enrich them
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
    # cast numeric columns and drop unused ones
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df = df.drop(columns=["track_popularity"], errors="ignore")
    return df


def compute_insights(df: pd.DataFrame) -> pd.DataFrame:
    # add derived metrics and true rank per year
    df["track_weight_in_artist"] = (df["track_listeners"] / df["listeners"]) * 100

    df = df.sort_values(
        by=["ranking_year", "track_listeners"],
        ascending=[True, False]
    )
    df["true_rank"] = (
        df.groupby("ranking_year")["track_listeners"]
        .rank(ascending=False, method="first")
        .astype(int)
    )

    return df


def save(df: pd.DataFrame, path: str) -> None:
    # export the dataset to CSV
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"\n[SAVED] {path} ({len(df)} rows)")


def analyse(df: pd.DataFrame) -> None:
    #Print analysis summaries to stdout
    print("\n====| Global Correlation Matrix (2020–2024) |====")
    print(df[NUMERIC_COLS].corr())

    print("\n----| Top 10 All-Time (by track_listeners) |----")
    top10 = (
        df.sort_values("track_listeners", ascending=False)
        [["track_name", "artist_name", "ranking_year", "track_listeners"]]
        .head(10)
    )
    print(top10.to_string(index=False))

    print("\n----| Most Recurring Artists in Annual Tops |----")
    print(df["artist_name"].value_counts().head(5).to_string())


def main() -> None:
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