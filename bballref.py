import pandas as pd
import os
import time
import requests
from bs4 import BeautifulSoup

years = list(range(2009, 2026))

base_reg = "https://www.basketball-reference.com/wnba/years/{}_totals.html"
base_ps  = "https://www.basketball-reference.com/wnba/playoffs/{}_totals.html"

os.makedirs("data", exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

def scrape_table_with_links(url):
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table", id="totals")

    # Get column headers
    thead = table.find("thead")
    headers = []
    for th in thead.find_all("tr")[-1].find_all("th"):
        headers.append(th.get("data-stat", th.get_text(strip=True)))

    # Extract data row by row
    rows_data = []
    hrefs = []
    player_ids = []

    for row in table.tbody.find_all("tr"):
        # Skip header rows that repeat in the table
        if "thead" in row.get("class", []):
            continue
            
        # Check if this is a real data row
        th = row.find("th", {"data-stat": "player"})
        if not th:
            continue

        player_name = th.get_text(strip=True)
        if player_name == "Player":
            continue  # repeated header rows

        # Extract href and player_id
        a = th.find("a")
        if a:
            href = a["href"]
            pid = href.split("/")[-1].replace(".html", "")
        else:
            href = None
            pid = None

        hrefs.append(href)
        player_ids.append(pid)

        # Extract all cell values for this row
        row_data = {}
        for td in row.find_all(["th", "td"]):
            stat_name = td.get("data-stat")
            if stat_name:
                # For player column, only get the text from the <a> tag if it exists
                if stat_name == "player":
                    a_tag = td.find("a")
                    if a_tag:
                        row_data[stat_name] = a_tag.get_text(strip=True)
                    else:
                        row_data[stat_name] = td.get_text(strip=True)
                else:
                    row_data[stat_name] = td.get_text(strip=True)
        
        rows_data.append(row_data)

    # Create DataFrame from the extracted data
    df = pd.DataFrame(rows_data)
    
    # Add href and player_id columns
    if len(df) != len(hrefs):
        raise ValueError(
            f"Row mismatch: stats={len(df)} links={len(hrefs)}"
        )

    df["player_href"] = hrefs
    df["player_url"] = [
        f"https://www.basketball-reference.com{h}" if h else None
        for h in hrefs
    ]
    df["player_id"] = player_ids

    print(df)
    return df

for year in years:
    print(f"\n--- {year} ---")

    # ---------- Regular Season ----------
    try:
        reg_url = base_reg.format(year)
        reg_df = scrape_table_with_links(reg_url)
        reg_out = f"data/{year}_bballref.csv"
        reg_df.to_csv(reg_out, index=False)
        print(f"Saved regular → {reg_out}")
    except Exception as e:
        print(f"Regular season failed for {year}: {e}")

    time.sleep(2)

    # ---------- Playoffs ----------
    try:
        ps_url = base_ps.format(year)
        ps_df = scrape_table_with_links(ps_url)
        ps_out = f"data/{year}ps_bballref.csv"
        ps_df.to_csv(ps_out, index=False)
        print(f"Saved playoffs → {ps_out}")
    except Exception as e:
        print(f"Playoffs failed for {year}: {e}")

    time.sleep(2)

print("\nDone.")