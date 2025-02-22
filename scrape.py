import os
import pandas as pd
import requests
from bs4 import BeautifulSoup

def get_wikipedia_ambiguity(entity):
    """Fetches all possible meanings and source links from Bangla Wikipedia."""
    base_url = "https://bn.wikipedia.org/wiki/"
    url = f"{base_url}{entity}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        
        # Check if it's a disambiguation page
        disambiguation_links = soup.select(".mw-disambig .mw-parser-output ul li a")
        if disambiguation_links:
            meanings = []
            links = []
            for link in disambiguation_links:
                text = link.get_text(strip=True)
                href = link.get("href")
                if href:
                    full_link = f"https://bn.wikipedia.org{href}"
                    meanings.append(text)
                    links.append(full_link)
            
            return "; ".join(meanings), "; ".join(links)
        
        # If not a disambiguation page, fetch the first paragraph
        paragraph = soup.find("p")
        if paragraph:
            return paragraph.get_text(strip=True), url
        else:
            return "No relevant information found", url

    except requests.exceptions.RequestException as e:
        return f"Error: {str(e)}", ""

def main():
    input_file = "Entity_List.xlsx"

    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found!")
        return
    
    try:
        df = pd.read_excel(input_file, engine="openpyxl")
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return

    if df.empty or df.columns[0] is None:
        print("Error: Excel file is empty or missing entity column.")
        return

    entity_column = df.columns[0]

    # Ensure necessary columns exist
    if "Ambiguity Data" not in df.columns:
        df["Ambiguity Data"] = ""
    if "Source Links" not in df.columns:
        df["Source Links"] = ""

    for index, row in df.iterrows():
        if pd.isna(row["Ambiguity Data"]) or row["Ambiguity Data"] == "":
            entity_name = row[entity_column]
            print(f"Fetching ambiguity data for: {entity_name}")
            meanings, links = get_wikipedia_ambiguity(entity_name)
            df.at[index, "Ambiguity Data"] = meanings
            df.at[index, "Source Links"] = links
            print(f"🔍 Ambiguity Data: {meanings}")
            print(f"🔗 Source Links: {links}")
    
    try:
        df.to_excel(input_file, index=False, engine="openpyxl")
        print(f"✅ Updated ambiguity data saved in {input_file}")
    except Exception as e:
        print(f"Error saving Excel file: {e}")

if __name__ == "__main__":
    main()
