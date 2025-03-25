import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
from bs4.builder import ParserRejectedMarkup
import time
from urllib.parse import unquote
from typing import Tuple
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

def get_wikipedia_ambiguity(entity: str) -> Tuple[str, str]:
    """Fetches ambiguity information from Bangla Wikipedia with robust error handling."""
    BASE_URL = "https://bn.wikipedia.org/wiki/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept-Language": "bn-BD, bn;q=0.9"
    }

    for attempt in range(3):
        try:
            response = requests.get(
                f"{BASE_URL}{entity}",
                headers=headers,
                timeout=15,
                allow_redirects=True
            )
            response.raise_for_status()
            try:
                soup = BeautifulSoup(response.text, "lxml")
            except (ParserRejectedMarkup):
                soup = BeautifulSoup(response.text, "html.parser")

            if soup.find("div", {"id": "disambigbox"}) or 'দ্ব্যর্থতা নিরসন' in soup.title.text:
                return parse_disambiguation_page(soup)

            return parse_article_page(soup), response.url

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return "Page not found", ""
            if attempt == 2:
                return f"HTTP Error {e.response.status_code}", ""
            time.sleep(2)
            
        except requests.exceptions.RequestException as e:
            if attempt == 2:
                return f"Connection Error: {str(e)}", ""
            time.sleep(2)
            
        except Exception as e:
            if attempt == 2:
                return f"Processing Error: {str(e)[:100]}", ""
            time.sleep(1)

    return "Max retries exceeded", ""

def parse_disambiguation_page(soup: BeautifulSoup) -> Tuple[str, str]:
    """Parses Bengali disambiguation pages with multiple sections."""
    meanings = []
    links = []
    for item in soup.select("#mw-content-text ul li"):
        link = item.find("a", href=True)
        if link:
            text = link.get_text(" ", strip=True)
            context = item.get_text().replace(text, "").strip(" :,-–•")
            if context:
                text = f"{text} ({context})"
            meanings.append(text)
            links.append(f"https://bn.wikipedia.org{unquote(link['href'])}")
    for section in soup.select(".mw-parser-output > h2"):
        section_title = section.get_text(strip=True)
        ul = section.find_next_sibling("ul")
        if ul:
            for item in ul.select("li"):
                link = item.find("a", href=True)
                if link:
                    text = f"{link.get_text(strip=True)} ({section_title})"
                    meanings.append(text)
                    links.append(f"https://bn.wikipedia.org{unquote(link['href'])}")

    return format_output(meanings, links)

def parse_article_page(soup: BeautifulSoup) -> str:
    """Extracts clean first paragraph from article pages."""
    content = soup.find("div", {"id": "mw-content-text"})
    
    for p in content.find_all("p", recursive=False):
        text = clean_text(p.get_text())
        if len(text) > 80:
            return text[:800] + "..." if len(text) > 800 else text
            
    return "Summary not available"

def clean_text(text: str) -> str:
    """Cleans Bengali text from references and markers."""
    text = text.replace("\n", " ").strip()
    while "[" in text and "]" in text:
        start = text.find("[")
        end = text.find("]", start)
        text = text[:start] + text[end+1:]
    return text

def format_output(meanings: list, links: list) -> Tuple[str, str]:
    """Formats output for Bengali text."""
    if not meanings:
        return "No disambiguation found", ""
        
    return (
        ";\n".join(meanings[:10]),
        ";\n".join(links[:10])
    )

def main():
    input_file = "Entity_List.xlsx"
    output_file = "Wikipedia_Ambiguity_Results.xlsx"
    backup_file = "Backup_Results.xlsx"
    
    try:
        df = pd.read_excel(input_file, engine="openpyxl")
        if df.empty:
            raise ValueError("Excel file is empty")

        for col in ["Ambiguity Data", "Source Links", "Processed", "Status"]:
            if col not in df.columns:
                df[col] = ""
                
        total = len(df)
        entity_column = df.columns[0]
        
        for index in df.index:
            if pd.isna(df.at[index, "Processed"]) or df.at[index, "Processed"] != "TRUE":
                entity = str(df.at[index, entity_column])
                print(f"Processing {index+1}/{total}: {entity}")
                
                meanings, links = get_wikipedia_ambiguity(entity)
                
                df.at[index, "Ambiguity Data"] = meanings
                df.at[index, "Source Links"] = links
                df.at[index, "Processed"] = "TRUE"
                df.at[index, "Status"] = "Success" if meanings else "Failed"
                
                # Save progress every 5 rows
                if (index + 1) % 5 == 0:
                    df.to_excel(backup_file, index=False, engine="openpyxl")
                    print(f"⏩ Saved backup to {backup_file}")

                time.sleep(1.5)
        
        df.to_excel(output_file, index=False, engine="openpyxl")
        print(f"\n✅ Successfully saved results to {output_file}")
        
    except Exception as e:
        print(f"❌ Critical Error: {str(e)}")
        if 'df' in locals():
            df.to_excel(backup_file, index=False)
            print(f"Saved recovery data to {backup_file}")

if __name__ == "__main__":
    main()