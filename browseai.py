import os
import pandas as pd
import requests

# Your Browse AI API Key (Replace with your actual key)
BROWSE_AI_API_KEY = "855d7c74-bdc9-4732-a004-7885772bbed9:f0bec6a7-d504-415e-b6e0-3b384dfb0d18"
BROWSE_AI_TASK_ID = "40a3dace-c072-4ebf-b975-386548458fcd"

def get_browse_ai_data(entity):
    """Fetch ambiguity data using Browse AI"""
    url = f"https://api.browse.ai/v2/tasks/{BROWSE_AI_TASK_ID}/execute"
    
    headers = {
        "Authorization": f"Bearer {BROWSE_AI_API_KEY}",
        "Content-Type": "application/json"
    }
    
    data = {"inputs": {"entity": entity}}
    
    try:
        response = requests.post(url, json=data, headers=headers, timeout=15)
        response.raise_for_status()
        
        result = response.json()
        
        if "outputs" in result and result["outputs"]:
            ambiguity_data = result["outputs"].get("ambiguity_data", "No data found")
            source_links = result["outputs"].get("source_links", "No links found")
            return ambiguity_data, source_links
        
        return "No data found", "No links found"

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

    if "Ambiguity Data" not in df.columns:
        df["Ambiguity Data"] = ""
    if "Source Links" not in df.columns:
        df["Source Links"] = ""

    for index, row in df.iterrows():
        if pd.isna(row["Ambiguity Data"]) or row["Ambiguity Data"] == "":
            entity_name = row[entity_column]
            print(f"Fetching ambiguity data for: {entity_name}")
            meanings, links = get_browse_ai_data(entity_name)
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
