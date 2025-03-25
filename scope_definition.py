import pandas as pd
import json
from pathlib import Path


def read_excel_data(file_path):
    """Read entity data from Excel file with multiple sheets"""
    xls = pd.ExcelFile(file_path)

    df_list = []
    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name)
        df_list.append(df)

    full_df = pd.concat(df_list, ignore_index=True)

    entities = []
    for _, row in full_df.iterrows():
        if pd.notna(row["Mention"]) and pd.notna(row["Entity"]):
            entities.append({
                "SL.No": row["SL.No"],
                "Mention": row["Mention"],
                "Entity": row["Entity"],
                "Category": row["Category"],
                "Ambiguity Data": row.get("Ambiguity Data", "")
            })
    return entities


class EntityProcessor:
    def __init__(self, excel_path):
        self.raw_entities = read_excel_data(excel_path)
        self.category_map = {
            # PERSON
            "বলিউড অভিনেতা": "PERSON",
            "Founder, CEO of Khan Academy": "PERSON",
            "অভিনেত্রী": "PERSON",
            "সুরকার ও গায়ক": "PERSON",
            "ব্রাজিলের সাবেক ফুটবল তারকা": "PERSON",
            "পর্তুগালের ফুটবল তারকা": "PERSON",
            "ব্যক্তি নাম": "PERSON",

            # LOCATION
            "বাংলাদেশের রাজধানী": "LOCATION",
            "প্রশাসনিক অঞ্চল": "LOCATION",
            "শহর": "LOCATION",
            "নদী": "LOCATION",
            "দেশ": "LOCATION",
            "অতিথি ভবন": "LOCATION",

            # ORGANIZATION
            "ক্রিকেট দল": "ORGANIZATION",
            "জুতা কোম্পানি": "ORGANIZATION",
            "ব্যবসায়ী প্রতিষ্ঠান": "ORGANIZATION",
            "বাংলাদেশের একটি বাজারের নাম": "ORGANIZATION",

            # MISC
            "চন্দ্রের একটি কলা": "MISC",
            "প্রাকৃতিক দৃশ্য": "MISC",
            "চলচ্চিত্র": "MISC",
            "ইতিহাস": "MISC",
            "খেলা/অনুষ্ঠান": "MISC"
        }

    def process_entities(self):
        """Main processing pipeline"""
        processed = {
            "PERSON": {"count": 0, "examples": [], "ambiguity": {}},
            "LOCATION": {"count": 0, "examples": [], "ambiguity": {}},
            "ORGANIZATION": {"count": 0, "examples": [], "ambiguity": {}},
            "MISC": {"count": 0, "examples": [], "ambiguity": {}}
        }

        for entity in self.raw_entities:
            category = self.category_map.get(entity["Category"], "MISC")

            processed[category]["count"] += 1

            if entity["Entity"] not in processed[category]["examples"]:
                processed[category]["examples"].append(entity["Entity"])

            mention = entity["Mention"]
            if mention not in processed[category]["ambiguity"]:
                processed[category]["ambiguity"][mention] = []

            processed[category]["ambiguity"][mention].append({
                "entity_id": entity["SL.No"],
                "entity_name": entity["Entity"],
                "description": entity["Category"],
                "ambiguity_data": entity["Ambiguity Data"],
                "wikipedia_link": f"https://bn.wikipedia.org/wiki/{entity['Entity'].replace(' ', '_')}"
            })

        return processed

def save_results(processed_data, output_dir):
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    with open(f"{output_dir}/processed_entities.json", "w", encoding="utf-8") as f:
        json.dump(processed_data, f, ensure_ascii=False, indent=2)

    for category, data in processed_data.items():
        with open(f"{output_dir}/{category.lower()}_entities.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Saved results to {output_dir}/")

if __name__ == "__main__":
    EXCEL_PATH = "Entity_List.xlsx"
    OUTPUT_DIR = "processed_data"
    processor = EntityProcessor(EXCEL_PATH)
    processed_data = processor.process_entities()

    save_results(processed_data, OUTPUT_DIR)

    print("\nDataset Summary:")
    for category, data in processed_data.items():
        print(f"\n{category}:")
        print(f"- Total Entities: {data['count']}")
        print(f"- Unique Mentions: {len(data['ambiguity'])}")
        print(f"- Example Entities: {', '.join(data['examples'][:3])}...")