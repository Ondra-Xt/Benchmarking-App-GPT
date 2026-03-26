from playwright.sync_api import sync_playwright
import pandas as pd
import time
import re
import os
import sys

class GeberitDiscoveryV7:
    def __init__(self, excel_path="benchmark_master_v3_fixed.xlsx"):
        self.excel_path = excel_path
        self.target_url = "https://catalog.geberit.de/de-DE/systems/CH3_3294141/products" 
        
        self.cols_tech = [
            "Brand", "Product_Name", "Article_Number_SKU", "Product_URL",
            "Length_mm", "Is_Cuttable", "Flow_Rate_ls", "Outlet_Type",
            "Is_Outlet_Selectable", "Height_Min_mm", "Height_Max_mm",
            "Material_Body", "Is_V4A", "Fleece_Preassembled",
            "Cert_DIN_EN1253", "Cert_DIN_18534", "Colors_Count",
            "Tile_In_Possible", "Wall_Installation", "Completeness_Type",
            "Ref_Price_Estimate_EUR", "Datasheet_URL", "Evidence_Text"
        ]

    def ensure_excel_exists(self):
        if not os.path.exists(self.excel_path):
            print(f"⚠️ Excel soubor {self.excel_path} nebyl nalezen.", file=sys.stderr)

    def analyze_geberit_surgical(self, text, h1_text):
        data = {
            "Length_mm": "", "Is_Cuttable": "No", "Flow_Rate_ls": "",
            "Height_Min_mm": "", "Material_Body": "", "Fleece_Preassembled": "No",
            "Outlet_Type": "", "Cert_DIN_EN1253": "No"
        }
        
        text_lower = text.lower().replace('\n', ' ')
        h1_lower = h1_text.lower()
        
        # 1. PRŮTOK (Ablaufleistung) -> Hledáme např. "Ablaufleistung 0,8 l/s"
        match_flow = re.search(r'ablaufleistung[^\d]{0,10}(\d+[.,]\d+)\s*l/s', text_lower)
        if match_flow: 
            data["Flow_Rate_ls"] = f"{match_flow.group(1).replace(',', '.')} l/s"

        # 2. VÝŠKA INSTALACE -> Geberit používá "Mindestestrichhöhe am Einlauf" nebo "H=" v cm/mm
        match_h_estrich = re.search(r'(?:mindestestrichhöhe|estrichhöhe)[^\d]{0,15}(\d+)\s*mm', text_lower)
        if match_h_estrich:
            data["Height_Min_mm"] = match_h_estrich.group(1)
        else:
            # Zkusíme najít "H = 9 cm" nebo "H = 90 mm"
            match_h_cm = re.search(r'\bh\s*[:=]?\s*(\d+(?:[.,]\d+)?)\s*cm', text_lower)
            if match_h_cm:
                data["Height_Min_mm"] = str(int(float(match_h_cm.group(1).replace(',', '.')) * 10))
            else:
                match_h_mm = re.search(r'\bh\s*[:=]?\s*(\d+)\s*mm', text_lower)
                if match_h_mm: data["Height_Min_mm"] = match_h_mm.group(1)

        # 3. DÉLKA -> Geberit uvádí "L = 30-90 cm" u roštů, nebo rovnou v H1 (např. 90 cm)
        match_len_range = re.search(r'\bl\s*[:=]?\s*(\d+)\s*-\s*(\d+)\s*cm', text_lower)
        if match_len_range:
            data["Length_mm"] = str(int(match_len_range.group(2)) * 10)
            data["Is_Cuttable"] = "Yes" # Pokud je tam rozsah, lze zkrátit
        else:
            match_len_single = re.search(r'\bl\s*[:=]?\s*(\d+)\s*cm', text_lower)
            if match_len_single:
                data["Length_mm"] = str(int(match_len_single.group(1)) * 10)
            else:
                # Záchrana z nadpisu (např. CleanLine20 90cm)
                match_h1_len = re.search(r'(\d+)\s*cm', h1_lower)
                if match_h1_len: data["Length_mm"] = str(int(match_h1_len.group(1)) * 10)

        # 4. ODPADNÍ TRUBKA (DN) -> Geberit zapisuje jako "d, ø = 50 mm"
        match_dn = re.search(r'd,\s*(?:ø|o)\s*[:=]?\s*(\d+)\s*mm', text_lower)
        if match_dn:
            data["Outlet_Type"] = f"DN {match_dn.group(1)}"
        elif "dn 50" in text_lower: data["Outlet_Type"] = "DN 50"
        elif "dn 40" in text_lower: data["Outlet_Type"] = "DN 40"

        # 5. MATERIÁL
        if "edelstahl" in text_lower or "nerez" in text_lower:
            data["Material_Body"] = "Edelstahl"
            if "v4a" in text_lower or "1.4404" in text_lower:
                data["Is_V4A"] = "Yes"
            else:
                data["Is_V4A"] = "No"

        # 6. TĚSNÍCÍ ROUNO
        if "dichtvlies vormontiert" in text_lower or "werkseitig" in text_lower:
            data["Fleece_Preassembled"] = "Yes"

        # 7. CERTIFIKACE
        if "en 1253" in text_lower:
            data["Cert_DIN_EN1253"] = "Yes"

        return data

    def run(self):
        self.ensure_excel_exists()
        discovered_products = []

        print("\n" + "="*60)
        print("🚀 Spouštím Geberit Discovery V7 (The Surgical Extractor)")
        print("="*60 + "\n", file=sys.stderr)

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context(viewport={"width": 1920, "height": 1080})
            page = context.new_page()

            print(f"⏳ Otevírám katalog Geberit: {self.target_url}", file=sys.stderr)
            page.goto(self.target_url, timeout=30000)
            page.wait_for_load_state("domcontentloaded")
            time.sleep(3)

            print("🍪 Zpracovávám Cookies...", file=sys.stderr)
            try:
                js_kill = """() => {
                    let btn = document.querySelector('#cmpbntyestxt');
                    if(btn) { btn.click(); return true; }
                    return false;
                }"""
                page.evaluate(js_kill)
            except: pass

            print("🔎 Sbírám odkazy na produkty...", file=sys.stderr)
            js_code = """() => {
                let links = Array.from(document.querySelectorAll('a'));
                let prodLinks = links.map(a => a.href).filter(href => href.includes('/product/PRO_'));
                return [...new Set(prodLinks)];
            }"""
            
            product_urls = page.evaluate(js_code)
            
            if not product_urls:
                print("   ❌ Žádné odkazy nebyly nalezeny.", file=sys.stderr)
                browser.close()
                return
                
            print(f"   📌 Nalezeno {len(product_urls)} odkazů. Analyzuji detaily:\n")

            for url in product_urls: 
                print(f"   ➡️ Otevírám: {url.split('/')[-1]}", file=sys.stderr)
                try:
                    page.goto(url, timeout=30000)
                    page.wait_for_load_state("domcontentloaded")
                    time.sleep(2) 
                    
                    # Otevření případných skrytých záložek "Technische Daten"
                    try:
                        tech_tab = page.locator("text='Technische Daten', text='Eigenschaften'").first
                        if tech_tab.is_visible(): tech_tab.click(timeout=1000); time.sleep(1)
                    except: pass
                    
                    # Název z JSON-LD
                    js_name = """() => {
                        let name = "";
                        document.querySelectorAll('script[type="application/ld+json"]').forEach(s => {
                            try { let d = JSON.parse(s.innerText); if(d.name) name = d.name; } catch(e){}
                        });
                        if(!name) { let h1 = document.querySelector('h1'); if(h1) name = h1.innerText; }
                        return name.trim();
                    }"""
                    h1_text = page.evaluate(js_name)
                    if not h1_text: h1_text = "Neznámý produkt Geberit"

                    page_text = page.locator("body").inner_text()
                    
                    # SKU
                    sku_match = re.search(r'(\d{3}\.\d{3}\.[a-zA-Z0-9]{2,3}\.\d)', page_text)
                    if not sku_match: sku_match = re.search(r'(\d{3}\.\d{3}\.\d{2}\.\d)', page_text)
                    sku = sku_match.group(1) if sku_match else "SKU_Nenalezeno"
                    
                    print(f"      ✅ Načteno: {h1_text[:50]}... (SKU: {sku})", file=sys.stderr)

                    # Přesná analýza
                    tech_data = self.analyze_geberit_surgical(page_text, h1_text)
                    
                    found_info = []
                    if tech_data["Flow_Rate_ls"]: found_info.append(f"Průtok: {tech_data['Flow_Rate_ls']}")
                    if tech_data["Length_mm"]: found_info.append(f"Délka: {tech_data['Length_mm']} mm")
                    if tech_data["Height_Min_mm"]: found_info.append(f"Výška: {tech_data['Height_Min_mm']} mm")
                    if tech_data["Outlet_Type"]: found_info.append(f"Trubka: {tech_data['Outlet_Type']}")
                    if tech_data["Material_Body"]: found_info.append(f"Materiál: {tech_data['Material_Body']}")
                    
                    if found_info:
                        print(f"         ⚙️ Získáno: {', '.join(found_info)}", file=sys.stderr)
                    else:
                        print(f"         ⚠️ Specifická technická data v textu nenalezena.", file=sys.stderr)
                    
                    record = {
                        "Brand": "Geberit", "Product_Name": h1_text, "Article_Number_SKU": sku,
                        "Product_URL": url, "Evidence_Text": page_text.replace('\n', ' ')[:200]
                    }
                    record.update(tech_data)
                    discovered_products.append(record)
                    
                except Exception as e:
                    print(f"      ❌ Chyba: {e}", file=sys.stderr)

            browser.close()
        
        print("\n" + "="*60)
        print(f"✅ Dokončeno. Zpracováno {len(discovered_products)} produktů.")
        print("="*60 + "\n", file=sys.stderr)
        self.save_to_excel(discovered_products)

    def save_to_excel(self, products):
        if not products: return
        print(f"💾 Ukládám {len(products)} záznamů do Excelu...", file=sys.stderr)
        with pd.ExcelWriter(self.excel_path, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
            try: start_row = writer.sheets['Products_Tech'].max_row
            except: start_row = 0
            df_prod = pd.DataFrame(products)
            for col in self.cols_tech:
                if col not in df_prod.columns: df_prod[col] = ""
            df_prod = df_prod[self.cols_tech]
            df_prod.to_excel(writer, sheet_name="Products_Tech", index=False, header=False, startrow=start_row)
        print("✅ Hotovo!", file=sys.stderr)

if __name__ == "__main__":
    scraper = GeberitDiscoveryV7()
    scraper.run()