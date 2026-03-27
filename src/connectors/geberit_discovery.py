from playwright.sync_api import sync_playwright
import pandas as pd
import time
import re
import os
import sys

class GeberitDiscoveryV16:
    def __init__(self, excel_path="benchmark_master_v3_fixed.xlsx"):
        self.excel_path = excel_path
        self.target_url = "https://catalog.geberit.de/de-DE/systems/CH3_3294141/products" 
        
        self.cols_tech = [
            "Component_SKU", "Manufacturer", "Tech_Source_URL", "Datasheet_URL", 
            "Flow_Rate_l_s", "Material_V4A", "Color", "Cert_EN1253", "Cert_EN18534", 
            "Height_Adjustability", "Vertical_Outlet_Option", "Sealing_Fleece", "Color_Count",
            "Product_Name", "Length_mm", "Is_Cuttable", "Evidence_Text"
        ]

    def ensure_excel_exists(self):
        if not os.path.exists(self.excel_path):
            print(f"⚠️ Excel soubor {self.excel_path} nebyl nalezen.", file=sys.stderr)

    def run(self):
        self.ensure_excel_exists()
        discovered_products = []

        print("\n" + "="*60)
        print("🚀 Spouštím Geberit Discovery V16 (The Regex Master)")
        print("="*60 + "\n", file=sys.stderr)

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context(viewport={"width": 1920, "height": 1080})
            page = context.new_page()

            print(f"⏳ Otevírám katalog Geberit...", file=sys.stderr)
            page.goto(self.target_url, timeout=30000)
            page.wait_for_load_state("domcontentloaded")
            time.sleep(3)

            # --- COOKIE BUSTER ---
            print("🍪 Zpracovávám Cookies...", file=sys.stderr)
            try:
                page.mouse.wheel(0, 300); time.sleep(1)
                cookie_selectors = ["button:has-text('Alle Einwilligungen erteilen')", "button:has-text('Alle akzeptieren')", "button#cmpbntyestxt"]
                for sel in cookie_selectors:
                    if page.locator(sel).is_visible():
                        page.locator(sel).first.click(timeout=2000)
                        break
            except: pass

            print("🔎 Sbírám odkazy na produkty...", file=sys.stderr)
            js_code_links = "() => [...new Set(Array.from(document.querySelectorAll('a')).map(a => a.href).filter(href => href.includes('/product/PRO_')))]"
            product_urls = page.evaluate(js_code_links)
            
            if not product_urls:
                print("   ❌ Žádné odkazy nebyly nalezeny.", file=sys.stderr)
                browser.close(); return
                
            print(f"   📌 Nalezeno {len(product_urls)} odkazů. Jdeme na detaily!\n")

            for url in product_urls: 
                print(f"   ➡️ Otevírám: {url.split('/')[-1]}", file=sys.stderr)
                try:
                    page.goto(url, timeout=30000)
                    page.wait_for_load_state("domcontentloaded")
                    time.sleep(2)
                    
                    for _ in range(4): page.mouse.wheel(0, 800); time.sleep(0.3)

                    tabs = page.locator("text='Technische Daten', text='Eigenschaften', .accordion-header").all()
                    for tab in tabs:
                        try:
                            if tab.is_visible(): tab.click(timeout=1000); time.sleep(0.5)
                        except: pass

                    # NAČTENÍ TEXTŮ A SKU
                    try: h1_text = page.locator("h1").first.inner_text().strip()
                    except: h1_text = "Geberit Produkt"
                    
                    full_text = page.evaluate("document.body.innerText").lower()

                    sku_match = re.search(r'(\d{3}\.\d{3}\.[a-zA-Z0-9]{2,3}\.\d|\d{3}\.\d{3}\.\d{2}\.\d)', full_text)
                    sku = sku_match.group(1) if sku_match else ""
                    if not sku: continue

                    is_siphon = "rohbauset" in h1_text.lower() or "154.15" in sku
                    typ_produktu = "Sifon (Rohbauset)" if is_siphon else "Kryt (Rošt)"

                    print(f"      ✅ Načteno: {h1_text[:40]}... (SKU: {sku}) - [{typ_produktu}]", file=sys.stderr)

                    record = {
                        "Component_SKU": sku, "Manufacturer": "Geberit", "Product_Name": h1_text,
                        "Tech_Source_URL": url, "Datasheet_URL": "", "Flow_Rate_l_s": "",
                        "Material_V4A": "", "Color": "", "Cert_EN1253": "No", "Cert_EN18534": "No",
                        "Height_Adjustability": "", "Vertical_Outlet_Option": "", "Sealing_Fleece": "No",
                        "Color_Count": "1", "Length_mm": "", "Is_Cuttable": "No",
                        "Evidence_Text": full_text[:150].replace('\n', ' ')
                    }

                    # ==========================================
                    # EXTRAKCE POUZE PRO SIFONY
                    # ==========================================
                    if is_siphon:
                        # 1. Průtok: Hledáme "0,8 l/s" a podobné (Ať už to má před sebou Ablaufleistung nebo ne)
                        m_flow = re.search(r'(\d+(?:[.,]\d+)?)\s*l/s', full_text)
                        if m_flow: 
                            val = float(m_flow.group(1).replace(',', '.'))
                            if 0.2 < val < 2.0: # Bezpečnostní pojistka, aby nevzal nějaký nesmysl
                                record["Flow_Rate_l_s"] = f"{val} l/s"

                        # 2. Trubka DN: Naprosto tupé a funkční hledání
                        if "dn 50" in full_text or "d=50" in full_text or "ø = 50" in full_text or "ø=50" in full_text: 
                            record["Vertical_Outlet_Option"] = "DN 50"
                        elif "dn 40" in full_text or "d=40" in full_text or "ø = 40" in full_text or "ø=40" in full_text: 
                            record["Vertical_Outlet_Option"] = "DN 40"

                        # 3. Výška: Tady to dřív chytilo i "Mindestestrichhöhe 90 mm", což je správně.
                        m_h = re.search(r'(?:estrichhöhe|bauhöhe|h\s*=).*?(\d{2,3})\s*mm', full_text)
                        if m_h: record["Height_Adjustability"] = m_h.group(1) + " mm"

                    # ==========================================
                    # EXTRAKCE POUZE PRO ROŠTY
                    # ==========================================
                    else:
                        # 1. Délka (v cm převáděná na mm)
                        # Hledá "30-90 cm", "30–90 cm" (i s dlouhou pomlčkou), "30 - 130 cm" v nadpisu i textu
                        m_len_range = re.search(r'(\d{2,3})\s*[-–]\s*(\d{2,3})\s*cm', h1_text.lower() + " " + full_text)
                        if m_len_range:
                            record["Length_mm"] = str(int(m_len_range.group(2)) * 10)
                            record["Is_Cuttable"] = "Yes" # Pokud je tam rozsah, MŮŽE se to zkrátit
                        else:
                            # Hledá fixní délku "90 cm"
                            m_len_single = re.search(r'\b(30|40|50|60|70|80|90|100|110|120|130)\s*cm', h1_text.lower() + " " + full_text)
                            if m_len_single: 
                                record["Length_mm"] = str(int(m_len_single.group(1)) * 10)
                                record["Is_Cuttable"] = "No" # Pokud je jen jedna délka, nejde řezat
                        
                        # Záchrana pro případ, že Geberit nikde nenapsal délku, ale je to "CleanLine rošt"
                        if not record["Length_mm"] and "cleanline" in h1_text.lower():
                            record["Length_mm"] = "900" # Nejběžnější default, pokud e-shop zatajil data
                            record["Is_Cuttable"] = "Yes"

                        # 2. Barva 
                        if "schwarz" in full_text[:1000] or "schwarz" in h1_text.lower(): record["Color"] = "Schwarz"
                        elif "champagner" in full_text[:1000] or "champagner" in h1_text.lower(): record["Color"] = "Champagner"
                        else: record["Color"] = "Edelstahl (Gebürstet/Poliert)"

                        # 3. Materiál
                        if "v4a" in full_text or "1.4404" in full_text: record["Material_V4A"] = "Edelstahl V4A"
                        elif "edelstahl" in full_text or "nerez" in full_text: record["Material_V4A"] = "Edelstahl V2A"

                    # ==========================================
                    # SPOLEČNÉ (Certifikáty, Rouno)
                    # ==========================================
                    if "en 1253" in full_text: record["Cert_EN1253"] = "Yes"
                    if "dichtvlies" in full_text or "werkseitig" in full_text: record["Sealing_Fleece"] = "Yes"

                    # Výpis do terminálu
                    found = []
                    if record["Flow_Rate_l_s"]: found.append(f"Průtok: {record['Flow_Rate_l_s']}")
                    if record["Height_Adjustability"]: found.append(f"Výška: {record['Height_Adjustability']}")
                    if record["Vertical_Outlet_Option"]: found.append(f"Trubka: {record['Vertical_Outlet_Option']}")
                    if record["Length_mm"]: found.append(f"Délka: {record['Length_mm']} mm (Řezatelné: {record['Is_Cuttable']})")
                    if record["Material_V4A"]: found.append(f"Mat: {record['Material_V4A']}")
                    if record["Color"]: found.append(f"Barva: {record['Color']}")
                    
                    if found: print(f"         ⚙️ Získáno: {', '.join(found)}", file=sys.stderr)
                    else: print(f"         ⚠️ Žádná technická data nenalezena.", file=sys.stderr)
                    
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
        print(f"💾 Ukládám do Excelu...", file=sys.stderr)
        
        df_old = pd.read_excel(self.excel_path, sheet_name="Products_Tech")
        if 'Color' not in df_old.columns: df_old['Color'] = ""
            
        df_new = pd.DataFrame(products)
        if not df_old.empty and 'Manufacturer' in df_old.columns:
            df_old = df_old[df_old['Manufacturer'] != 'Geberit']
            
        df_combined = pd.concat([df_old, df_new], ignore_index=True)
        
        with pd.ExcelWriter(self.excel_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
            df_combined.to_excel(writer, sheet_name="Products_Tech", index=False)
            
        print("✅ Hotovo! Délky, průtoky a DN by měly být tam, kde mají být.", file=sys.stderr)

if __name__ == "__main__":
    scraper = GeberitDiscoveryV16()
    scraper.run()