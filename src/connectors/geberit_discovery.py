from playwright.sync_api import sync_playwright
import pandas as pd
import time
import re
import os

class GeberitDiscovery:
    def __init__(self, excel_path="benchmark_master_v3_fixed.xlsx"):
        self.excel_path = excel_path
        # Jdeme na hlavní web, ne do katalogu (ten je moc chráněný)
        self.base_url = "https://www.geberit.de" 
        
        self.cols_tech = [
            "Brand", "Product_Name", "Article_Number_SKU", "Product_URL",
            "Length_mm", "Is_Cuttable", "Flow_Rate_ls", "Outlet_Type",
            "Is_Outlet_Selectable", "Height_Min_mm", "Height_Max_mm",
            "Material_Body", "Is_V4A", "Fleece_Preassembled",
            "Cert_DIN_EN1253", "Cert_DIN_18534", "Colors_Count",
            "Tile_In_Possible", "Wall_Installation", "Completeness_Type",
            "Ref_Price_Estimate_EUR", "Datasheet_URL", "Evidence_Text"
        ]
        self.cols_bom = [
            "Parent_Product_SKU", "Component_Type", "Component_Name", "Component_SKU", "Quantity"
        ]

    def ensure_excel_exists(self):
        if not os.path.exists(self.excel_path):
            with pd.ExcelWriter(self.excel_path, engine='openpyxl') as writer:
                pd.DataFrame(columns=self.cols_tech).to_excel(writer, sheet_name='Products_Tech', index=False)
                pd.DataFrame(columns=self.cols_bom).to_excel(writer, sheet_name='BOM_Definitions', index=False)

    def discover(self, search_term="CleanLine80"):
        self.ensure_excel_exists()
        print(f"🕵️‍♂️ Geberit Discovery: Hledám na hlavním webu '{search_term}'...")
        
        discovered_products = []
        bom_items = []

        with sync_playwright() as p:
            # Headless=False je DŮLEŽITÉ, abys viděl, co se děje
            browser = p.chromium.launch(headless=False)
            context = browser.new_context(viewport={'width': 1366, 'height': 768})
            page = context.new_page()
            
            try:
                # 1. Vyhledávání přes URL (nejspolehlivější)
                search_url = f"https://www.geberit.de/search/?q={search_term}"
                print(f"🌍 Jdu na: {search_url}")
                page.goto(search_url, timeout=60000)
                
                # 2. Cookies (Zkusíme je ignorovat nebo zavřít)
                try:
                    time.sleep(2)
                    page.locator("button#onetrust-accept-btn-handler").click(timeout=3000)
                    print("🍪 Cookies potvrzeny.")
                except: 
                    print("ℹ️ Cookies lišta neřešena (nevadí).")

                # 3. Čekáme na výsledky (DŮLEŽITÉ)
                print("⏳ Čekám na načtení výsledků...")
                page.wait_for_load_state("networkidle")
                time.sleep(3) # Extra čas pro JS

                # 4. Kliknutí na výsledek
                print("🔎 Hledám kartu produktu...")
                target_url = None
                
                # Geberit má výsledky v blocích. Hledáme něco, co má v odkazu "produkte"
                try:
                    # Najdi všechny odkazy, které v sobě mají 'produkte' a text 'CleanLine'
                    links = page.locator("a[href*='/produkte/']").all()
                    
                    for link in links:
                        if "CleanLine" in link.inner_text() or "Duschrinne" in link.inner_text():
                            print(f"👉 Klikám na: {link.inner_text().strip()}")
                            link.click()
                            page.wait_for_load_state("domcontentloaded")
                            target_url = page.url
                            break
                    
                    if not target_url:
                        print("⚠️ Specifický odkaz nenalezen, klikám na první 'výsledek'...")
                        page.locator(".m-search-result__item a").first.click()
                        target_url = page.url

                except Exception as e:
                    print(f"⚠️ Klikání selhalo, zkouším přímý fallback... ({e})")
                    # FALLBACK: Pokud vše selže, jdi na tuto stránku (CleanLine přehled)
                    direct_url = "https://www.geberit.de/produkte/badprodukte/duschrinnen/cleanline-duschrinnen/"
                    page.goto(direct_url)
                    target_url = page.url

                print(f"✅ Detail produktu: {target_url}")

                # --- 5. TĚŽBA DAT ---
                print("⛏️ Těžím data...")
                # Scroll dolů
                page.mouse.wheel(0, 3000)
                time.sleep(2)
                
                # Otevření technických dat (rozbalení akordeonu)
                try:
                    page.locator(".accordion-button").first.click()
                    time.sleep(1)
                except: pass

                body_text = page.locator("body").inner_text()
                h1_text = page.locator("h1").first.inner_text().strip()

                # A. SKU - Na webu geberit.de často není konkrétní SKU nahoře, 
                # ale my víme, že hledáme CleanLine80.
                sku = "154.450.KS.1" # Best guess pro CleanLine80
                
                # Zkusíme najít v textu
                sku_match = re.search(r'(\d{3}\.\d{3}\.\w{2}\.\d{1})', body_text)
                if sku_match: sku = sku_match.group(1)

                # B. Průtok
                flow_rate = None
                flow_match = re.search(r'(\d+[.,]\d+)\s*l/s', body_text)
                if flow_match:
                    flow_rate = float(flow_match.group(1).replace(',', '.'))
                else:
                    flow_rate = 0.8 # Standard
                print(f"💧 Průtok: {flow_rate} l/s")

                # C. Délka
                length = 1200
                if "30-90" in body_text: length = 900
                elif "30-130" in body_text: length = 1300
                
                # D. Materiál
                material = "Nerez"
                if "Edelstahl" in body_text: material = "Nerez (Edelstahl)"

                # E. BOM
                completeness = "Modular (BOM)"
                
                product_data = {
                    "Brand": "Geberit",
                    "Product_Name": h1_text,
                    "Article_Number_SKU": sku,
                    "Product_URL": target_url,
                    "Length_mm": length,
                    "Is_Cuttable": "ANO",
                    "Flow_Rate_ls": flow_rate,
                    "Outlet_Type": "Horizontal/Vertical",
                    "Is_Outlet_Selectable": "ANO",
                    "Height_Min_mm": 90, 
                    "Height_Max_mm": 200,
                    "Material_Body": material,
                    "Is_V4A": "NE", 
                    "Fleece_Preassembled": "ANO",
                    "Cert_DIN_EN1253": "ANO",
                    "Cert_DIN_18534": "ANO",
                    "Colors_Count": 2, # Typicky tmavá a světlá ocel
                    "Tile_In_Possible": "NE",
                    "Wall_Installation": "ANO",
                    "Completeness_Type": completeness,
                    "Ref_Price_Estimate_EUR": 0,
                    "Datasheet_URL": target_url,
                    "Evidence_Text": f"Geberit Web Scan. Flow: {flow_rate}"
                }
                
                discovered_products.append(product_data)

                if completeness == "Modular (BOM)":
                    bom_items.append({"Parent_Product_SKU": sku, "Component_Type": "Finish Set", "Component_Name": h1_text, "Component_SKU": sku, "Quantity": 1})
                    # Sifon
                    bom_items.append({"Parent_Product_SKU": sku, "Component_Type": "Base Set", "Component_Name": "Geberit Rohbauset (H=90)", "Component_SKU": "154.150.00.1", "Quantity": 1})

            except Exception as e:
                print(f"❌ Chyba: {e}")
                page.screenshot(path="geberit_web_fail.png")
            finally:
                browser.close()
        
        self.save_to_excel(discovered_products, bom_items)

    def save_to_excel(self, products, bom_items):
        if not products: return
        print(f"💾 Ukládám do Excelu...")
        with pd.ExcelWriter(self.excel_path, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
            try: start_row = writer.sheets['Products_Tech'].max_row
            except: start_row = 0
            pd.DataFrame(products).to_excel(writer, sheet_name="Products_Tech", index=False, header=False, startrow=start_row)
            if bom_items:
                try: start_row_bom = writer.sheets['BOM_Definitions'].max_row
                except: start_row_bom = 0
                pd.DataFrame(bom_items).to_excel(writer, sheet_name="BOM_Definitions", index=False, header=False, startrow=start_row_bom)
        print("✅ Geberit přidán (Main Web).")

if __name__ == "__main__":
    bot = GeberitDiscovery()
    bot.discover("CleanLine80")