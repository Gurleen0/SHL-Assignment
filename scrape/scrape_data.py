from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import logging
import sys
import os
import time
from tabulate import tabulate

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('scraping.log')
    ]
)

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--disable-popup-blocking")
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(30)
        return driver
    except Exception as e:
        logging.error(f"Failed to initialize Chrome driver: {str(e)}")
        raise

def wait_for_element(driver, by, selector, timeout=20, retries=3):
    for attempt in range(retries):
        try:
            element = WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((by, selector))
            )
            return element
        except TimeoutException:
            if attempt == retries - 1:
                raise
            logging.warning(f"Timeout waiting for element {selector}. Attempt {attempt + 1} of {retries}")
            time.sleep(2)

def clean_text(text):
    if not text:
        return ""
    return " ".join(text.strip().split())

def get_yes_no_status(row, position):
    try:
        row.find_element(By.CSS_SELECTOR, f"td:nth-child({position}) .catalogue__circle.-yes")
        return True
    except NoSuchElementException:
        return False

def get_test_codes(row):
    try:
        # Try to get codes as badges, fallback to text
        codes = [clean_text(key.text) for key in row.find_elements(
            By.CSS_SELECTOR, "td.product-catalogue__keys .product-catalogue__key")]
        if not codes:
            # fallback: split by lines in the 4th column
            text = row.find_element(By.CSS_SELECTOR, "td:nth-child(4)").text
            codes = [clean_text(t) for t in text.splitlines() if t.strip()]
        valid_codes = [code for code in codes if code]
        return ", ".join(sorted(set(valid_codes))) if valid_codes else "N/A"
    except Exception as e:
        logging.warning(f"Error getting test codes: {str(e)}")
        return "N/A"

def standardize_url(url):
    if not url:
        return ""
    url = url.strip()
    if not url.startswith(('http://', 'https://')):
        url = f"https://www.shl.com{url}"
    return url

def scrape_page(driver, url, page_num):
    products = []
    max_retries = 3
    for attempt in range(max_retries):
        try:
            driver.get(url)
            wait_for_element(driver, By.CSS_SELECTOR, "div.custom__table-wrapper table")
            tables = driver.find_elements(By.CSS_SELECTOR, "div.custom__table-wrapper table")
            for table in tables:
                rows = table.find_elements(By.CSS_SELECTOR, "tr[data-course-id], tr[data-entity-id]")
                for row in rows:
                    try:
                        name_element = row.find_element(By.CSS_SELECTOR, "td a")
                        product_data = {
                            "page": page_num,
                            "assessment_name": clean_text(name_element.text),
                            "url": standardize_url(name_element.get_attribute('href')),
                            "remote_testing": get_yes_no_status(row, 2),
                            "adaptive_irt_support": get_yes_no_status(row, 3),
                            "test_type": get_test_codes(row),
                            "id": row.get_attribute("data-course-id") or row.get_attribute("data-entity-id")
                        }
                        products.append(product_data)
                    except Exception as e:
                        logging.warning(f"Error processing row on page {page_num}: {str(e)}")
                        continue
            return products
        except (TimeoutException, WebDriverException) as e:
            if attempt == max_retries - 1:
                logging.error(f"Failed to scrape page {page_num} after {max_retries} attempts: {str(e)}")
                return []
            logging.warning(f"Retrying page {page_num}. Attempt {attempt + 1} of {max_retries}")
            time.sleep(5)
    return products

def scrape_all_shl_products():
    driver = None
    products = []
    base_url = "https://www.shl.com/products/product-catalog/"
    output_file = "data/shl_catalog_all.csv"
    existing_ids = set()
    existing_df = None

    # Step 1: Load existing data if file exists
    if os.path.exists(output_file):
        existing_df = pd.read_csv(output_file, dtype=str)
        if 'id' in existing_df.columns:
            existing_ids = set(existing_df['id'].astype(str))
        else:
            existing_ids = set()
    else:
        existing_ids = set()

    try:
        driver = setup_driver()
        page = 1
        while True:
            logging.info(f"Scraping page {page}...")
            url = f"{base_url}?start={(page-1)*12}" if page > 1 else base_url
            page_products = scrape_page(driver, url, page)
            if not page_products:
                logging.info(f"No products found on page {page}. Assuming end of catalog.")
                break
            # Step 2: Filter out products with IDs already in CSV
            new_products = [prod for prod in page_products if str(prod['id']) not in existing_ids]
            if not new_products:
                logging.info(f"All products on page {page} already exist. Stopping incremental scrape.")
                break
            products.extend(new_products)
            # Add new IDs to the set to avoid duplicates in the same run
            existing_ids.update(str(prod['id']) for prod in new_products)
            try:
                next_button = driver.find_element(By.CSS_SELECTOR, "li.pagination__item.-arrow.-next a")
                if not next_button.is_displayed() or not next_button.is_enabled():
                    break
                page += 1
                time.sleep(2)
            except NoSuchElementException:
                break

        # Step 3: Combine with existing data and save
        if products:
            new_df = pd.DataFrame(products)
            if existing_df is not None:
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                combined_df = new_df
            combined_df = combined_df.drop_duplicates(subset=["id"], keep='first')
            combined_df = combined_df.sort_values(by=["assessment_name"]).reset_index(drop=True)
            combined_df = combined_df[['id', 'assessment_name', 'url', 'remote_testing', 'adaptive_irt_support', 'test_type']]
            os.makedirs("data", exist_ok=True)
            combined_df.to_csv(output_file, index=False)
            logging.info(f"Successfully added {len(new_df)} new products. Total: {len(combined_df)}")
            print("\nFirst few rows of the table (pandas DataFrame):")
            print(combined_df.head())
            print(tabulate(combined_df.head(), headers='keys', tablefmt='psql'))
            return combined_df
        else:
            logging.info("No new products found to add.")
            return existing_df if existing_df is not None else None
    except Exception as e:
        logging.error(f"Scraping failed: {str(e)}")
        raise
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    try:
        product_data = scrape_all_shl_products()
        if product_data is not None:
            print("\nFirst few products:")
            print(product_data.head())
    except Exception as e:
        logging.error(f"Script failed: {str(e)}")
        sys.exit(1)
