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
import os
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('scraping.log')
    ]
)

def setup_driver():
    """Set up Chrome WebDriver with proper options."""
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

def wait_for_element(driver, by, selector, timeout=20):
    """Wait for an element to be present."""
    try:
        element = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((by, selector))
        )
        return element
    except TimeoutException:
        logging.error(f"Timeout waiting for element: {selector}")
        raise

def handle_cookie_dialog(driver):
    """Handle the cookie dialog if it appears."""
    try:
        cookie_button = driver.find_element(By.CSS_SELECTOR, "button.accept-cookies")
        if cookie_button.is_displayed() and cookie_button.is_enabled():
            cookie_button.click()
            logging.info("Cookie dialog accepted.")
            time.sleep(2)  # Wait for the dialog to disappear
    except NoSuchElementException:
        logging.info("No cookie dialog found.")

def click_next_button(driver):
    """Click the 'Next' button with retries."""
    for attempt in range(3):
        try:
            next_button = driver.find_element(By.CSS_SELECTOR, "li.pagination__item.-arrow.-next a")
            next_button.click()
            time.sleep(2)  # Wait for the next page to load
            return True
        except Exception as e:
            logging.warning(f"Retry {attempt + 1}: Failed to click 'Next' button: {str(e)}")
            time.sleep(2)
    logging.error("Failed to click 'Next' button after 3 attempts.")
    return False

def scrape_page(driver, url, page_num):
    """Scrape a single page."""
    products = []
    try:
        driver.get(url)
        
        # Handle the cookie dialog if it appears
        handle_cookie_dialog(driver)
        
        wait_for_element(driver, By.CSS_SELECTOR, "div.custom__table-wrapper table")
        
        rows = driver.find_elements(By.CSS_SELECTOR, "div.custom__table-wrapper table tr[data-course-id]")
        for row in rows:
            try:
                name_element = row.find_element(By.CSS_SELECTOR, "td a")
                name = name_element.text.strip()
                full_url = name_element.get_attribute("href")
                
                # Handle missing data gracefully
                remote = "Yes" if row.find_elements(By.CSS_SELECTOR, "td:nth-child(2) .catalogue__circle.-yes") else "No"
                adaptive = "Yes" if row.find_elements(By.CSS_SELECTOR, "td:nth-child(3) .catalogue__circle.-yes") else "No"
                test_type = row.find_element(By.CSS_SELECTOR, "td:nth-child(4)").text.strip()
                
                products.append({
                    "Page": page_num,
                    "Assessment Name": name,
                    "URL": full_url,
                    "Remote Testing Support": remote,
                    "Adaptive/IRT Support": adaptive,
                    "Test Type": test_type
                })
            except NoSuchElementException:
                logging.warning(f"Missing data in row on page {page_num}")
                continue
    except Exception as e:
        logging.error(f"Error scraping page {page_num}: {str(e)}")
    return products

def scrape_all_pages():
    """Scrape all pages of the product catalog."""
    driver = None
    products = []
    base_url = "https://www.shl.com/solutions/products/product-catalog/"
    page = 1

    try:
        driver = setup_driver()
        while True:
            logging.info(f"Scraping page {page}...")
            url = f"{base_url}?start={(page-1)*12}" if page > 1 else base_url
            page_products = scrape_page(driver, url, page)
            
            if not page_products:
                logging.info(f"No products found on page {page}. Assuming end of catalog.")
                break
            
            products.extend(page_products)
            
            if not click_next_button(driver):
                break
        
        # Save data to CSV
        if products:
            df = pd.DataFrame(products)
            os.makedirs("data", exist_ok=True)
            output_file = "data/shl_catalog_all.csv"
            df.to_csv(output_file, index=False)
            logging.info(f"Scraping complete. Data saved to {output_file}")
        else:
            logging.warning("No products were scraped.")
    except Exception as e:
        logging.error(f"Scraping failed: {str(e)}")
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    scrape_all_pages()
