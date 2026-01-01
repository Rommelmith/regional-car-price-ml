import requests
from bs4 import BeautifulSoup
import csv
import json
import time
from requests.exceptions import RequestException, Timeout, ReadTimeout

SEARCH_URL = "https://www.pakwheels.com/used-cars/search/-/"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

MAX_RETRIES = 5
BASE_DELAY = 2
SAVE_INTERVAL = 20


def extract_city_from_description(desc: str) -> str:
    desc_lower = desc.lower()
    cities = [
        "karachi", "lahore", "islamabad", "rawalpindi",
        "faisalabad", "multan", "peshawar", "quetta",
        "sialkot", "gujranwala", "hyderabad", "bahawalpur"
    ]
    for city in cities:
        if city in desc_lower:
            return city.title()
    return ""


def parse_engine_specs(engine_text: str):
    parts = [p.strip() for p in engine_text.split(' . ')]
    fuel_type = ""
    engine_capacity = ""
    transmission = ""
    if len(parts) >= 1:
        fuel_type = parts[0]
    if len(parts) >= 2:
        engine_capacity = parts[1]
    if len(parts) >= 3:
        transmission = parts[2]
    return fuel_type, engine_capacity, transmission


def scrape_page(session: requests.Session, page: int):
    for attempt in range(MAX_RETRIES):
        try:
            print(f"Scraping page {page}... (attempt {attempt + 1}/{MAX_RETRIES})")
            params = {"page": page}
            timeout = 20 + (attempt * 10)
            resp = session.get(SEARCH_URL, headers=HEADERS, params=params, timeout=timeout)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")
            cars = []
            for li in soup.select("ul.search-results li.classified-listing"):
                script_tag = li.find("script", attrs={"type": "application/ld+json"})
                if not script_tag or not script_tag.string:
                    continue
                try:
                    data = json.loads(script_tag.string)
                except Exception:
                    continue
                title = data.get("name", "").strip()
                desc = data.get("description", "").strip()
                year = data.get("modelDate", "")
                offers = data.get("offers", {}) or {}
                price = offers.get("price", "")
                currency = offers.get("priceCurrency", "PKR")
                link = offers.get("url", "")
                city = extract_city_from_description(desc)
                if isinstance(year, (int, float)):
                    year = str(year)
                if isinstance(price, (int, float)):
                    price_str = f"{currency} {price:,}"
                else:
                    price_str = str(price)
                mileage = ""
                color = ""
                registered_in = ""
                fuel_type = ""
                transmission = ""
                engine_capacity = ""
                specs_ul = li.select_one("ul.ad-specs")
                if specs_ul:
                    for spec_li in specs_ul.find_all("li"):
                        icon = spec_li.find("i")
                        classes = icon.get("class", []) if icon else []
                        text = spec_li.get_text(strip=True)
                        if any("pw-mileage" in c for c in classes):
                            mileage = text
                        elif any("pw-color" in c for c in classes):
                            color = text
                        elif any("pw-registration" in c for c in classes):
                            registered_in = text
                        elif any("pw-engine" in c for c in classes):
                            fuel_type, engine_capacity, transmission = parse_engine_specs(text)
                cars.append({
                    "title": title,
                    "price": price_str,
                    "city": city,
                    "year": year,
                    "mileage": mileage,
                    "color": color,
                    "registered_in": registered_in,
                    "fuel_type": fuel_type,
                    "engine_capacity": engine_capacity,
                    "transmission": transmission,
                    "link": link,
                })
            print(f"  -> found {len(cars)} cars on this page")
            return cars
        except (ReadTimeout, Timeout):
            wait_time = BASE_DELAY * (2 ** attempt)
            print(f"  ⚠️  Timeout on page {page} (attempt {attempt + 1}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES - 1:
                print(f"  ⏳ Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            else:
                print(f"  ❌ Failed to scrape page {page} after {MAX_RETRIES} attempts")
                return []
        except RequestException as e:
            print(f"  ⚠️  Request error on page {page}: {type(e).__name__}")
            wait_time = BASE_DELAY * (2 ** attempt)
            if attempt < MAX_RETRIES - 1:
                print(f"  ⏳ Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            else:
                print(f"  ❌ Failed to scrape page {page} after {MAX_RETRIES} attempts")
                return []
        except Exception as e:
            print(f"  ❌ Unexpected error on page {page}: {type(e).__name__}: {e}")
            return []
    return []


def save_to_csv(cars, filename):
    fieldnames = [
        "title",
        "price",
        "city",
        "year",
        "mileage",
        "color",
        "registered_in",
        "fuel_type",
        "engine_capacity",
        "transmission",
        "link",
    ]
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(cars)
    print(f"💾 Saved to {filename}")


def main():
    all_cars = []
    session = requests.Session()

    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    start = time.perf_counter()

    page = 1
    consecutive_empty = 0
    MAX_PAGES = 2000

    print("🚀 Starting PakWheels scraper...")
    print(f"   Settings: Max retries={MAX_RETRIES}, Save interval={SAVE_INTERVAL} pages\n")
    print(f"   Target pages: {MAX_PAGES}\n")

    try:
        while page <= MAX_PAGES:
            cars = scrape_page(session, page)

            if not cars:
                print(f"⚠️  No cars found on page {page}")
                consecutive_empty += 1
                page += 1
                time.sleep(2)
                continue

            consecutive_empty = 0
            all_cars.extend(cars)
            print(f"📊 Total collected so far: {len(all_cars)}")

            if page % SAVE_INTERVAL == 0:
                backup_file = f"pakwheels_cars_backup_page{page}.csv"
                save_to_csv(all_cars, backup_file)
                print(f"✅ Checkpoint saved at page {page}\n")

            page += 1
            time.sleep(1.5)

    except KeyboardInterrupt:
        print("\n\n⚠️  Scraping interrupted by user!")
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {type(e).__name__}: {e}")
    finally:
        print(f"\n{'='*60}")
        print(f"📈 Final Statistics:")
        print(f"   Total cars collected: {len(all_cars)}")
        print(f"   Pages scraped: {page - 1}")
        end = time.perf_counter()
        elapsed = end - start
        print(f"   Total time: {elapsed:.2f} seconds ({elapsed / 60:.2f} minutes)")
        print(f"{'='*60}\n")

        if all_cars:
            save_to_csv(all_cars, "pakwheels_cars_final.csv")
            print("\n✅ Scraping complete!")
        else:
            print("\n⚠️  No data collected")


if __name__ == "__main__":
    main()
