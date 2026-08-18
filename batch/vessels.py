import requests
from bs4 import BeautifulSoup
import re
import time
import datetime
import pandas as pd
import os
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


class Logger:
    def __init__(self):
        self.logger = logging.getLogger("vessels")

    def log(self, level, message):
        getattr(self.logger, level if level in {"debug", "info", "warning", "error"} else "debug")(message)



class VesselScraper:
    def __init__(self, delay=10):
        self.delay = delay
        self.website = "https://www.vesselfinder.com"
        self.type_urls = {
            "4": f"{self.website}/vessels?type=4&flag=EG&page={{page}}",
            "6": f"{self.website}/vessels?type=6&flag=EG&page={{page}}"
        }
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Referer": f"{self.website}/",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        }
        self.ports_cache = {}
        self.session = None
        self.vessels_df = pd.DataFrame()
        
        # Initialize logger
        self.logger = Logger()
        self.logger.log('info', 'VesselScraper initialized')

    def _get_page(self, url):
        """Fetch a web page with error handling."""
        maximum_attempts = 3
        for attempt in range(1, maximum_attempts + 1):
            try:
                self.logger.log('debug', f'Fetching URL: {url}')
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                time.sleep(self.delay)
                self.logger.log('debug', f'Successfully fetched URL: {url}')
                return response
            except requests.RequestException as error:
                self.logger.log(
                    'warning',
                    f'Attempt {attempt}/{maximum_attempts} failed for {url}: {error}',
                )
                if attempt == maximum_attempts:
                    self.logger.log('error', f'Error fetching {url}: {error}')
                    raise
                time.sleep(self.delay * attempt)

    def _get_total_pages(self, soup):
        """Extract total number of pages."""
        patterns = [
            r"page\s*1\s*/\s*(\d+)",
            r"Page\s*1\s*of\s*(\d+)",
            r"1\s*-\s*\d+\s*of\s*\d+\s*\(\s*(\d+)\s*pages?\)"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, soup.text, re.IGNORECASE)
            if match:
                total_pages = int(match.group(1))
                self.logger.log('debug', f'Found total pages: {total_pages}')
                return total_pages
        
        self.logger.log('warning', 'Could not determine total pages, defaulting to 1')
        return 1

    def _extract_vessel_data(self, soup, vessel_type):
        """Extract vessel information from page."""
        table = soup.find("table")
        if not table:
            self.logger.log('warning', 'No table found on page')
            return []
        
        vessels = []
        rows = table.find_all("tr")
        self.logger.log('debug', f'Found {len(rows)} rows in table')
        
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 5:
                continue
            
            link = cells[0].find("a", href=True)
            if not link:
                continue
            
            url = link["href"]
            if url.startswith("/"):
                url = self.website + url
            
            vessel_data = {}
            
            # Extract name and type
            text = cells[0].get_text().strip()
            parts = [p.strip() for p in text.split("\n") if p.strip()]
            if len(parts) >= 2:
                vessel_data["name"] = parts[0]
                vessel_data["type"] = parts[1]
            elif len(parts) == 1:
                vessel_data["name"] = parts[0]
                vessel_data["type"] = vessel_type
            else:
                vessel_data["name"] = "Unknown"
                vessel_data["type"] = vessel_type
            
            # Extract other data
            vessel_data["year_built"] = cells[1].get_text().strip() or "-"
            vessel_data["gross_tonnage"] = cells[2].get_text().strip() or "-"
            vessel_data["deadweight"] = cells[3].get_text().strip() or "-"
            
            # Extract size
            size_text = cells[4].get_text().strip()
            if "/" in size_text:
                length, beam = [p.strip() for p in size_text.split("/")]
                vessel_data["length(m)"] = length
                vessel_data["beam(m)"] = beam
            else:
                vessel_data["length(m)"] = size_text or "-"
                vessel_data["beam(m)"] = "-"
            
            vessel_data["detail_link"] = url
            vessels.append(vessel_data)
        
        self.logger.log('debug', f'Extracted {len(vessels)} vessels from page')
        return vessels

    def _format_eta(self, eta_str):
        """Format ETA string to standardized datetime."""
        if not eta_str or not eta_str.strip():
            return ""
        
        patterns = [
            r":\s*([A-Za-z]+)\s+(\d+)(?:,\s*(\d{4}))?[\s,]+(\d{2}:\d{2})",
            r"([A-Za-z]+)\s+(\d+)(?:,\s*(\d{4}))?[\s,]+(\d{2}:\d{2})"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, eta_str)
            if match:
                month_str, day, year, time_part = match.groups()
                year = year or str(datetime.datetime.now().year)
                try:
                    month_num = datetime.datetime.strptime(month_str, "%b").month
                    formatted_eta = f"{year}-{month_num:02d}-{int(day):02d} {time_part}"
                    self.logger.log('debug', f'Formatted ETA: {eta_str} -> {formatted_eta}')
                    return formatted_eta
                except ValueError:
                    continue
        
        self.logger.log('debug', f'Could not format ETA: {eta_str}')
        return ""

    def _get_port_location(self, response):
        """Extract port coordinates."""
        try:
            match = re.search(r"is located in .* at (\d+\.\d+[NS]), (\d+\.\d+[EW])", response.text)
            if match:
                coords = match.groups()
                self.logger.log('debug', f'Extracted port coordinates: {coords}')
                return coords
            else:
                self.logger.log('debug', 'No port coordinates found')
                return ("", "")
        except Exception as e:
            self.logger.log('error', f'Error extracting port coordinates: {e}')
            return "", ""

    def _get_destination_info(self, soup):
        """Extract destination port information."""
        dest_div = soup.find("div", class_="vi__r1 vi__sbt")
        if not dest_div:
            self.logger.log('debug', 'No destination information found')
            return {
                "arrival_date": "",
                "destination_port_country": "",
                "destination_port_name": "",
                "destination_port_lat": "",
                "destination_port_lon": ""
            }
        
        port = ["", ""]
        eta = ""
        href = ""
        lat = long = ""
        
        dest_link = dest_div.find("a", class_="_npNa")
        child_divs = dest_div.find_all("div")
        
        if dest_link:
            port_name = dest_link.get_text(strip=True)
            port = [p.strip() for p in port_name.split(",")]
            if len(port) == 1:
                port.append("")
            
            href = dest_link["href"]
            if href.startswith("/"):
                href = self.website + href
            
            if len(child_divs) >= 2:
                eta = self._format_eta(child_divs[1].get_text().strip())
        else:
            if len(child_divs) >= 2:
                port_text = child_divs[1].get_text()
                port_parts = port_text.split(",")
                if len(port_parts) >= 2:
                    port = [p.strip() for p in port_parts[:2]]
                elif len(port_parts) == 1:
                    port = [port_parts[0].strip(), ""]
                eta = self._format_eta(port_text)
        
        # Get coordinates
        if href and href != self.website:
            if href in self.ports_cache:
                lat, long = self.ports_cache[href]
                self.logger.log('debug', f'Retrieved port coordinates from cache: {port[0]}')
            else:
                try:
                    response = self._get_page(href)
                    lat, long = self._get_port_location(response)
                    self.ports_cache[href] = (lat, long)
                    self.logger.log('debug', f'Cached port coordinates for: {port[0]}')
                except Exception as e:
                    self.logger.log('error', f'Error getting port coordinates for {port[0]}: {e}')
                    lat = long = ""
        
        return {
            "arrival_date": eta,
            "destination_port_country": port[1] if len(port) > 1 else "",
            "destination_port_name": port[0],
            "destination_port_lat": lat,
            "destination_port_lon": long
        }

    def _get_departure_info(self, soup):
        """Extract departure port information."""
        dest_div = soup.find("div", class_="vi__r1 vi__stp")
        if not dest_div:
            self.logger.log('debug', 'No departure information found')
            return {
                "departure_date": "",
                "last_port_country": "",
                "last_port_name": ""
            }
        
        port = ["", ""]
        eta = ""
        
        dest_link = dest_div.find("a", class_="_npNa")
        child_divs = dest_div.find_all("div")
        
        if dest_link:
            port_name = dest_link.get_text(strip=True)
            port = [p.strip() for p in port_name.split(",")]
            if len(port) == 1:
                port.append("")
            
            if len(child_divs) >= 2:
                eta = self._format_eta(child_divs[1].get_text().strip())
        else:
            if len(child_divs) >= 2:
                port_text = child_divs[1].get_text()
                port_parts = port_text.split(",")
                if len(port_parts) >= 2:
                    port = [p.strip() for p in port_parts[:2]]
                elif len(port_parts) == 1:
                    port = [port_parts[0].strip(), ""]
                eta = self._format_eta(port_text)
        
        return {
            "departure_date": eta,
            "last_port_country": port[1] if len(port) > 1 else "",
            "last_port_name": port[0]
        }

    def _get_report_status(self, soup):
        """Extract vessel status and report time."""
        try:
            table = soup.find("table")
            if not table:
                return {"reported_status": "", "report_date": ""}
            
            info = table.get_text(separator="\n", strip=True)
            status_match = re.search(r"Navigation Status\n(.*)\n", info)
            status = status_match.group(1).strip() if status_match else ""
            
            svg = table.find("svg", class_="ttt1 info")
            report_time = ""
            if svg and svg.has_attr("data-title"):
                report_time = self._format_eta(svg["data-title"])
            
            return {
                "reported_status": status,
                "report_date": report_time
            }
        except Exception as e:
            self.logger.log('error', f'Error extracting report status: {e}')
            return {"reported_status": "", "report_date": ""}

    def _get_vessel_details(self, url):
        """Get detailed information for a vessel."""
        try:
            response = self._get_page(url)
            soup = BeautifulSoup(response.text, "html.parser")
            
            details = {}
            details.update(self._get_departure_info(soup))
            details.update(self._get_destination_info(soup))
            details.update(self._get_report_status(soup))
            
            self.logger.log('debug', f'Successfully extracted vessel details from: {url}')
            return details
        except Exception as e:
            self.logger.log('error', f'Error getting vessel details from {url}: {e}')
            return {
                "departure_date": "",
                "last_port_country": "",
                "last_port_name": "",
                "arrival_date": "",
                "destination_port_country": "",
                "destination_port_name": "",
                "destination_port_lat": "",
                "destination_port_lon": "",
                "reported_status": "",
                "report_date": ""
            }

    def _save_to_csv(self, output_file):
        """Save DataFrame to CSV file."""
        if not self.vessels_df.empty:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            self.vessels_df.to_csv(output_file, index=False, encoding='utf-8')
            self.logger.log('info', f'Successfully saved {len(self.vessels_df)} vessels to {output_file}')
        else:
            self.logger.log('warning', 'No data to save')

    def scrape(self, vessel_types=["4", "6"], output_file=None):
        """Main scraping method."""
        if output_file is None:
            output_file = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "data",
                "vessels",
                f"vessels_{datetime.datetime.now().strftime('%Y-%m-%d')}.csv",
            )
        
        self.logger.log('info', f'Starting vessel scraping for types: {vessel_types}')
        self.logger.log('info', f'Output file: {output_file}')
        
        vessels_data = []
        vessels_processed = 0
        total_pages_processed = 0
        start_time = time.time()
        
        with requests.Session() as session:
            session.headers.update(self.headers)
            self.session = session
            
            try:    
                for vessel_type in vessel_types:
                    self.logger.log('info', f'Processing vessel type {vessel_type}...')
                    
                    # Get first page to determine total pages
                    url = self.type_urls[vessel_type].format(page=1)
                    response = self._get_page(url)
                    soup = BeautifulSoup(response.text, "html.parser")
                    
                    total_pages = self._get_total_pages(soup)
                    self.logger.log('info', f'Type {vessel_type}: {total_pages} pages to process')
                    
                    # Process all pages
                    for page in range(1, total_pages + 1):
                        self.logger.log('info', f'Processing page {page}/{total_pages} for type {vessel_type}')
                        
                        if page > 1:
                            url = self.type_urls[vessel_type].format(page=page)
                            response = self._get_page(url)
                            soup = BeautifulSoup(response.text, "html.parser")
                        
                        page_vessels = self._extract_vessel_data(soup, vessel_type)
                        self.logger.log('info', f'Extracted {len(page_vessels)} vessels from page {page}')
                        
                        for vessel in page_vessels:
                            vessels_processed += 1
                            vessel_name = vessel["name"]
                            vessel_url = vessel["detail_link"]
                            
                            self.logger.log('info', f'Processing vessel {vessels_processed}: {vessel_name}')
                            
                            # Get detailed information and merge
                            vessel_details = self._get_vessel_details(vessel_url)
                            vessel.update(vessel_details)
                            
                            # Add to collection
                            vessels_data.append(vessel)
                        
                        total_pages_processed += 1
                        
                        # Log progress every 5 pages
                        if total_pages_processed % 5 == 0:
                            elapsed_time = time.time() - start_time
                            self.logger.log('info', f'Progress: {total_pages_processed} pages processed, {vessels_processed} vessels collected in {elapsed_time:.2f} seconds')
                            
            except Exception as e:
                self.logger.log('error', f'Fatal error during scraping: {e}')
                raise
            
            finally:
                self.session = None
        
        # Create DataFrame from collected data
        if vessels_data:
            self.vessels_df = pd.DataFrame(vessels_data)
            self.logger.log('info', f'Created DataFrame with {len(self.vessels_df)} rows and {len(self.vessels_df.columns)} columns')

            # VesselFinder's broad tanker category also includes water tankers.
            # The case study requires cargo and oil-related tanker vessels only.
            water_tankers = self.vessels_df["type"].str.contains(
                "Water Tanker",
                case=False,
                na=False,
            )
            excluded_count = int(water_tankers.sum())
            if excluded_count:
                self.vessels_df = self.vessels_df.loc[~water_tankers].copy()
                self.logger.log(
                    'info',
                    f'Excluded {excluded_count} water tankers from the final dataset',
                )
            
            # Ensure proper column order
            desired_columns = [
                "name", "type", "year_built", "gross_tonnage", "deadweight", 
                "length(m)", "beam(m)", "detail_link",
                "departure_date", "last_port_country", "last_port_name",
                "arrival_date", "destination_port_country", "destination_port_name",
                "destination_port_lat", "destination_port_lon",
                "reported_status", "report_date"
            ]
            
            # Reorder columns and fill missing ones
            for col in desired_columns:
                if col not in self.vessels_df.columns:
                    self.vessels_df[col] = ""
            
            self.vessels_df = self.vessels_df[desired_columns]
            self.logger.log('info', f'Reordered DataFrame columns: {list(self.vessels_df.columns)}')
        else:
            self.vessels_df = pd.DataFrame()
            self.logger.log('warning', 'No vessels data collected')
        
        # Save DataFrame to CSV
        self._save_to_csv(output_file)
        
        # Final statistics
        elapsed_time = time.time() - start_time
        self.logger.log('info', f'Scraping completed successfully!')
        self.logger.log('info', f'Total vessels processed: {len(self.vessels_df)}')
        self.logger.log('info', f'Total pages processed: {total_pages_processed}')
        self.logger.log('info', f'Total execution time: {elapsed_time:.2f} seconds')
        self.logger.log('info', f'Average time per vessel: {elapsed_time/max(1, len(self.vessels_df)):.2f} seconds')
        self.logger.log('info', f'Ports cache size: {len(self.ports_cache)} entries')
        

    def get_vessels_data(self):
        """Return collected vessels data as DataFrame."""
        return self.vessels_df


if __name__ == "__main__":
    scraper = VesselScraper(delay=float(os.getenv("VESSEL_REQUEST_DELAY", "2")))
    scraper.scrape()
