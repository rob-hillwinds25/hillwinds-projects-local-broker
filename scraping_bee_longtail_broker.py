import csv
import os
import re
import threading

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from scraping_bee import ScrapingBee
from time import sleep
from typing import List, Dict, Any, Optional

_csv_lock = threading.Lock()

CSV_COLUMNS = [
    "agency_name",
    "agency_website",
    "agency_street_address",
    "agency_city",
    "agency_state",
    "agency_zip_code",
    "agency_google_rating",
    "agency_number_google_ratings",
    "agency_google_map_category",
    "agency_phone_number",
    "agency_google_cid",
    "agency_insert_category",
    "search_query",
]


class ScrapingBeeLongtailBrokerGoogleSearch(ScrapingBee):
    def _split_us_address(self, address: str) -> Dict[str, Optional[str]]:
        """
        Split an address like:
          "12058 San Jose Blvd STE 304, Jacksonville, FL 32223"
          "1819 Hendricks Ave Ste 3, Jacksonville, Florida 32207"
        into street, city, state, zip.
        Keeps state abbreviations exactly as they appear.
        """
        street = city = state = zip_code = None
        if not address:
            return {
                "agency_street_address": street,
                "agency_city": city,
                "agency_state": state,
                "agency_zip_code": zip_code,
            }

        parts = [p.strip() for p in address.split(",")]
        if len(parts) >= 3:
            street = ", ".join(parts[:-2])
            city = parts[-2]
            state_zip = parts[-1]
        elif len(parts) == 2:
            street = parts[0]
            state_zip = parts[1]
        else:
            street = address
            state_zip = ""

        # Match "ST 12345" pattern
        m = re.search(r"([A-Za-z]{2})\s+(\d{5}(?:-\d{4})?)$", state_zip)
        if m:
            state = m.group(1).upper()
            zip_code = m.group(2)
            return {
                "agency_street_address": street,
                "agency_city": city,
                "agency_state": state,
                "agency_zip_code": zip_code,
            }

        # Otherwise extract zip if present
        m = re.search(r"(\b\d{5}(?:-\d{4})?\b)$", state_zip)
        if m:
            zip_code = m.group(1)
            state_token = state_zip[: m.start()].strip()
        else:
            state_token = state_zip.strip()

        if state_token:
            if len(state_token) == 2 and state_token.isalpha():
                state = state_token.upper()
            else:
                state = state_token

        return {
            "agency_street_address": street,
            "agency_city": city,
            "agency_state": state,
            "agency_zip_code": zip_code,
        }

    def _parse_maps_results(self, res: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Convert 'maps_results' from Google search response into rows
        matching Supabase table fields.
        """
        rows: List[Dict[str, Any]] = []
        maps_results = res.get("maps_results", []) or []

        for item in maps_results:
            address_fields = self._split_us_address(item.get("address") or "")
            phone = item.get("phone")

            rows.append(
                {
                    "agency_name": item.get("title"),
                    "agency_website": item.get("link"),
                    "agency_street_address": address_fields["agency_street_address"],
                    "agency_city": address_fields["agency_city"],
                    "agency_state": address_fields["agency_state"],
                    "agency_zip_code": address_fields["agency_zip_code"],
                    "agency_google_rating": item.get("rating"),
                    "agency_number_google_ratings": item.get("reviews"),
                    "agency_google_map_category": item.get("category"),
                    "agency_phone_number": phone,
                    "agency_google_cid": item.get("cid"),
                    "agency_insert_category": "local_services_county_maps",
                }
            )

        return rows

    def check_google_cid_exists(self, google_cid):
        """
        Checks if the given google_cide exists in the 'agency_google_cid' field
        of the 'sb_google_maps_search' table in Supabase. Retries up to 5 times if the request fails.
        Returns True if the CID exists or if all retries fail. Returns False only if the query
        succeeds and the CID does not exist.

        :param google_cid: The ID to check for existence.
        :return: True if the ID exists or query fails after retries, False otherwise.
        """
        for attempt in range(5):
            try:
                response = (
                    self.supabase.table("sb_google_maps_search")
                    .select("agency_google_cid")
                    .eq("agency_google_cid", google_cid)
                    .limit(1)
                    .execute()
                )
                if len(response.data) > 0:
                    print(
                        f"Found a Google CID that already exists in the sb_google_maps_search table: {google_cid}"
                    )
                    return True
                else:
                    return False

            except Exception as e:
                if attempt == 4:
                    print(
                        f"Could not connect to Supabase to check sb_google_maps_search Google CID, assuming it doesn't exist: {google_cid}"
                    )
                    return False

    def fetch_and_format_supabase_companies(self, city):
        try:
            city_name, state_abv = self.parse_city_state(city)

            response = (
                self.supabase.table("sb_google_maps_search")
                .select("agency_name, agency_city, agency_state, agency_google_cid")
                .eq("agency_insert_category", "feb_longtail_broker_maps_only")
                .eq("agency_city", city_name)
                .eq("agency_state", state_abv)
                .is_("updated_at", "null")
                .order("created_at", desc=False)
                .limit(1000)
                .execute()
            )

            formatted_data = []
            if not response.data:
                return formatted_data

            for row in response.data:
                formatted_data.append(
                    {
                        "agency_name": row["agency_name"],
                        "agency_city": row["agency_city"],
                        "agency_state": row["agency_state"],
                        "agency_google_cid": row["agency_google_cid"],
                    }
                )

            print(f"Got {len(formatted_data)} companies from Supabase")

            return formatted_data

        except Exception as e:
            print(f"Error fetching companies from Supabase: {e}")
            return []

    def load_city_state_list(self, csv_path="census_all_places.csv"):
        city_state_list = []

        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) < 2:
                    continue  # skip malformed rows
                city = row[0].strip()
                if city.startswith("#"):
                    continue  # skip comment / section-header rows
                state = row[1].strip()
                city_state_list.append(f"{city}, {state}")

        return city_state_list

    def parse_city_state(self, location: str):
        parts = [p.strip() for p in location.split(",")]
        city = parts[0] if parts else None
        state = parts[1] if len(parts) > 1 else None
        return city, state

    def sb_google_maps_search_updated_at(self, agency_google_cid: str):
        try:
            response = (
                self.supabase.table("sb_google_maps_search")
                .update({"updated_at": datetime.now(timezone.utc).isoformat()})
                .eq("agency_google_cid", agency_google_cid)
                .execute()
            )

            print("Updated at, updated")
            print(response)
            # Optional: Check if any rows were updated
            if response.count == 0:
                print(f"No row found for agency_google_cid: {agency_google_cid}")
            return response
        except Exception as e:
            print(f"Error updating updated_at in Supabase: {e}")
            return None

    def write_maps_results_to_supabase(
        self,
        res: Dict[str, Any],
        source_city: Optional[str] = None,
        batch_size: int = 500,
    ) -> None:
        """
        Insert parsed rows into 'sb_google_maps_search' table only if the
        agency_google_cid does not already exist.
        """
        rows = self._parse_maps_results(res)
        if not rows:
            print(f"No maps_results to write for {source_city or 'unknown'}")
            return

        table = "sb_google_maps_search"

        new_rows = []
        # Querying for all agency_google_cid in the table can timeout
        for row in rows:
            sleep(1)
            cid_exists = self.check_google_cid_exists(row["agency_google_cid"])

            if not cid_exists:
                new_rows.append(row)

        if not new_rows:
            print(f"No new rows to insert for {source_city or 'unknown'}")
            return

        # Insert in batches
        for i in range(0, len(new_rows), batch_size):
            chunk = new_rows[i : i + batch_size]
            (self.supabase.table(table).insert(chunk).execute())

        print(
            f"Inserted {len(new_rows)} new rows for {source_city or 'unknown'} "
            f"(skipped {len(rows) - len(new_rows)} duplicates)."
        )

    def write_maps_results_to_csv(
        self,
        res: Dict[str, Any],
        csv_path: str,
        search_query: Optional[str] = None,
    ) -> None:
        """
        Append parsed maps results to a local CSV file.
        Thread-safe: uses a module-level lock so concurrent threads
        don't corrupt the file.
        """
        rows = self._parse_maps_results(res)
        if not rows:
            return

        file_exists = os.path.isfile(csv_path)

        with _csv_lock:
            with open(csv_path, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
                if not file_exists or os.path.getsize(csv_path) == 0:
                    writer.writeheader()
                for row in rows:
                    row["search_query"] = search_query
                    writer.writerow(row)

        print(f"Wrote {len(rows)} rows to {csv_path} (query: {search_query})")


def _chunk_list(lst, n):
    """Split lst into n (roughly) equal non-empty chunks."""
    if n <= 0:
        return [lst]

    k, m = divmod(len(lst), n)
    chunks = []
    start = 0
    for i in range(n):
        end = start + k + (1 if i < m else 0)
        if start < end:
            chunks.append(lst[start:end])
        start = end
    return chunks


def _process_city_batch(cities_batch, titles, company_types, csv_path=None):
    """Worker function run in each thread."""
    bee = ScrapingBeeLongtailBrokerGoogleSearch()

    for city in cities_batch:
        for company_type in company_types:
            print(f"[THREAD {id(bee)}] Searching in city: {city}")
            city_search_string = f"{company_type} in {city}"

            agency_city, agency_state = bee.parse_city_state(city)

            city_search_res = bee.google_search(
                search=city_search_string, search_type="maps", pages=20, max_retries=2
            )

            for city_search_res_page in city_search_res:
                if csv_path:
                    bee.write_maps_results_to_csv(
                        city_search_res_page,
                        csv_path=csv_path,
                        search_query=city_search_string,
                    )
                else:
                    bee.write_maps_results_to_supabase(
                        city_search_res_page, source_city=agency_city
                    )

                # Personnel linkedin url search via google search
                """
                for map_result in city_search_res_page.get("maps_results", []):
                    agency_name = map_result["title"]
                    for job_title in titles:
                        agency_search_string = (
                            f'site:linkedin.com/in "{agency_name}" '
                            f"{agency_city} {agency_state} {job_title}"
                        )

                        agency_search_res = bee.google_search(
                            search=agency_search_string
                        )

                        db_input = (
                            bee.parse_google_search_results_for_personnel_li_urls(
                                agency_search_res, agency_search_string
                            )
                        )

                        bee.write_contacts_to_supabase(
                            csv_path="no_case",
                            contacts=db_input,
                            contact_category="feb_longtail_broker_maps_only",
                        )

                        sleep(1)  # keep your pacing to avoid rate limit issues
                """


def _process_city_batch_with_supabase_agencies(cities_batch, titles):
    bee = ScrapingBeeLongtailBrokerGoogleSearch()

    for city in cities_batch:
        while True:
            supabase_companies = bee.fetch_and_format_supabase_companies(city)

            print(
                f"Total companies to search for personnel in {city}: {len(supabase_companies)}"
            )
            sleep(10)

            if len(supabase_companies) == 0:
                print("Not more companies to process from Supabase Google Maps table")
                break

            for company in supabase_companies:
                print(
                    f"[THREAD {id(bee)}] Searching for personnel with info: {company}"
                )

                # Personnel linkedin url search via google search):
                agency_name = company["agency_name"]
                agency_city = company["agency_city"]
                agency_state = company["agency_state"]
                agency_google_cid = company["agency_google_cid"]

                for job_title in titles:
                    agency_search_string = (
                        f'site:linkedin.com/in "{agency_name}" '
                        f"{agency_city} {agency_state} {job_title}"
                    )

                    agency_search_res = bee.google_search(search=agency_search_string)

                    db_input = bee.parse_google_search_results_for_personnel_li_urls(
                        agency_search_res, agency_search_string
                    )

                    bee.write_contacts_to_supabase(
                        csv_path="no_case",
                        contacts=db_input,
                        contact_category="feb_longtail_broker_all_cities",
                    )

                    sleep(1)  # keep your pacing to avoid rate limit issues

                bee.sb_google_maps_search_updated_at(agency_google_cid)


def _process_supabase_batch(supabase_company_batch, titles):
    """Worker function run in each thread."""
    bee = ScrapingBeeLongtailBrokerGoogleSearch()

    for company in supabase_company_batch:
        print(f"[THREAD {id(bee)}] Searching for personnel with info: {company}")

        # Personnel linkedin url search via google search):
        agency_name = company["agency_name"]
        agency_city = company["agency_city"]
        agency_state = company["agency_state"]
        agency_google_cid = company["agency_google_cid"]

        for job_title in titles:
            agency_search_string = (
                f'site:linkedin.com/in "{agency_name}" '
                f"{agency_city} {agency_state} {job_title}"
            )

            agency_search_res = bee.google_search(search=agency_search_string)

            db_input = bee.parse_google_search_results_for_personnel_li_urls(
                agency_search_res, agency_search_string
            )

            bee.write_contacts_to_supabase(
                csv_path="no_case",
                contacts=db_input,
                contact_category="feb_longtail_broker_all_cities",
            )

            sleep(1)  # keep your pacing to avoid rate limit issues

        bee.sb_google_maps_search_updated_at(agency_google_cid)


def _round_robin_split(lst, n):
    """Split into n round-robin groups."""
    return [lst[i::n] for i in range(n)]


def main(num_threads: int = 3):
    loader_bee = ScrapingBeeLongtailBrokerGoogleSearch()

    # Cities spanning Clark County NV, Washoe County NV,
    # Los Angeles County CA, Orange County CA, and Stanislaus County CA
    cities = loader_bee.load_city_state_list("county_cities.csv")

    print(f"Total cities: {len(cities)}")
    sleep(10)

    # Job titles used for the (currently commented-out) LinkedIn personnel search
    titles = [
        "Owner",
        "CEO",
        "President",
        "COO",
        "General Manager",
        "Office Manager",
        "Director",
        "Principal",
        "Partner",
    ]

    # Target industries across the five counties
    company_types = [
        "pool service company",
        "nonprofit organization",
        "auto repair shop",
        "car dealership",
        "veterinary clinic",
        "medical doctor office",
        "optometrist",
        "dental office",
        "electrician",
        "plumber",
        "general contractor",
    ]

    # Set to a file path to write results locally instead of Supabase.
    # e.g. csv_output = "local_services_results.csv"
    # Set to None to write to Supabase instead.
    csv_output = "local_services_results.csv"

    city_batches = _round_robin_split(cities, num_threads)

    print(f"Running with {len(city_batches)} threads...")

    with ThreadPoolExecutor(max_workers=len(city_batches)) as executor:
        futures = [
            executor.submit(_process_city_batch, batch, titles, company_types, csv_output)
            for batch in city_batches
        ]

        for future in as_completed(futures):
            future.result()

    # TEST _PROCESS_SUPABASE_BATCH
    """
    loader_bee = ScrapingBeeLongtailBrokerGoogleSearch()

    while True:
        supabase_companies = loader_bee.fetch_and_format_supabase_companies()

        print(f"Total cities: {len(supabase_companies)}")
        sleep(10)

        titles = [
            "Benefits Advisor",
            "Health Insurance Agent",
            "Account Executive",
            "Agency Owner",
            "Broker",
            "Benefits Consultant",
            "President",
            "Owner",
            "CEO",
            "Producer",
            "VP",
            "Principal",
            "Partner",
            "Account Manager",
            "Client Executive",
            "Director",
            "Practice Leader",
            "Managing Director",
        ]

        supabase_company_batches = _round_robin_split(supabase_companies, num_threads)

        if len(supabase_company_batches) == 0:
            print("Not more companies to process from Supabase Google Maps table")
            break

        print(f"Running with {len(supabase_company_batches)} threads...")

        with ThreadPoolExecutor(max_workers=len(supabase_company_batches)) as executor:
            futures = [
                # Update the batch type you are processing here
                executor.submit(_process_supabase_batch, batch, titles)
                for batch in supabase_company_batches
            ]

            for future in as_completed(futures):
                future.result()
"""


if __name__ == "__main__":
    main()
