import logging
from typing import List

import pyodbc
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class CarMakesExtractor:
    """Extract car makes from NHTSA API and insert into database"""

    NHTSA_API_URL = "https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes"
    TIMEOUT = 10  # seconds

    def __init__(self, connection_string: str):
        """
        Initialize the extractor with database connection string

        Args:
            connection_string: SQL Server connection string with Windows Authentication
        """
        self.connection_string = connection_string
        self.session = requests.Session()

    def fetch_car_makes(self) -> List[str]:
        """
        Fetch all car makes from NHTSA API

        Returns:
            List of car make names

        Raises:
            requests.RequestException: If API call fails
        """
        try:
            logger.info(f"Fetching car makes from {self.NHTSA_API_URL}")

            response = self.session.get(
                self.NHTSA_API_URL, params={"format": "json"}, timeout=self.TIMEOUT
            )

            # Raise exception for bad status codes
            response.raise_for_status()

            data = response.json()

            # Extract makes from the response
            makes = [item["Make_Name"] for item in data.get("Results", [])]

            logger.info(f"Successfully fetched {len(makes)} car makes")
            return makes

        except requests.exceptions.Timeout:
            logger.error(f"Request timed out after {self.TIMEOUT} seconds")
            raise
        except requests.exceptions.ConnectionError:
            logger.error("Failed to connect to the API")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error occurred: {e.response.status_code}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"An error occurred while fetching data: {e}")
            raise
        except ValueError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            raise

    def insert_car_makes(self, makes: List[str]) -> tuple:
        """
        Insert car makes into the database

        Args:
            makes: List of car make names

        Returns:
            Tuple of (inserted_count, skipped_count)
        """
        inserted_count = 0
        skipped_count = 0

        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()

            logger.info(f"Connecting to database and inserting {len(makes)} car makes")

            for make in makes:
                try:
                    cursor.execute(
                        "INSERT INTO dbo.CarTypeMaster (CarMake) VALUES (?)", make
                    )
                    inserted_count += 1

                except pyodbc.IntegrityError:
                    # Duplicate entry, skip it
                    skipped_count += 1
                    logger.debug(f"Skipped duplicate: {make}")
                except pyodbc.ProgrammingError as e:
                    logger.error(f"Error inserting {make}: {e}")
                    raise

            conn.commit()
            logger.info(
                f"Successfully inserted {inserted_count} car makes, skipped {skipped_count} duplicates"
            )
            cursor.close()
            conn.close()

            return inserted_count, skipped_count

        except pyodbc.OperationalError as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            raise

    def run(self) -> None:
        """Execute the complete process: fetch and insert"""
        try:
            # Fetch car makes from API
            makes = self.fetch_car_makes()

            # Insert into database
            inserted, skipped = self.insert_car_makes(makes)

            logger.info(f"Process completed: {inserted} inserted, {skipped} skipped")

        except Exception as e:
            logger.error(f"Process failed: {e}")
            raise


def main():
    # Connection string using Windows Authentication
    connection_string = (
        r"Driver={ODBC Driver 17 for SQL Server};"
        r"Server=(localdb)\MSSQLLocalDB;"  # Change to your server name if different
        r"Database=AdventureWorks2017;"
        r"Trusted_Connection=yes;"
    )

    # Create extractor and run
    extractor = CarMakesExtractor(connection_string)
    extractor.run()


if __name__ == "__main__":
    main()
