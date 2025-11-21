import logging
from io import BytesIO
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import quote

import pyodbc
import requests
from PIL import Image

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class CarLogoDownloader:
    """Download car logos and update database with paths"""

    # Logo sources in priority order
    # Using GitHub car-logos-dataset for thumbnail logos
    LOGO_SOURCES = [
        # GitHub car-logos-dataset - primary source (thumbnails)
        "https://raw.githubusercontent.com/filippofilip95/car-logos-dataset/master/logos/thumb/{make_name}.png",
        "https://raw.githubusercontent.com/filippofilip95/car-logos-dataset/master/logos/thumb/{make_name}.jpg",
        # Fallback sources
        "https://www.carlogos.org/logo/{make_name}.png",
        "https://www.carlogos.org/logo/{make_name}.jpg",
    ]

    def __init__(self, connection_string: str, photos_folder: str = "photos"):
        """
        Initialize the logo downloader

        Args:
            connection_string: SQL Server connection string
            photos_folder: Directory to store downloaded logos
        """
        self.connection_string = connection_string
        self.photos_folder = Path(photos_folder)
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        )
        self.timeout = 10

        # Create photos folder if it doesn't exist
        self.photos_folder.mkdir(exist_ok=True)
        logger.info(f"Photos folder set to: {self.photos_folder.absolute()}")

    def get_car_makes_from_db(self) -> dict:
        """
        Fetch all car makes from database and filter out makes starting with numbers

        Returns:
            Dictionary with {CarTypeID: CarMake} (excluding makes that start with numbers)
        """
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()

            cursor.execute(
                """SELECT CarTypeID, CarMake FROM dbo.CarTypeMaster where carmake not like '%American%' and carmake not like '%Company%' and carmake not like '%/%'
  and carmake not like '%[0-9]%' and carmake not like '%&%' and carmake not like '% INC%' and carmake not like '%Co.%'
  and carmake not like '%Trailer%' and carmake not like '% HOME%'
  and carmake not like '%Performance%'
    and carmake not like '%WELDING%' and carmake not like '%AAA%'
	and carmake not like '%SUPPLY%'
	and carmake not like '%BULK%'"""
            )
            all_makes = {row[0]: row[1] for row in cursor.fetchall()}

            cursor.close()
            conn.close()

            # Filter out makes that start with numbers
            makes = {
                car_id: make
                for car_id, make in all_makes.items()
                if make and not make[0].isdigit()
            }

            logger.info(
                f"Retrieved {len(makes)} car makes from database (filtered from {len(all_makes)})"
            )
            if len(makes) < len(all_makes):
                logger.info(
                    f"Excluded {len(all_makes) - len(makes)} makes that start with numbers"
                )

            return makes

        except pyodbc.OperationalError as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def download_logo(self, make_name: str) -> Optional[Tuple[str, str]]:
        """
        Download logo for a car make and save it to disk

        Args:
            make_name: Name of the car make

        Returns:
            Tuple of (filename, extension) if successful, None otherwise
        """
        # Sanitize make name for filename
        safe_make_name = "".join(c for c in make_name if c.isalnum() or c in " -_")
        safe_make_name = safe_make_name.strip().replace(" ", "")

        # Try different logo sources
        for source_template in self.LOGO_SOURCES:
            # Format the source URL
            if "{domain}" in source_template:
                # For logo.dev, use the make name as domain
                source_url = source_template.format(domain=safe_make_name.lower())
            else:
                # For carlogos.org, format the make name properly
                source_url = source_template.format(
                    make_name=quote(safe_make_name.lower())
                )

            try:
                logger.debug(f"Trying to download logo from: {source_url}")

                response = self.session.get(source_url, timeout=self.timeout)

                if response.status_code == 200:
                    # Verify it's actually an image
                    try:
                        Image.open(BytesIO(response.content))

                        # Determine file extension from URL or content-type
                        content_type = response.headers.get("content-type", "").lower()
                        if "png" in content_type or ".png" in source_url:
                            extension = "png"
                        elif (
                            "jpg" in content_type
                            or "jpeg" in content_type
                            or ".jpg" in source_url
                        ):
                            extension = "jpg"
                        else:
                            extension = "png"  # default

                        filename = f"{safe_make_name}.{extension}"

                        # Save the logo to disk
                        if self.save_logo(make_name, response.content, extension):
                            logger.info(
                                f"Successfully downloaded and saved logo for {make_name} from {source_url}"
                            )
                            return filename, extension
                        else:
                            logger.warning(f"Failed to save logo for {make_name}")
                            continue

                    except Exception as e:
                        logger.debug(f"Invalid image from {source_url}: {e}")
                        continue

            except requests.exceptions.Timeout:
                logger.debug(f"Timeout downloading from {source_url}")
                continue
            except requests.exceptions.RequestException as e:
                logger.debug(f"Error downloading from {source_url}: {e}")
                continue

        logger.warning(f"Could not find logo for {make_name}")
        return None

    def save_logo(
        self, make_name: str, response_content: bytes, extension: str
    ) -> bool:
        """
        Save downloaded logo to disk

        Args:
            make_name: Name of the car make
            response_content: Binary content of the image
            extension: File extension (png or jpg)

        Returns:
            True if saved successfully, False otherwise
        """
        safe_make_name = "".join(c for c in make_name if c.isalnum() or c in " -_")
        safe_make_name = safe_make_name.strip().replace(" ", "")

        try:
            # Validate image before saving
            Image.open(BytesIO(response_content))

            file_path = self.photos_folder / f"{safe_make_name}.{extension}"

            with open(file_path, "wb") as f:
                f.write(response_content)

            logger.debug(f"Saved logo to {file_path}")
            return True

        except Exception as e:
            logger.error(f"Error saving logo for {make_name}: {e}")
            return False

    def download_all_logos(self) -> dict:
        """
        Download logos for all car makes in database

        Returns:
            Dictionary with {CarTypeID: (filename, extension)} for successful downloads
        """
        makes = self.get_car_makes_from_db()
        downloaded_logos = {}
        failed_makes = []

        logger.info(f"Starting to download {len(makes)} car logos")

        for car_type_id, make_name in makes.items():
            result = self.download_logo(make_name)

            if result:
                filename, extension = result
                downloaded_logos[car_type_id] = (filename, extension)
            else:
                failed_makes.append(make_name)

        logger.info(
            f"Download complete: {len(downloaded_logos)} successful, {len(failed_makes)} failed"
        )
        if failed_makes:
            logger.info(f"Failed makes: {', '.join(failed_makes[:10])}")

        return downloaded_logos

    def generate_sql_script(
        self,
        downloaded_logos: dict,
        server_path: str,
        output_file: str = "update_logo_paths.sql",
    ) -> str:
        """
        Generate SQL script with UPDATE statements for all downloaded logos

        Args:
            downloaded_logos: Dictionary with {CarTypeID: (filename, extension)}
            server_path: Network path to photos folder (e.g., \\server\photos)
            output_file: Output SQL file path

        Returns:
            Path to generated SQL script
        """
        if not downloaded_logos:
            logger.warning("No logos to generate SQL script for")
            return None

        try:
            # First, get the make names for the downloaded logos from the database
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            cursor.execute("SELECT CarTypeID, CarMake FROM dbo.CarTypeMaster")
            make_names = {row[0]: row[1] for row in cursor.fetchall()}
            cursor.close()
            conn.close()

            sql_lines = [
                "-- Auto-generated SQL script to update car logo paths",
                "-- Generated by CarLogoDownloader",
                "",
                "USE AdventureWorks2017;",
                "GO",
                "",
                "-- Update logo paths for downloaded car makes",
                "",
            ]

            for car_type_id, (filename, extension) in downloaded_logos.items():
                make_name = make_names.get(car_type_id, "")
                logo_path = f"{server_path}\\{filename}"
                # Escape single quotes in the path for SQL
                escaped_path = logo_path.replace("'", "''")
                escaped_make_name = make_name.replace("'", "''")

                sql_lines.append(
                    f"UPDATE dbo.CarTypeMaster SET LogoPath = '{escaped_path}', ModifiedDate = GETUTCDATE() WHERE CarMake LIKE '%{escaped_make_name}%';"
                )

            # Add summary statistics
            sql_lines.extend(
                [
                    "",
                    "-- Summary of updated records",
                    "SELECT",
                    "    COUNT(*) as TotalMakes,",
                    "    SUM(CASE WHEN LogoPath IS NOT NULL THEN 1 ELSE 0 END) as MakesWithLogo,",
                    "    SUM(CASE WHEN LogoPath IS NULL THEN 1 ELSE 0 END) as MakesWithoutLogo",
                    "FROM dbo.CarTypeMaster;",
                    "",
                    "-- List all makes with logos",
                    "SELECT CarTypeID, CarMake, LogoPath FROM dbo.CarTypeMaster WHERE LogoPath IS NOT NULL ORDER BY CarMake;",
                ]
            )

            # Write to file
            output_path = Path(output_file)
            with open(output_path, "w", encoding="utf-8") as f:
                f.write("\n".join(sql_lines))

            logger.info(f"Generated SQL script: {output_path.absolute()}")
            return str(output_path.absolute())

        except Exception as e:
            logger.error(f"Error generating SQL script: {e}")
            raise

    def update_database(self, downloaded_logos: dict, server_path: str) -> int:
        """
        Update database with logo paths

        Args:
            downloaded_logos: Dictionary with {CarTypeID: (filename, extension)}
            server_path: Network path to photos folder (e.g., \\\\server\\photos)

        Returns:
            Number of records updated
        """
        if not downloaded_logos:
            logger.warning("No logos to update in database")
            return 0

        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()

            updated_count = 0

            for car_type_id, (filename, extension) in downloaded_logos.items():
                logo_path = f"{server_path}\\{filename}"

                try:
                    cursor.execute(
                        "UPDATE dbo.CarTypeMaster SET LogoPath = ?, ModifiedDate = GETUTCDATE() WHERE CarTypeID = ?",
                        (logo_path, car_type_id),
                    )
                    updated_count += 1

                except pyodbc.ProgrammingError as e:
                    logger.error(f"Error updating CarTypeID {car_type_id}: {e}")

            conn.commit()
            logger.info(f"Updated {updated_count} records in database")

            cursor.close()
            conn.close()

            return updated_count

        except pyodbc.OperationalError as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def run(self, server_path: str = r"\\server\photos") -> None:
        """
        Execute the complete process: download logos, generate SQL, and update database

        Args:
            server_path: Network path to photos folder
        """
        try:
            # Download all logos
            downloaded_logos = self.download_all_logos()

            # Generate SQL script with UPDATE statements
            sql_script_path = self.generate_sql_script(downloaded_logos, server_path)

            if sql_script_path:
                logger.info(f"SQL script generated at: {sql_script_path}")
                logger.info("You can run this script in SQL Server Management Studio")

            # Update database with logo paths
            self.update_database(downloaded_logos, server_path)

            logger.info(
                "Logo download, SQL generation, and database update completed successfully"
            )

        except Exception as e:
            logger.error(f"Process failed: {e}")
            raise


def main():
    # Connection string using Windows Authentication
    connection_string = (
        r"Driver={ODBC Driver 17 for SQL Server};"
        r"Server=(localdb)\MSSQLLocalDB;"
        r"Database=AdventureWorks2017;"
        r"Trusted_Connection=yes;"
    )

    # Network path to photos folder - update this to your actual network path
    # Example: \\server\photos or \\192.168.1.100\photos
    server_path = r"\\server\photos"

    # Create downloader and run
    downloader = CarLogoDownloader(connection_string, photos_folder="photos")
    downloader.run(server_path=server_path)


if __name__ == "__main__":
    main()
