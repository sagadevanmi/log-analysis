import os
import time
import pandas as pd
from sqlalchemy import create_engine, exc
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from column_mapping import column_mapping
from utils.log_utils import set_logger

logger = set_logger()


class FileHandler(FileSystemEventHandler):
    """
    Handles file system events for the monitored folder.

    Attributes:
        folder_to_track (str): The folder being monitored.
    """

    def __init__(self, folder_to_track: str):
        """
        Initializes the FileHandler with the folder to monitor.

        Args:
            folder_to_track (str): The path to the folder being monitored.
        """
        self.folder_to_track = folder_to_track

    def on_created(self, event: FileSystemEventHandler) -> None:
        """
        Triggered when a file is created in the monitored folder.

        Args:
            event (FileSystemEventHandler): The event representing the file creation.
        """
        if event.is_directory:
            return

        # Get the file extension and check if it is an Excel file
        _, file_extension = os.path.splitext(event.src_path)
        if file_extension == ".xlsx":
            logger.info(f"New file detected: {event.src_path}")
            try:
                logger.info("Calling load_excel_to_sql()")
                load_excel_to_sql(event.src_path)
            except Exception as exc:
                logger.exception(
                    f"Failed to load {event.src_path} into SQL Server. Error: {exc}"
                )


def start_monitoring(folder_to_track: str) -> None:
    """
    Starts monitoring a folder for new Excel files.

    Args:
        folder_to_track (str): The path to the folder being monitored.
    """
    event_handler = FileHandler(folder_to_track)
    observer = Observer()
    observer.schedule(event_handler, folder_to_track, recursive=False)
    observer.start()
    logger.info(f"Monitoring {folder_to_track} for new files...")

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        observer.stop()
        logger.info("Stopped monitoring.")
    observer.join()


def load_excel_to_sql(file_path: str) -> None:
    """
    Loads data from an Excel file into a SQL Server table.

    Args:
        file_path (str): The path to the Excel file.

    Raises:
        ValueError: If the file path is invalid or the Excel file is empty.
        SQLAlchemyError: If there is an issue connecting to SQL Server or inserting data.
    """
    try:
        # Read the Excel file into a DataFrame
        print(file_path)
        df: pd.DataFrame = pd.read_excel(file_path)

        if df.empty:
            raise ValueError(f"The file {file_path} is empty.")

        df.rename(columns=column_mapping, inplace=True)
        df["EventTimestamp"] = pd.to_datetime(df["EventTimestamp"]) + pd.Timedelta(
            minutes=15
        )
        columns_to_load = column_mapping.values()
        df = df[columns_to_load]

        conn_str: str = (
            "mssql+pyodbc://localhost/abg?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"
        )

        # Create an engine to connect to SQL Server
        engine = create_engine(conn_str)

        # Load data into the SQL Server table
        df.to_sql("EventLog", con=engine, if_exists="append", index=False)
        logger.info(f"Data from {file_path} has been loaded into SQL Server.")

    except (pd.errors.EmptyDataError, FileNotFoundError) as ex:
        logger.exception(f"Error reading {file_path}: {ex}")
        raise ValueError(f"Invalid file: {file_path}. Error: {ex}")

    except exc.SQLAlchemyError as ex:
        logger.exception(f"Error connecting to SQL Server or inserting data: {ex}")
        raise exc.SQLAlchemyError(f"Database operation failed. Error: {ex}")


if __name__ == "__main__":
    # Path to the folder you want to monitor
    folder_to_track: str = r"C:\\Users\\sagad\\Desktop\\ABG\\log-analysis\\log_files"

    # Start monitoring the folder
    start_monitoring(folder_to_track)
