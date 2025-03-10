#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Road Closure Bot

This script monitors the Cameron County, TX website for road closure notices,
particularly those related to SpaceX activities. It detects new closures,
status changes, and expiration, then posts updates to Twitter.

The bot works by:
1. Scraping the Cameron County SpaceX page for closure announcements
2. Parsing the closure information (dates, times, type, status)
3. Storing closure data in a SQLite database
4. Posting updates to Twitter when new closures are found or statuses change
5. Checking for and managing expired closures

Author: Luke Leisher
License: MIT
"""

import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
import time
import logging
import re
from typing import List, Dict, Tuple, Optional
import os
from dotenv import load_dotenv
import hashlib
import requests
import tweepy
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dataclasses import dataclass

@dataclass
class Config:
    """Configuration settings for the Road Closure Bot.
    
    This class centralizes all configuration parameters used by the bot,
    including timing settings, retry parameters, file paths, and API credentials.
    Default values are provided, but can be overridden via environment variables.
    """
    CHECK_INTERVAL: int = 60  # Time in seconds between checking for new closures
    MAX_RETRIES: int = 5  # Maximum number of retry attempts for network requests
    BACKOFF_FACTOR: int = 1  # Exponential backoff factor for retries
    REQUEST_TIMEOUT: int = 60  # Timeout in seconds for HTTP requests
    MAX_URLS_PER_CYCLE: int = 5  # Maximum number of URLs to process in each cycle
    CACHE_DURATION: int = 300  # Cache duration in seconds (5 minutes)
    DB_PATH: str = 'road_closures.db'  # Path to SQLite database file
    LOG_FILE: str = 'road_closure_bot.log'  # Path to log file
    MAIN_URL: str = "https://www.cameroncountytx.gov/spacex/"  # Main URL to scrape
    
    # Twitter API settings - these should be provided via environment variables
    TWITTER_BEARER_TOKEN: Optional[str] = None
    TWITTER_CONSUMER_KEY: Optional[str] = None
    TWITTER_CONSUMER_SECRET: Optional[str] = None
    TWITTER_ACCESS_TOKEN: Optional[str] = None
    TWITTER_ACCESS_TOKEN_SECRET: Optional[str] = None

    @classmethod
    def from_env(cls) -> 'Config':
        """Create Config instance from environment variables.
        
        Loads environment variables from .env file and creates a Config instance
        with values from environment variables, falling back to defaults if not provided.
        
        Returns:
            Config: A Config instance populated with values from environment variables
        """
        # TODO: Update this path to be relative or use a standard location
        load_dotenv("D:/Projects/Bocaroad/Test/.env")
        return cls(
            CHECK_INTERVAL=int(os.getenv('CHECK_INTERVAL', '60')),
            TWITTER_BEARER_TOKEN=os.getenv('TWITTER_BEARER_TOKEN'),
            TWITTER_CONSUMER_KEY=os.getenv('TWITTER_CONSUMER_KEY'),
            TWITTER_CONSUMER_SECRET=os.getenv('TWITTER_CONSUMER_SECRET'),
            TWITTER_ACCESS_TOKEN=os.getenv('TWITTER_ACCESS_TOKEN'),
            TWITTER_ACCESS_TOKEN_SECRET=os.getenv('TWITTER_ACCESS_TOKEN_SECRET')
        )

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='road_closure_bot.log'
)

def parse_time(time_str: str) -> Optional[datetime]:
    """Parses a time string, handling various formats and removing 'c.s.t.'.
    
    This utility function handles the various time formats that may appear
    in the Cameron County closure notices, normalizing them and converting
    to datetime objects.
    
    Args:
        time_str (str): A string containing time information in various formats
                        (e.g., "8 a.m.", "8:30 p.m.", "1000-p-m")
    
    Returns:
        Optional[datetime]: A datetime object representing the parsed time,
                           or None if parsing failed
    """
    time_str = time_str.lower().strip().replace("c.s.t.", "").strip()
    
    # Handle special case of format like "1000-p-m" (meaning 10:00 PM)
    match = re.match(r"(\d{1,4})-([ap])-m", time_str)
    if match:
        digits, period = match.groups()
        if len(digits) == 4:
            hours = digits[:2]
            minutes = digits[2:]
            time_str = f"{hours}:{minutes} {period}.m."
        elif len(digits) in (1, 2):
            hours = digits
            time_str = f"{hours} {period}.m."
    
    # Normalize time format for standard parsing
    time_str = time_str.replace("a.m.", "am").replace("p.m.", "pm").replace("-", ":")
    
    # Try various time formats until one works
    formats = ["%I:%M %p", "%I %p", "%I:%M", "%I"]
    for fmt in formats:
        try:
            return datetime.strptime(time_str, fmt)
        except ValueError:
            pass
    
    logging.warning(f"Could not parse time: {time_str}")
    return None

class RoadClosureBot:
    """Main bot class for monitoring and tweeting about road closures.
    
    This class handles all the major functionality of the bot, including:
    - Setting up database connections and Twitter clients
    - Scraping and parsing road closure data from the Cameron County website
    - Storing and retrieving closure information from the database
    - Posting updates to Twitter when closures are announced, changed, or expired
    - Managing the main monitoring loop
    """
    
    def __init__(self):
        """Initialize the bot with necessary connections and configurations.
        
        Sets up:
        - SQLite database connection
        - HTTP session with retry capabilities
        - Twitter API client
        """
        # Set up database connection
        self.db_conn = sqlite3.connect('road_closures.db', check_same_thread=False)
        self.setup_database()
        
        # Set up HTTP session with user agent and retry mechanism
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        })
        retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount('https://', adapter)
        
        # Set up Twitter client
        self.client = self.setup_twitter_client()
        
        # Track last processed URL to avoid duplicates
        self.last_processed_url = None

    def setup_twitter_client(self):
        """Sets up the Tweepy v2 API client.
        
        Retrieves Twitter API credentials from environment variables
        and creates a Tweepy client for Twitter API v2.
        
        Returns:
            tweepy.Client or None: Initialized Tweepy client if successful,
                                 None if there was an error or missing credentials
        """
        try:
            # Get Twitter API credentials from environment variables
            bearer_token = os.getenv("TWITTER_BEARER_TOKEN")
            consumer_key = os.getenv("TWITTER_CONSUMER_KEY")
            consumer_secret = os.getenv("TWITTER_CONSUMER_SECRET")
            access_token = os.getenv("TWITTER_ACCESS_TOKEN")
            access_token_secret = os.getenv("TWITTER_ACCESS_TOKEN_SECRET")

            # Check if all required credentials are available
            if not all([bearer_token, consumer_key, consumer_secret, access_token, access_token_secret]):
                missing_vars = [var for var, value in locals().items() if value is None and var.startswith("TWITTER_")]
                logging.error(f"Missing environment variables: {', '.join(missing_vars)}")
                return None

            client = tweepy.Client(
                bearer_token=bearer_token,
                consumer_key=consumer_key,
                consumer_secret=consumer_secret,
                access_token=access_token,
                access_token_secret=access_token_secret,
                wait_on_rate_limit=True
            )
            logging.info("Successfully authenticated with Twitter v2 API")
            return client
        except tweepy.TweepyException as e:
            logging.error(f"Error setting up Twitter v2 API client: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error during Twitter setup: {e}")
            return None

    def setup_database(self):
        """Creates the database table if it doesn't exist.
        
        Initializes the SQLite database with the road_closures table,
        which stores all closure information including status and tweet IDs.
        The table has a unique constraint on the hash field to prevent duplicates.
        """
        cursor = self.db_conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS road_closures (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                closure_type TEXT,                  -- Type of closure (Flight Closure, Non-Flight Closure)
                subject TEXT,                       -- Subject of closure (Road Closure, Road Delay)
                place TEXT,                         -- Location affected
                primary_start_datetime TEXT,        -- Start datetime of the primary closure window
                primary_end_datetime TEXT,          -- End datetime of the primary closure window
                backup_start_datetime TEXT,         -- Start datetime of backup closure window (if any)
                backup_end_datetime TEXT,           -- End datetime of backup closure window (if any)
                full_title TEXT NOT NULL,           -- Full title/description of the closure
                source_url TEXT,                    -- URL source of the closure information
                hash TEXT UNIQUE NOT NULL,          -- Unique hash identifier
                status TEXT DEFAULT 'active',       -- Status: active, expired, or revoked
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When this record was created
                tweet_id TEXT,                      -- ID of the primary tweet
                backup_tweet_id TEXT                -- ID of the backup tweet (if any)
            )
        ''')
        self.db_conn.commit()

    def get_with_retry(self, url, max_retries=5, backoff_factor=1):
        """Performs a GET request with retries and exponential backoff.
        
        Attempts to fetch the given URL, with automatic retries on failure
        using exponential backoff to avoid overwhelming the server.
        
        Args:
            url (str): The URL to request
            max_retries (int): Maximum number of retry attempts
            backoff_factor (int): Factor to calculate delay between retries
            
        Returns:
            requests.Response or None: The response object if successful, None otherwise
        """
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=60)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                # Calculate backoff time based on attempt number
                backoff_time = backoff_factor * (2 ** attempt)
                logging.error(f"Request error: {e}. Retrying in {backoff_time} seconds...")
                time.sleep(backoff_time)
        
        logging.error(f"Failed to fetch {url} after {max_retries} attempts.")
        return None

    def fetch_closure_urls(self) -> List[str]:
        """Fetches URLs of closure detail pages.
        
        Scrapes the main Cameron County SpaceX page to extract all URLs
        pointing to individual road closure announcement pages.
        
        Returns:
            List[str]: List of URLs to individual closure announcement pages
        """
        main_url = "https://www.cameroncountytx.gov/spacex/"
        logging.info(f"Fetching closure URLs from {main_url}...")
        
        # Fetch the main page with retry mechanism
        response = self.get_with_retry(main_url)
        if response is None:
            return []
            
        try:
            # Parse the HTML and extract all closure links
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Use list comprehension with walrus operator to find links
            # in all h5 tags with the specific class
            urls = [a_tag.get('href') for h5_tag in soup.find_all('h5', class_='entry-title reverse-link-color')
                    if (a_tag := h5_tag.find('a')) and a_tag.get('href')]
                    
            logging.info(f"Fetched closure URLs: {urls}")
            return urls
        except Exception as e:
            logging.error(f"Error parsing closure URLs: {e}")
            return []

    def parse_url_dates_times(self, url: str) -> Tuple[Optional[str], Optional[str], Optional[str], List[str], Optional[str], Optional[str]]:
        """Parses dates and times from the URL.
        
        Cameron County closure URLs often contain date and time information in their structure.
        This method extracts that information by parsing the URL string.
        
        Example URL format:
        .../order-to-temporarily-close-state-hwy-4-June-13-2023-from-8-a-m-to-8-p-m-in-the-alternative-June-14-2023-or-June-15-2023-from-8-a-m-to-8-p-m/
        
        Args:
            url (str): The URL to parse
            
        Returns:
            Tuple containing:
            - primary_date (str): Primary closure date (e.g., "June-13-2023")
            - primary_start_time (str): Primary start time (e.g., "8-a-m")
            - primary_end_time (str): Primary end time (e.g., "8-p-m")
            - alternative_dates (List[str]): List of alternative dates
            - alternative_start_time (str): Alternative start time
            - alternative_end_time (str): Alternative end time
        """
        try:
            # Split the URL to separate primary closure and alternative dates
            parts = url.split("-in-the-alternative-")
            primary_part = parts[0]
            alternative_part = parts[1] if len(parts) > 1 else None

            # Extract date and times from primary part
            primary_date_time = primary_part.split("state-hwy-4-")[1]
            primary_date, primary_times = primary_date_time.split("-from-")
            primary_start_time, primary_end_time = primary_times.split("-to-")

            # Initialize alternative date/time variables
            alternative_dates = []
            alternative_start_time = None
            alternative_end_time = None
            
            # Extract alternative dates and times if they exist
            if alternative_part:
                alt_dates_times = alternative_part.split("-from-")
                alt_dates_str = alt_dates_times[0]
                alternative_dates = alt_dates_str.split("-or-")  # Handle multiple alternative dates
                
                # Extract alternative times if present
                if len(alt_dates_times) > 1:
                    alt_times = alt_dates_times[1].split("-to-")
                    alternative_start_time = alt_times[0]
                    alternative_end_time = alt_times[1].replace("/", "")  # Clean up URL ending

            return primary_date, primary_start_time, primary_end_time, alternative_dates, alternative_start_time, alternative_end_time
        except Exception as e:
            logging.error(f"Error parsing URL dates and times: {e}")
            return None, None, None, [], None, None

    def fetch_and_parse_detail_page(self, url: str) -> Optional[Dict]:
        """Fetches and parses a closure detail page.
        
        Retrieves the content of a specific closure announcement page,
        extracts relevant information about the closure, and returns
        it as a structured dictionary.
        
        Args:
            url (str): URL of the closure detail page to parse
            
        Returns:
            Optional[Dict]: Dictionary containing parsed closure information, or None if parsing failed
            
            The dictionary includes:
            - closure_type: "Flight Closure" or "Non-Flight Closure"
            - subject: "Road Closure" or "Road Delay"
            - place: Location affected
            - primary_start_datetime: ISO format start datetime
            - primary_end_datetime: ISO format end datetime
            - backup_dates: List of backup dates (if any)
            - full_title: Full text content of the closure notice
            - source_url: Original URL of the closure notice
            - status: "active" or "revoked" based on notice content
        """
        # Fetch the detail page with retry mechanism
        response = self.get_with_retry(url)
        if response is None:
            return None
            
        try:
            logging.info(f"Fetching detail page: {url}")
            
            # Parse the HTML content
            soup = BeautifulSoup(response.text, 'html.parser')
            content_area = soup.find('div', class_='entry-content')
            if not content_area:
                logging.error(f"Could not find content area on {url}")
                return None

            # Extract all text content from paragraphs
            text_content = " ".join(p.get_text(separator=" ", strip=True) for p in content_area.find_all('p')).lower()
            logging.info(f"Extracted text: {text_content}")

            # Determine if this is a flight-related closure
            flight_keywords = ["space flight", "launch", "testing activities for spacex", "flight activity"]
            is_flight_closure = any(keyword in text_content for keyword in flight_keywords)
            closure_type = "Flight Closure" if is_flight_closure else "Non-Flight Closure"
            
            # Determine if this is a full closure or just a delay
            subject = "Road Closure" if "remain open" not in text_content else "Road Delay"
            place = "Boca Chica Beach and State Hwy 4"

            # First try to parse dates and times from the URL
            primary_date_str, primary_start_time_str, primary_end_time_str, alternative_dates, alt_start_time_str, alt_end_time_str = self.parse_url_dates_times(url)
            
            # If URL parsing fails, try to extract dates and times from the page content
            if not primary_date_str:
                # Extract date using regex
                date_match = re.search(r"on (\w+ \d{1,2}, \d{4})", text_content)
                if date_match:
                    primary_date_str = date_match.group(1)
                    primary_date = datetime.strptime(primary_date_str, "%B %d, %Y")
                else:
                    logging.error(f"Could not extract closure date from text content: {url}")
                    return None

                # Look for time ranges using regex (e.g., "8:00 a.m. - 8:00 p.m.")
                time_matches = re.findall(r"(\d{1,2}:\d{2}\s*[ap]\.?m\.?)\s*[–-]\s*(\d{1,2}:\d{2}\s*[ap]\.?m\.?)", text_content)
                if not time_matches:
                    logging.error(f"Could not extract time from text content: {url}")
                    return None
                
                # Parse start and end times from all matches
                start_times = []
                end_times = []
                for start_str, end_str in time_matches:
                    start_time = parse_time(start_str)
                    end_time = parse_time(end_str)
                    if start_time and end_time:
                        start_times.append(start_time)
                        end_times.append(end_time)
                
                # Ensure we have at least one valid time range
                if not start_times or not end_times:
                    logging.error(f"Could not parse times from text content: {url}")
                    return None
                
                # Use earliest start time and latest end time if multiple ranges exist
                primary_start_time = min(start_times, key=lambda x: x.time())
                primary_end_time = max(end_times, key=lambda x: x.time())
                alternative_dates = []
            else:
                # If URL parsing succeeded, convert the strings to datetime objects
                primary_date = datetime.strptime(primary_date_str, "%B-%d-%Y")
                primary_start_time = parse_time(primary_start_time_str)
                primary_end_time = parse_time(primary_end_time_str)
                if not primary_start_time or not primary_end_time:
                    logging.error(f"Could not parse times from URL: {url}")
                    return None

            # Create full datetime objects by combining date and time
            primary_start_datetime = datetime.combine(primary_date, primary_start_time.time(), tzinfo=timezone.utc)
            primary_end_datetime = datetime.combine(primary_date, primary_end_time.time(), tzinfo=timezone.utc)

            # Process alternative/backup dates if any
            backup_dates = []
            for alt_date_str in alternative_dates:
                alt_date = datetime.strptime(alt_date_str, "%B-%d-%Y")
                alt_start_time = parse_time(alt_start_time_str) if alt_start_time_str else primary_start_time
                alt_end_time = parse_time(alt_end_time_str) if alt_end_time_str else primary_end_time
                backup_dates.append(alt_date.strftime("%B %d, %Y"))

            # Build and return the closure information dictionary
            return {
                'closure_type': closure_type,
                'subject': subject,
                'place': place,
                'primary_start_datetime': primary_start_datetime.isoformat(),
                'primary_end_datetime': primary_end_datetime.isoformat(),
                'backup_dates': backup_dates,
                'full_title': text_content,
                'source_url': url,
                'status': "revoked" if "revoked" in text_content or "cancelled" in text_content else "active"
            }
        except Exception as e:
            logging.error(f"Error fetching/parsing page {url}: {e}")
            return None

    def _format_datetime(self, dt_str: Optional[str]) -> Optional[str]:
        """Formats a datetime string for display.
        
        Converts an ISO format datetime string into a more readable format
        for display in tweets and console output.
        
        Args:
            dt_str (str): ISO format datetime string
            
        Returns:
            str: Formatted datetime string (e.g., "Mar 15, 2024 10:30 AM"),
                 or None if parsing fails
        """
        if not dt_str:
            return None
        try:
            dt = datetime.fromisoformat(dt_str)
            return dt.strftime('%b %d, %Y %I:%M %p')
        except ValueError as e:
            logging.error(f"Error formatting datetime: {dt_str}, {e}")
            return None

    def generate_closure_hash(self, closure: Dict) -> str:
        """Generates a unique hash for a closure.
        
        Creates a unique identifier for each closure based on its type
        and timing information. This hash is used to detect duplicate
        closures and track status changes.
        
        Args:
            closure (Dict): Closure information dictionary
            
        Returns:
            str: MD5 hash string uniquely identifying this closure
        """
        closure_str = f"{closure['closure_type']}{closure['primary_start_datetime']}{closure['primary_end_datetime']}"
        return hashlib.md5(closure_str.encode()).hexdigest()

    def get_stored_status_and_tweet_id(self, closure_hash: str) -> Tuple[Optional[str], Optional[str]]:
        """Retrieves the stored status and tweet ID of a closure.
        
        Looks up a closure in the database by its hash to get its current
        status and associated tweet ID.
        
        Args:
            closure_hash (str): Hash identifier of the closure
            
        Returns:
            Tuple[Optional[str], Optional[str]]: (status, tweet_id) if found,
                                               (None, None) if not found
        """
        cursor = self.db_conn.cursor()
        cursor.execute('SELECT status, tweet_id FROM road_closures WHERE hash = ?', (closure_hash,))
        result = cursor.fetchone()
        return result if result else (None, None)

    def update_closure_tweet_id(self, closure_hash: str, tweet_id: str):
        """Updates the tweet ID for a closure in the database.
        
        After successfully posting a tweet about a closure, this method
        stores the tweet's ID in the database for future reference.
        
        Args:
            closure_hash (str): Hash identifier of the closure
            tweet_id (str): Twitter's ID for the posted tweet
        """
        try:
            cursor = self.db_conn.cursor()
            cursor.execute('UPDATE road_closures SET tweet_id = ? WHERE hash = ?', (tweet_id, closure_hash))
            self.db_conn.commit()
            logging.info(f"Updated tweet_id to {tweet_id} for hash: {closure_hash}")
        except sqlite3.Error as e:
            logging.error(f"Error updating tweet_id: {e}")

    def store_closure(self, closure: Dict, closure_hash: str, tweet_id: Optional[str] = None, backup_tweet_id: Optional[str] = None):
        """Stores a closure in the database.
        
        Saves all information about a new closure to the database,
        including its details, status, and associated tweet IDs.
        
        Args:
            closure (Dict): Closure information dictionary
            closure_hash (str): Unique hash identifier for the closure
            tweet_id (str, optional): ID of the primary tweet
            backup_tweet_id (str, optional): ID of the backup dates tweet
        """
        try:
            cursor = self.db_conn.cursor()
            cursor.execute('''
                INSERT INTO road_closures (closure_type, subject, place, primary_start_datetime, primary_end_datetime,
                backup_start_datetime, backup_end_datetime, full_title, source_url, hash, status, tweet_id, backup_tweet_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (closure['closure_type'], closure['subject'], closure['place'],
                  closure['primary_start_datetime'], closure['primary_end_datetime'],
                  None, None, closure['full_title'], closure['source_url'], closure_hash, closure['status'], tweet_id, backup_tweet_id))
            self.db_conn.commit()
            logging.info(f"Stored closure: {closure['closure_type']} at {closure['place']} | Status: {closure['status']}")
        except sqlite3.IntegrityError:
            logging.warning(f"Duplicate closure ignored: {closure['full_title']}")
        except sqlite3.Error as e:
            logging.error(f"Database error: {e}")

    def update_closure_status(self, closure_hash: str, new_status: str, tweet_id: Optional[str] = None, backup_tweet_id: Optional[str] = None):
        """Updates the status of a closure.
        
        Changes the status of an existing closure (e.g., from active to revoked)
        and optionally updates associated tweet IDs.
        
        Args:
            closure_hash (str): Hash identifier of the closure
            new_status (str): New status to set ("active", "revoked", etc.)
            tweet_id (str, optional): New primary tweet ID
            backup_tweet_id (str, optional): New backup tweet ID
        """
        try:
            cursor = self.db_conn.cursor()
            if tweet_id and backup_tweet_id:
                cursor.execute('UPDATE road_closures SET status = ?, tweet_id = ?, backup_tweet_id = ? WHERE hash = ?', (new_status, tweet_id, backup_tweet_id, closure_hash))
            elif tweet_id:
                cursor.execute('UPDATE road_closures SET status = ?, tweet_id = ? WHERE hash = ?', (new_status, tweet_id, closure_hash))
            elif backup_tweet_id:
                cursor.execute('UPDATE road_closures SET status = ?, backup_tweet_id = ? WHERE hash = ?', (new_status, backup_tweet_id, closure_hash))
            else:
                cursor.execute('UPDATE road_closures SET status = ? WHERE hash = ?', (new_status, closure_hash))
            self.db_conn.commit()
            logging.info(f"Updated status to {new_status} for hash: {closure_hash}")
        except sqlite3.Error as e:
            logging.error(f"Error updating status: {e}")

    def construct_tweet(self, closure: Dict) -> str:
        """Constructs the primary tweet text.
        
        Creates a formatted tweet announcing a road closure,
        including emojis, timing information, and the source URL.
        
        Args:
            closure (Dict): Closure information dictionary
            
        Returns:
            str: Formatted tweet text ready for posting
        """
        tweet_text = f"🚧 {closure['subject']} Alert 🚧\n"
        if closure['subject'] == "Road Delay":
            tweet_text += f"🚗 Road Delay at {closure['place']}\n"
        elif closure['closure_type'] == "Flight Closure":
            tweet_text += f"✈️ {closure['closure_type']} at {closure['place']}\n"
        else:
            tweet_text += f"🚗 {closure['closure_type']} at {closure['place']}\n"

        if closure['primary_start_datetime'] and closure['primary_end_datetime']:
            start_str = self._format_datetime(closure['primary_start_datetime'])
            end_str = self._format_datetime(closure['primary_end_datetime'])
            if start_str and end_str:
                date_str = datetime.fromisoformat(closure['primary_start_datetime']).strftime('%B %d, %Y')
                start_time_str = datetime.fromisoformat(closure['primary_start_datetime']).strftime('%I:%M %p')
                end_time_str = datetime.fromisoformat(closure['primary_end_datetime']).strftime('%I:%M %p')
                tweet_text += f"📅 {date_str}\n⏰ {start_time_str} - {end_time_str} CST\n"
            else:
                tweet_text += "⚠️ Date/time unavailable\n"
        else:
            tweet_text += "⚠️ Date/time unavailable\n"

        tweet_text += f"Source: {closure['source_url']}"
        logging.info(f"Constructed tweet: {tweet_text}")
        return tweet_text

    def construct_backup_tweet(self, closure: Dict) -> str:
        """Constructs the backup dates tweet text.
        
        Creates a formatted tweet for announcing backup/alternative dates
        for a road closure.
        
        Args:
            closure (Dict): Closure information dictionary
            
        Returns:
            str: Formatted tweet text for backup dates, or None if no backup dates
        """
        if not closure.get('backup_dates'):
            logging.info("No backup dates found")
            return None
        tweet_text = f"🔄 Backup dates for {closure['closure_type']}:\n"
        for date in closure['backup_dates']:
            tweet_text += f"📅 {date}\n⏰ {datetime.fromisoformat(closure['primary_start_datetime']).strftime('%I:%M %p')} - {datetime.fromisoformat(closure['primary_end_datetime']).strftime('%I:%M %p')} CST\n"
        tweet_text += f"Source: {closure['source_url']}"
        logging.info(f"Constructed backup tweet: {tweet_text}")
        return tweet_text

    def post_tweet(self, tweet_text: str, max_retries=5, backoff_factor=1) -> Optional[str]:
        """Posts a tweet with retries and exponential backoff.
        
        Attempts to post a tweet to Twitter, with automatic retries on failure
        using exponential backoff to handle rate limits and temporary failures.
        
        Args:
            tweet_text (str): The text to tweet
            max_retries (int): Maximum number of retry attempts
            backoff_factor (int): Factor to calculate delay between retries
            
        Returns:
            str: Tweet ID if successful, None if failed
        """
        if not self.client:
            logging.error("Twitter client not initialized")
            return None
        for attempt in range(max_retries):
            try:
                response = self.client.create_tweet(text=tweet_text)
                tweet_id = response.data['id']
                logging.info(f"Tweet posted: {tweet_id}")
                return tweet_id
            except (tweepy.TweepyException, requests.exceptions.RequestException) as e:
                logging.error(f"Error posting tweet: {e}. Retrying in {backoff_factor * (2 ** attempt)} seconds...")
                time.sleep(backoff_factor * (2 ** attempt))
        logging.error(f"Failed to post tweet after {max_retries} attempts.")
        return None

    def print_closure_info(self, closure: Dict):
        """Prints closure info to console.
        
        Outputs formatted information about a closure to the console
        for monitoring and debugging purposes.
        
        Args:
            closure (Dict): Closure information dictionary
        """
        print(f"🚧 {closure['subject']} Alert 🚧")
        print(f"{closure['closure_type']} at {closure['place']}")
        if closure['primary_start_datetime'] and closure['primary_end_datetime']:
            start_str = self._format_datetime(closure['primary_start_datetime'])
            end_str = self._format_datetime(closure['primary_end_datetime'])
            print(f"  From: {start_str}\n  To:   {end_str}")
        else:
            print("  Date/Time: Unavailable")
        print(f"🔗 Source: {closure['source_url']}")
        logging.info(f"Printed info for {closure['closure_type']} at {closure['place']}")

    def print_revoked_info(self, closure: Dict):
        """Prints revoked closure info.
        
        Outputs information about a closure that has been revoked or cancelled.
        
        Args:
            closure (Dict): Closure information dictionary
        """
        print(f"✅ {closure['closure_type']} at {closure['place']} is no longer in effect.")
        logging.info(f"Printed revoked info for {closure['closure_type']} at {closure['place']}")

    def check_for_expired_closures(self):
        """Checks for and revokes expired closures.
        
        Periodically checks all active closures to see if their end time
        has passed, and if so, marks them as revoked in the database.
        """
        cursor = self.db_conn.cursor()
        now_utc = datetime.now(timezone.utc)
        cursor.execute("SELECT id, hash, full_title, primary_end_datetime, status, tweet_id, backup_tweet_id FROM road_closures WHERE status = 'active'")
        for closure_id, closure_hash, full_title, end_datetime_str, _, tweet_id, backup_tweet_id in cursor.fetchall():
            if end_datetime_str:
                try:
                    end_datetime = datetime.fromisoformat(end_datetime_str).replace(tzinfo=timezone.utc)
                    if now_utc > end_datetime:
                        self.update_closure_status(closure_hash, "revoked")
                        cursor.execute("SELECT closure_type, place FROM road_closures WHERE hash = ?", (closure_hash,))
                        result = cursor.fetchone()
                        if result:
                            closure_type, place = result
                            self.print_revoked_info({'closure_type': closure_type, 'place': place})
                except ValueError as e:
                    logging.error(f"Error processing expiry: {e}")

    def run(self):
        """Main loop to process closures.
        
        The main execution loop of the bot that:
        1. Fetches new closure announcements
        2. Processes each closure (up to MAX_URLS_PER_CYCLE)
        3. Posts tweets for new closures
        4. Updates status changes
        5. Checks for expired closures
        6. Sleeps for the configured interval
        
        The loop continues until interrupted by Ctrl+C or an unhandled error.
        """
        try:
            while True:
                logging.info("Starting closure check...")
                closure_urls = self.fetch_closure_urls()
                for url in closure_urls[:5]:  # Limit to first 5 URLs per cycle
                    closure = self.fetch_and_parse_detail_page(url)
                    if closure:
                        closure_hash = self.generate_closure_hash(closure)
                        stored_status, stored_tweet_id = self.get_stored_status_and_tweet_id(closure_hash)
                        if stored_status is None:
                            tweet_text = self.construct_tweet(closure)
                            tweet_id = self.post_tweet(tweet_text)
                            backup_tweet_text = self.construct_backup_tweet(closure)
                            backup_tweet_id = self.post_tweet(backup_tweet_text) if backup_tweet_text else None
                            self.store_closure(closure, closure_hash, tweet_id, backup_tweet_id)
                            self.print_closure_info(closure)
                        elif stored_status == 'active' and stored_tweet_id is None:
                            tweet_text = self.construct_tweet(closure)
                            tweet_id = self.post_tweet(tweet_text)
                            if tweet_id:
                                self.update_closure_tweet_id(closure_hash, tweet_id)
                        elif stored_status != closure['status']:
                            if closure['status'] == 'revoked':
                                self.update_closure_status(closure_hash, 'revoked')
                                self.print_revoked_info(closure)
                self.check_for_expired_closures()
                time.sleep(int(os.getenv('CHECK_INTERVAL', 60)))
        except KeyboardInterrupt:
            logging.info("Shutting down.")
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            #This might be the shittest code i have ever had the displeasure of writing

if __name__ == "__main__":
    bot = RoadClosureBot()
    bot.run()