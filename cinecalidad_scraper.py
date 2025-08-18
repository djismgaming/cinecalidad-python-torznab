import argparse
import base64
import json
import logging
import sqlite3
import threading
import time
import queue
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from urllib.parse import quote_plus, urlparse, parse_qs
import re

from flask import Flask, request, Response
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# --- CONFIGURATION ---
HOST = '0.0.0.0'
PORT = 9697
DB_FILE = 'cinecalidad_cache.sqlite'
SCRAPE_INTERVAL_SECONDS = 3600
BASE_URL = 'https://www.cinecalidad.vg'

# Default timeouts (in milliseconds)
DEFAULT_PAGE_TIMEOUT = 60000
DEFAULT_ELEMENT_TIMEOUT = 15000
DEFAULT_NAVIGATION_TIMEOUT = 30000

# --- ASYNC TASK QUEUE ---
scraping_queue = queue.Queue()
active_scraping_tasks = set()
scraping_lock = threading.Lock()

# --- LOGGING SETUP ---
def setup_logging(log_level='INFO'):
    """Setup logging with specified level"""
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {log_level}')
    
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Reduce noise from playwright
    logging.getLogger('playwright').setLevel(logging.WARNING)

# --- DATABASE LOGIC ---
def init_db():
    """Initialize SQLite database with movies table"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS movies (
            guid TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            year TEXT,
            details_page TEXT,
            poster TEXT,
            magnet_link TEXT,
            quality TEXT,
            language TEXT,
            size_bytes INTEGER,
            scraped_at TIMESTAMP,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Add index for better performance
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_title_search 
        ON movies(title COLLATE NOCASE)
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_scraped_at 
        ON movies(scraped_at DESC)
    ''')
    
    conn.commit()
    conn.close()
    logging.info("Database initialized successfully")

def extract_year_from_text(text):
    """Extract year from text, return 'N/A' if not found"""
    if not text:
        return 'N/A'
    
    # Look for 4-digit year
    year_match = re.search(r'\b(19|20)\d{2}\b', text)
    return year_match.group(0) if year_match else 'N/A'

def extract_quality_info(button_text):
    """Extract quality and language info from button text"""
    quality = 'Unknown'
    language = 'Unknown'
    
    if not button_text:
        return quality, language
    
    text_lower = button_text.lower()
    
    # Extract quality
    if '1080p' in text_lower:
        quality = '1080p'
    elif '720p' in text_lower:
        quality = '720p'
    elif '480p' in text_lower:
        quality = '480p'
    
    # Extract language
    if 'dual' in text_lower:
        language = 'Dual'
    elif 'latino' in text_lower:
        language = 'Latino'
    elif 'inglÃ©s' in text_lower or 'ingles' in text_lower:
        language = 'English'
    
    return quality, language

def scrape_movie_details(page, details_url, page_timeout=DEFAULT_PAGE_TIMEOUT, 
                        element_timeout=DEFAULT_ELEMENT_TIMEOUT, 
                        navigation_timeout=DEFAULT_NAVIGATION_TIMEOUT):
    """
    Scrape individual movie page for torrent information
    """
    try:
        logging.debug(f"Loading movie details page: {details_url}")
        page.goto(details_url, wait_until='domcontentloaded', timeout=page_timeout)
        
        # Look for torrent buttons - prefer "Dual 1080p" but accept any torrent link
        torrent_buttons = page.locator("a.btn:has-text('Torrent'), a.btn:has-text('Bittorrent')").all()
        
        if not torrent_buttons:
            logging.debug("No torrent buttons found")
            return None, 'Unknown', 'Unknown'
        
        logging.debug(f"Found {len(torrent_buttons)} torrent button(s)")
        
        # Try to find the best torrent link (prefer Dual 1080p)
        best_button = None
        best_quality = 'Unknown'
        best_language = 'Unknown'
        
        for button in torrent_buttons:
            try:
                # Get the text content near the button to determine quality/language
                parent = button.locator('xpath=..')
                try:
                    parent_text = parent.inner_text(timeout=element_timeout) if parent.count() > 0 else ""
                except Exception:
                    parent_text = ""
                
                quality, language = extract_quality_info(parent_text)
                logging.debug(f"Torrent button found - Quality: {quality}, Language: {language}")
                
                # Prefer Dual 1080p, but accept anything
                if quality == '1080p' and language == 'Dual':
                    best_button = button
                    best_quality = quality
                    best_language = language
                    break
                elif quality == '1080p' and not best_button:
                    best_button = button
                    best_quality = quality
                    best_language = language
                elif not best_button:
                    best_button = button
                    best_quality = quality
                    best_language = language
                    
            except Exception as e:
                logging.debug(f"Error analyzing torrent button: {str(e)}")
                continue
        
        if not best_button:
            logging.warning("No suitable torrent button found")
            return None, 'Unknown', 'Unknown'
        
        # Get the data-url attribute with timeout
        try:
            data_url_encoded = best_button.get_attribute('data-url', timeout=element_timeout)
        except Exception as e:
            logging.warning(f"Error getting data-url attribute: {str(e)}")
            return None, best_quality, best_language
            
        if not data_url_encoded:
            logging.warning("No data-url attribute found in torrent button")
            return None, best_quality, best_language
        
        try:
            # Decode the intermediate URL
            intermediate_url = base64.b64decode(data_url_encoded).decode('utf-8')
            logging.debug(f"Decoded intermediate URL: {intermediate_url}")
            
            # Navigate to intermediate page
            page.goto(intermediate_url, wait_until='domcontentloaded', timeout=navigation_timeout)
            
            # Look for magnet link with timeout
            try:
                magnet_locator = page.locator("a[href^='magnet:']")
                magnet_locator.wait_for(timeout=element_timeout)
                magnet_link = magnet_locator.get_attribute('href', timeout=element_timeout)
            except Exception:
                # Alternative: look in input field
                try:
                    input_locator = page.locator("input[value^='magnet:']")
                    input_locator.wait_for(timeout=element_timeout)
                    magnet_link = input_locator.get_attribute('value', timeout=element_timeout)
                except Exception:
                    logging.warning("No magnet link found on intermediate page")
                    return None, best_quality, best_language
            
            if magnet_link:
                logging.debug(f"Successfully extracted magnet link: {magnet_link[:50]}...")
                return magnet_link, best_quality, best_language
            else:
                logging.warning("Magnet link attribute was empty")
                return None, best_quality, best_language
                
        except Exception as e:
            logging.error(f"Error processing intermediate URL: {str(e)}")
            return None, best_quality, best_language
            
    except Exception as e:
        logging.error(f"Error scraping movie details from {details_url}: {str(e)}")
        return None, 'Unknown', 'Unknown'

# --- ASYNC BACKGROUND SCRAPING ---
def background_scraping_worker():
    """Worker thread que procesa tareas de scraping en background"""
    logging.info("Background scraping worker started")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-dev-shm-usage']
        )
        page = browser.new_page()
        
        # Set timeouts y headers
        page.set_default_timeout(DEFAULT_ELEMENT_TIMEOUT)
        page.set_default_navigation_timeout(DEFAULT_NAVIGATION_TIMEOUT)
        page.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        while True:
            try:
                # Obtener tarea de la cola (bloquea hasta que hay una tarea)
                task = scraping_queue.get(timeout=30)
                
                query = task.get('query')
                limit = task.get('limit', 10)
                task_id = task.get('task_id')
                
                logging.info(f"Processing background scraping task: {task_id} - Query: '{query}'")
                
                # Realizar scraping
                perform_background_scraping(page, query, limit)
                
                # Remover de tareas activas
                with scraping_lock:
                    active_scraping_tasks.discard(task_id)
                
                logging.info(f"Completed background scraping task: {task_id}")
                
            except queue.Empty:
                # Timeout en la cola, continuar esperando
                continue
            except Exception as e:
                logging.error(f"Error in background scraping worker: {str(e)}")
                with scraping_lock:
                    if 'task_id' in locals():
                        active_scraping_tasks.discard(task_id)

def perform_background_scraping(page, query, limit=10):
    """Realiza scraping en background sin bloquear el endpoint"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    try:
        if query:
            search_url = f"{BASE_URL}/?s={quote_plus(query)}"
        else:
            search_url = BASE_URL
            
        logging.debug(f"Background scraping URL: {search_url}")
        
        page.goto(search_url, wait_until='domcontentloaded', timeout=DEFAULT_PAGE_TIMEOUT)
        page.wait_for_selector('article', timeout=DEFAULT_ELEMENT_TIMEOUT)
        
        # Buscar artículos de películas
        article_locators = page.locator(
            'article:has(a[href*="/pelicula/"]):not(:has(div.selt)):not(:has(a[href*="click."]))'
        ).all()
        
        logging.info(f"Found {len(article_locators)} articles for background processing")
        
        processed = 0
        for i, article in enumerate(article_locators):
            if processed >= limit:
                break
                
            try:
                # Extraer información básica
                details_link = article.locator('a[href*="/pelicula/"]')
                details_url = details_link.get_attribute('href', timeout=DEFAULT_ELEMENT_TIMEOUT)
                
                if not details_url:
                    continue
                
                # Verificar si ya existe y tiene magnet link
                cursor.execute("SELECT magnet_link FROM movies WHERE guid=? AND magnet_link IS NOT NULL", (details_url,))
                if cursor.fetchone():
                    logging.debug(f"Movie already processed: {details_url}")
                    continue
                
                # Extraer título
                title_element = article.locator('a[href*="/pelicula/"] span.sr-only')
                title = title_element.inner_text(timeout=DEFAULT_ELEMENT_TIMEOUT) if title_element.count() > 0 else "Unknown Title"
                
                # Extraer año
                year_element = article.locator('div[style*="left: 5px"]')
                year_text = year_element.inner_text(timeout=DEFAULT_ELEMENT_TIMEOUT) if year_element.count() > 0 else ""
                year = extract_year_from_text(year_text)
                
                # Extraer poster
                poster_element = article.locator('img.rounded')
                poster_url = None
                if poster_element.count() > 0:
                    poster_url = (poster_element.get_attribute('data-src', timeout=DEFAULT_ELEMENT_TIMEOUT) or 
                                poster_element.get_attribute('src', timeout=DEFAULT_ELEMENT_TIMEOUT))
                
                logging.info(f"Background processing: {title}")
                
                # Scraping de detalles para magnet link
                magnet_link, quality, language = scrape_movie_details(
                    page, details_url, DEFAULT_PAGE_TIMEOUT, 
                    DEFAULT_ELEMENT_TIMEOUT, DEFAULT_NAVIGATION_TIMEOUT
                )
                
                # Guardar en BD
                cursor.execute('''
                    INSERT OR REPLACE INTO movies 
                    (guid, title, year, details_page, poster, magnet_link, quality, language, size_bytes, scraped_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    details_url, title, year, details_url, poster_url,
                    magnet_link, quality, language, 2147483648,
                    datetime.now(timezone.utc)
                ))
                
                conn.commit()
                processed += 1
                
                # Pequeña pausa para ser respetuoso
                time.sleep(1)
                
            except Exception as e:
                logging.error(f"Error processing article in background: {str(e)}")
                continue
        
        logging.info(f"Background scraping completed: {processed} movies processed")
        
    except Exception as e:
        logging.error(f"Error in background scraping: {str(e)}")
    finally:
        conn.close()

def queue_scraping_task(query, limit=10):
    """Encola una tarea de scraping si no está ya en proceso"""
    task_id = f"{query or 'latest'}_{int(time.time())}"
    
    with scraping_lock:
        # Verificar si ya hay una tarea similar en proceso
        similar_tasks = [task for task in active_scraping_tasks if query and query in task]
        if similar_tasks:
            logging.debug(f"Similar scraping task already active: {similar_tasks[0]}")
            return False
        
        # Agregar a tareas activas
        active_scraping_tasks.add(task_id)
    
    # Encolar tarea
    task = {
        'query': query,
        'limit': limit,
        'task_id': task_id
    }
    
    try:
        scraping_queue.put_nowait(task)
        logging.info(f"Queued background scraping task: {task_id}")
        return True
    except queue.Full:
        logging.warning("Scraping queue is full, discarding task")
        with scraping_lock:
            active_scraping_tasks.discard(task_id)
        return False

def is_search_stale(query, max_age_hours=2):
    """Verifica si los resultados de búsqueda son muy antiguos"""
    if not query:
        return False
        
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    try:
        # Buscar películas relacionadas con la query que sean recientes
        cursor.execute("""
            SELECT MAX(scraped_at) 
            FROM movies 
            WHERE (title LIKE ? OR title LIKE ?) 
            AND scraped_at > ?
        """, (
            f'%{query}%', 
            f'%{query.replace(" ", ".")}%',
            datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
        ))
        
        result = cursor.fetchone()[0]
        return result is None  # True si no hay resultados recientes
        
    finally:
        conn.close()

def start_async_scraping_worker():
    """Inicia el worker de scraping asíncrono"""
    worker_thread = threading.Thread(
        target=background_scraping_worker,
        name="ScrapingWorker",
        daemon=True
    )
    worker_thread.start()
    logging.info("Async scraping worker started")
    return worker_thread

# --- ORIGINAL SYNCHRONOUS SCRAPING (FALLBACK) ---
def scrape_and_store(query=None, limit=24, max_pages=2, 
                     page_timeout=DEFAULT_PAGE_TIMEOUT, 
                     element_timeout=DEFAULT_ELEMENT_TIMEOUT,
                     navigation_timeout=DEFAULT_NAVIGATION_TIMEOUT):
    """Original scraping function (kept for compatibility and testing)"""
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-dev-shm-usage']
        )
        page = browser.new_page()
        
        # Set timeouts
        page.set_default_timeout(element_timeout)
        page.set_default_navigation_timeout(navigation_timeout)
        
        # Set user agent to avoid detection
        page.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        try:
            processed_count = 0
            total_articles_found = 0
            
            # If searching, only check page 1
            pages_to_check = 1 if query else max_pages
            
            for page_num in range(1, pages_to_check + 1):
                if query:
                    start_url = f"{BASE_URL}/?s={quote_plus(query)}"
                    logging.info(f"Searching for query: '{query}'")
                else:
                    start_url = BASE_URL if page_num == 1 else f"{BASE_URL}/page/{page_num}/"
                    logging.info(f"Scraping page {page_num}: {start_url}")
                
                try:
                    page.goto(start_url, wait_until='domcontentloaded', timeout=page_timeout)
                    logging.debug(f"Page loaded successfully: {start_url}")
                    
                    # Wait for articles to load
                    page.wait_for_selector('article', timeout=element_timeout)
                    
                    # Find movie articles (exclude series and ads)
                    article_locators = page.locator(
                        'article:has(a[href*="/pelicula/"]):not(:has(div.selt)):not(:has(a[href*="click."]))'
                    ).all()
                    
                    page_articles = len(article_locators)
                    total_articles_found += page_articles
                    logging.info(f"Found {page_articles} movie articles on page {page_num}")
                    
                    if page_articles == 0:
                        logging.warning(f"No articles found on page {page_num}, stopping pagination")
                        break
                    
                    for i, article in enumerate(article_locators):
                        if processed_count >= limit:
                            logging.info(f"Reached processing limit of {limit}")
                            break
                        
                        try:
                            # Extract movie details from the article with explicit timeout
                            details_link = article.locator('a[href*="/pelicula/"]')
                            details_url = details_link.get_attribute('href', timeout=element_timeout)
                            
                            if not details_url:
                                logging.warning(f"No details URL found for article {i+1}")
                                continue
                            
                            # Check if already processed (for non-search queries)
                            if not query:
                                cursor.execute("SELECT magnet_link FROM movies WHERE guid=?", (details_url,))
                                existing = cursor.fetchone()
                                if existing and existing[0]:  # Has magnet link
                                    logging.debug(f"Skipping already processed movie: {details_url}")
                                    continue
                            
                            # Extract title with timeout
                            title_element = article.locator('a[href*="/pelicula/"] span.sr-only')
                            try:
                                title = title_element.inner_text(timeout=element_timeout) if title_element.count() > 0 else "Unknown Title"
                            except Exception:
                                title = "Unknown Title"
                                logging.debug(f"Could not extract title for article {i+1}")
                            
                            logging.info(f"Processing movie {processed_count + 1}/{limit}: {title}")
                            
                            # Extract year with timeout
                            year_element = article.locator('div[style*="left: 5px"]')
                            try:
                                year_text = year_element.inner_text(timeout=element_timeout) if year_element.count() > 0 else ""
                                year = extract_year_from_text(year_text)
                            except Exception:
                                year = 'N/A'
                                logging.debug(f"Could not extract year for: {title}")
                            
                            # Extract poster with timeout
                            poster_element = article.locator('img.rounded')
                            poster_url = None
                            if poster_element.count() > 0:
                                try:
                                    poster_url = (poster_element.get_attribute('data-src', timeout=element_timeout) or 
                                                poster_element.get_attribute('src', timeout=element_timeout))
                                except Exception:
                                    logging.debug(f"Could not extract poster for: {title}")
                            
                            logging.debug(f"Extracted basic info - Title: {title}, Year: {year}")
                            
                            # Now scrape the details page for torrent links
                            magnet_link, quality, language = scrape_movie_details(
                                page, details_url, page_timeout, element_timeout, navigation_timeout
                            )
                            
                            if magnet_link:
                                logging.info(f"Successfully extracted magnet link for: {title} ({quality}, {language})")
                            else:
                                logging.warning(f"No magnet link found for: {title}")
                            
                            # Store in database
                            cursor.execute('''
                                INSERT OR REPLACE INTO movies 
                                (guid, title, year, details_page, poster, magnet_link, quality, language, size_bytes, scraped_at)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                details_url, title, year, details_url, poster_url,
                                magnet_link, quality, language, 2147483648,  # Default size
                                datetime.now(timezone.utc)
                            ))
                            
                            conn.commit()
                            processed_count += 1
                            
                            # Small delay to be respectful
                            time.sleep(1)
                            
                        except Exception as e:
                            logging.error(f"Error processing article {i+1}: {str(e)}")
                            continue
                    
                    if processed_count >= limit:
                        break
                        
                except Exception as e:
                    logging.error(f"Error loading page {page_num}: {str(e)}")
                    continue
            
            logging.info(f"Scraping completed. Found {total_articles_found} articles, processed {processed_count} movies")
            
        except Exception as e:
            logging.error(f"Critical error in scraping process: {str(e)}")
        finally:
            browser.close()
            conn.close()

def background_scraper_task(max_pages=2, page_timeout=DEFAULT_PAGE_TIMEOUT,
                          element_timeout=DEFAULT_ELEMENT_TIMEOUT,
                          navigation_timeout=DEFAULT_NAVIGATION_TIMEOUT):
    """Background task to keep database updated (periodic scraping)"""
    logging.info("Starting periodic background scraper task...")
    while True:
        try:
            logging.info("Starting periodic scrape...")
            scrape_and_store(query=None, limit=48, max_pages=max_pages,
                           page_timeout=page_timeout, element_timeout=element_timeout,
                           navigation_timeout=navigation_timeout)
            logging.info(f"Periodic scrape completed. Sleeping for {SCRAPE_INTERVAL_SECONDS // 60} minutes.")
        except Exception as e:
            logging.error(f"Error in periodic background scraper: {str(e)}")
        
        time.sleep(SCRAPE_INTERVAL_SECONDS)

# --- WEB SERVER (FLASK) AND TORZNAB LOGIC ---
app = Flask(__name__)

def generate_torznab_xml(movies):
    """Generate Torznab-compatible XML from movies data"""
    rss = ET.Element('rss', version='2.0', attrib={'xmlns:torznab': 'http://torznab.com/schemas/2012/rss'})
    channel = ET.SubElement(rss, 'channel')
    ET.SubElement(channel, 'title').text = 'Cinecalidad Enhanced Async'
    ET.SubElement(channel, 'description').text = 'Enhanced Async Cinecalidad Feed with Background Scraping'
    ET.SubElement(channel, 'link').text = BASE_URL
    
    for movie in movies:
        if not movie.get('magnet_link'):  # Skip movies without magnet links
            continue
            
        item = ET.SubElement(channel, 'item')
        
        # Enhanced title with quality and language info
        title_parts = [movie['title']]
        if movie.get('year') and movie['year'] != 'N/A':
            title_parts.append(f"({movie['year']})")
        if movie.get('quality') and movie['quality'] != 'Unknown':
            title_parts.append(movie['quality'])
        if movie.get('language') and movie['language'] != 'Unknown':
            title_parts.append(movie['language'])
        
        title_text = ' '.join(title_parts)
        ET.SubElement(item, 'title').text = title_text
        ET.SubElement(item, 'guid', isPermaLink='false').text = movie['guid']
        ET.SubElement(item, 'link').text = movie['magnet_link']
        ET.SubElement(item, 'enclosure', url=movie['magnet_link'], type='application/x-bittorrent')
        
        # Format publication date
        scraped_at = movie.get('scraped_at')
        if isinstance(scraped_at, str):
            try:
                scraped_dt = datetime.fromisoformat(scraped_at.replace('Z', '+00:00'))
            except ValueError:
                scraped_dt = datetime.now(timezone.utc)
        else:
            scraped_dt = datetime.now(timezone.utc)
        
        ET.SubElement(item, 'pubDate').text = scraped_dt.strftime('%a, %d %b %Y %H:%M:%S %z')
        
        # Enhanced description
        description_parts = []
        if movie.get('quality'):
            description_parts.append(f"Quality: {movie['quality']}")
        if movie.get('language'):
            description_parts.append(f"Language: {movie['language']}")
        
        if description_parts:
            ET.SubElement(item, 'description').text = ' | '.join(description_parts)
        
        # Torznab attributes - Multiple categories for better Radarr compatibility
        ET.SubElement(item, 'torznab:attr', name='category', value='2000')  # Movies (parent)
        ET.SubElement(item, 'torznab:attr', name='category', value='2040')  # Movies/HD (default)
        
        # Add specific categories based on quality
        if movie.get('quality') == '720p':
            ET.SubElement(item, 'torznab:attr', name='category', value='2030')  # Movies/SD
        elif movie.get('quality') == '1080p':
            ET.SubElement(item, 'torznab:attr', name='category', value='2050')  # Movies/BluRay
            
        ET.SubElement(item, 'torznab:attr', name='size', value=str(movie.get('size_bytes', 2147483648)))
        ET.SubElement(item, 'torznab:attr', name='seeders', value='1')
        ET.SubElement(item, 'torznab:attr', name='peers', value='2')
        ET.SubElement(item, 'torznab:attr', name='magneturl', value=movie['magnet_link'])
        ET.SubElement(item, 'torznab:attr', name='downloadvolumefactor', value='0')  # Freeleech
        ET.SubElement(item, 'torznab:attr', name='uploadvolumefactor', value='1')
        
        # Additional attributes
        if movie.get('poster'):
            ET.SubElement(item, 'torznab:attr', name='poster', value=movie['poster'])

    return ET.tostring(rss, encoding='unicode', method='xml')

# Add these imports at the top of your file
# import requests

# TMDB API Configuration (no API key needed for basic usage)
TMDB_BASE_URL = 'https://api.themoviedb.org/3'
TMDB_API_KEY = None  # Optional - leave None for rate-limited but free access

def get_movie_title_from_imdb_id(imdb_id):
    """
    Convert IMDB ID to movie title using TMDB API
    Returns the movie title or None if not found
    """
    if not imdb_id:
        return None
    
    # Ensure IMDB ID has the 'tt' prefix
    if not imdb_id.startswith('tt'):
        imdb_id = f'tt{imdb_id}'
    
    try:
        # Use TMDB Find API to get movie info by IMDB ID
        url = f"{TMDB_BASE_URL}/find/{imdb_id}"
        params = {'external_source': 'imdb_id'}
        
        if TMDB_API_KEY:
            params['api_key'] = TMDB_API_KEY
        
        logging.debug(f"Fetching movie info for IMDB ID: {imdb_id}")
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        # Check if we found movie results
        if data.get('movie_results'):
            movie = data['movie_results'][0]  # Take the first result
            title = movie.get('title', movie.get('original_title'))
            year = None
            
            # Extract year from release_date
            release_date = movie.get('release_date')
            if release_date:
                try:
                    year = release_date.split('-')[0]
                except:
                    pass
            
            logging.info(f"IMDB ID {imdb_id} resolved to: '{title}' ({year})")
            return title, year
            
        logging.warning(f"No movie found for IMDB ID: {imdb_id}")
        return None, None
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching movie info for IMDB ID {imdb_id}: {str(e)}")
        return None, None
    except Exception as e:
        logging.error(f"Unexpected error resolving IMDB ID {imdb_id}: {str(e)}")
        return None, None

def clean_search_query(query):
    """
    Clean search query by removing year and other common suffixes that Radarr adds
    but that don't match Cinecalidad's title format
    """
    if not query:
        return query
    
    # Remove year patterns (2019, 2020, etc.) from the end
    # Pattern: 4 digits at the end, optionally preceded by space
    query = re.sub(r'\s+(19|20)\d{2}$', '', query)
    
    # Remove common quality/format suffixes that might be added
    # These patterns are case insensitive
    quality_patterns = [
        r'\s+(1080p|720p|480p|4k|uhd)$',
        r'\s+(bluray|brrip|webrip|hdtv|dvdrip)$',
        r'\s+(x264|x265|h264|h265)$'
    ]
    
    for pattern in quality_patterns:
        query = re.sub(pattern, '', query, flags=re.IGNORECASE)
    
    # Clean up extra spaces
    query = ' '.join(query.split())
    
    return query.strip()

@app.route('/api')
def torznab_endpoint_async():
    """Enhanced Async Torznab endpoint with fast response and background scraping"""
    query_type = request.args.get('t', '')
    search_query_raw = request.args.get('q', '').strip()
    search_query = clean_search_query(search_query_raw)  # Clean the query
    imdb_id = request.args.get('imdbid', '').strip()
    tmdb_id = request.args.get('tmdbid', '').strip()
    categories = request.args.get('cat', '').strip()
    
    # Handle limit and offset parameters
    try:
        limit = min(int(request.args.get('limit', 50)), 100)  # Max 100 results
    except (ValueError, TypeError):
        limit = 50
        
    try:
        offset = max(int(request.args.get('offset', 0)), 0)
    except (ValueError, TypeError):
        offset = 0
    
    # Log both raw and cleaned queries for debugging
    if search_query_raw != search_query:
        logging.info(f"Query cleaned: '{search_query_raw}' → '{search_query}'")
    
    logging.info(f"Torznab request - Type: {query_type}, Query: '{search_query}', IMDB: {imdb_id}, TMDB: {tmdb_id}, Categories: {categories}, Limit: {limit}, Offset: {offset}")

    if query_type == 'caps':
        xml_caps = """<?xml version="1.0" encoding="utf-8"?>
        <caps>
            <server version="1.1" title="Cinecalidad Enhanced Async" strapline="Enhanced Async Cinecalidad Indexer" 
                    email="admin@cinecalidad.custom" url="http://localhost:9697" image=""/>
            <limits max="100" default="50" />
            <registration available="no" open="no"/>
            <searching>
                <search available="yes" supportedParams="q,limit,offset,cat"/>
                <movie-search available="yes" supportedParams="q,limit,offset,cat,imdbid,tmdbid"/>
            </searching>
            <categories>
                <category id="2000" name="Movies">
                    <subcat id="2030" name="SD"/>
                    <subcat id="2040" name="HD"/>
                    <subcat id="2050" name="BluRay"/>
                    <subcat id="2060" name="3D"/>
                </category>
            </categories>
        </caps>
        """
        return Response(xml_caps, mimetype='application/xml')

    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        movies = []
        
        # Process categories - if specific categories requested, validate them
        movie_categories = ['2000', '2030', '2040', '2050', '2060']
        if categories:
            requested_cats = [cat.strip() for cat in categories.split(',')]
            # Check if any requested category is in our supported movie categories
            if not any(cat in movie_categories for cat in requested_cats):
                logging.warning(f"No supported movie categories in request: {categories}")
                # Return empty results if no movie categories requested
                xml_feed = generate_torznab_xml([])
                return Response(xml_feed, mimetype='application/xml')
        
        # IMDB ID RESOLUTION: Convert IMDB ID to search query
        resolved_title = None
        resolved_year = None
        
        if imdb_id and not search_query:
            logging.info(f"Resolving IMDB ID to title: {imdb_id}")
            resolved_title, resolved_year = get_movie_title_from_imdb_id(imdb_id)
            if resolved_title:
                search_query = resolved_title
                logging.info(f"IMDB ID {imdb_id} resolved to search query: '{search_query}'")
            else:
                logging.warning(f"Could not resolve IMDB ID: {imdb_id}")
        
        # Handle different search types
        if query_type == 'movie' or query_type == 'movie-search':
            # MOVIE SEARCH (Interactive Search from Radarr)
            logging.info(f"Movie search - Query: '{search_query}', IMDB: {imdb_id}, TMDB: {tmdb_id}")
            
            if search_query:
                # Enhanced search patterns for better matching
                search_patterns = [
                    f'%{search_query}%',  # Exact match
                    f'%{search_query.replace(" ", ".")}%',  # With dots
                    f'%{search_query.replace(" ", "%")}%'  # Flexible word matching
                ]
                
                # Try multiple search patterns
                for pattern in search_patterns:
                    cursor.execute("""
                        SELECT * FROM movies 
                        WHERE title LIKE ? 
                        AND magnet_link IS NOT NULL 
                        ORDER BY scraped_at DESC 
                        LIMIT ? OFFSET ?
                    """, (pattern, limit, offset))
                    
                    movies = [dict(row) for row in cursor.fetchall()]
                    if movies:  # If we found results, break
                        logging.info(f"Found {len(movies)} movies with pattern: {pattern}")
                        break
                
                # If no results or few results, do immediate scraping
                if len(movies) < 3:
                    logging.info(f"Few results found ({len(movies)}), triggering immediate scraping")
                    try:
                        # Quick synchronous scrape for interactive search
                        scrape_and_store(query=search_query, limit=5, max_pages=1)
                        
                        # Query again after scraping with all patterns
                        for pattern in search_patterns:
                            cursor.execute("""
                                SELECT * FROM movies 
                                WHERE title LIKE ? 
                                AND magnet_link IS NOT NULL 
                                ORDER BY scraped_at DESC 
                                LIMIT ? OFFSET ?
                            """, (pattern, limit, offset))
                            
                            new_movies = [dict(row) for row in cursor.fetchall()]
                            if new_movies:
                                movies = new_movies
                                logging.info(f"After immediate scraping: {len(movies)} results with pattern: {pattern}")
                                break
                        
                    except Exception as e:
                        logging.error(f"Error in immediate scraping: {str(e)}")
                
                # También encolar para background scraping
                if len(movies) < 5 or is_search_stale(search_query):
                    queue_scraping_task(search_query, limit=15)
                    
            else:
                logging.info("Movie search without query - returning recent releases")
                cursor.execute("""
                    SELECT * FROM movies 
                    WHERE magnet_link IS NOT NULL 
                    ORDER BY scraped_at DESC 
                    LIMIT ? OFFSET ?
                """, (limit, offset))
                movies = [dict(row) for row in cursor.fetchall()]
                
        elif search_query:
            # REGULAR SEARCH (RSS search from Radarr)
            logging.info(f"Regular search for: '{search_query}' (limit: {limit}, offset: {offset})")
            
            # Enhanced search with multiple patterns
            search_patterns = [
                f'%{search_query}%',
                f'%{search_query.replace(" ", ".")}%',
                f'%{search_query.replace(" ", "%")}%'
            ]
            
            for pattern in search_patterns:
                cursor.execute("""
                    SELECT * FROM movies 
                    WHERE title LIKE ? 
                    AND magnet_link IS NOT NULL 
                    ORDER BY scraped_at DESC 
                    LIMIT ? OFFSET ?
                """, (pattern, limit, offset))
                
                movies = [dict(row) for row in cursor.fetchall()]
                if movies:
                    break
            
            # 2. SCRAPING ASÍNCRONO: Si los resultados son escasos o antiguos
            if len(movies) < 3 or is_search_stale(search_query):
                queue_scraping_task(search_query, limit=15)
                logging.info(f"Queued background scraping for: '{search_query}'")
            
            logging.info(f"Returning {len(movies)} movies immediately (background scraping queued)")
            
        else:
            # RSS feed - return recent movies with magnet links
            cursor.execute("""
                SELECT * FROM movies 
                WHERE magnet_link IS NOT NULL 
                ORDER BY scraped_at DESC 
                LIMIT ? OFFSET ?
            """, (limit, offset))
            movies = [dict(row) for row in cursor.fetchall()]
            
            # Encolar scraping general si no hay suficientes películas recientes
            cursor.execute("""
                SELECT COUNT(*) 
                FROM movies 
                WHERE scraped_at > ? AND magnet_link IS NOT NULL
            """, (datetime.now(timezone.utc) - timedelta(hours=4),))
            
            recent_count = cursor.fetchone()[0]
            if recent_count < 20:
                queue_scraping_task(None, limit=25)
                logging.info("Queued background scraping for latest movies")
        
        logging.info(f"Final result: {len(movies)} movies found")
        
    except Exception as e:
        logging.error(f"Database query error: {str(e)}")
        movies = []
    finally:
        conn.close()

    xml_feed = generate_torznab_xml(movies)
    return Response(xml_feed, mimetype='application/xml')

def torznab_endpoint_sync():
    """Original synchronous endpoint (for fallback/testing)"""
    query_type = request.args.get('t', '')
    search_query_raw = request.args.get('q', '').strip()
    search_query = clean_search_query(search_query_raw)  # Clean the query
    imdb_id = request.args.get('imdbid', '').strip()
    tmdb_id = request.args.get('tmdbid', '').strip()
    categories = request.args.get('cat', '').strip()
    
    # Handle limit and offset parameters
    try:
        limit = min(int(request.args.get('limit', 50)), 100)
    except (ValueError, TypeError):
        limit = 50
        
    try:
        offset = max(int(request.args.get('offset', 0)), 0)
    except (ValueError, TypeError):
        offset = 0
    
    # Log query cleaning for debugging
    if search_query_raw != search_query:
        logging.info(f"SYNC Query cleaned: '{search_query_raw}' → '{search_query}'")
    
    logging.info(f"Torznab SYNC request - Type: {query_type}, Query: '{search_query}', IMDB: {imdb_id}, TMDB: {tmdb_id}, Categories: {categories}")

    if query_type == 'caps':
        xml_caps = """<?xml version="1.0" encoding="utf-8"?>
        <caps>
            <server version="1.1" title="Cinecalidad Enhanced" strapline="Enhanced Cinecalidad Indexer" 
                    email="admin@cinecalidad.custom" url="http://localhost:9697" image=""/>
            <limits max="100" default="50" />
            <registration available="no" open="no"/>
            <registration available="no" open="no"/>
            <searching>
                <search available="yes" supportedParams="q,limit,offset,cat"/>
                <movie-search available="yes" supportedParams="q,limit,offset,cat,imdbid,tmdbid"/>
            </searching>
            <categories>
                <category id="2000" name="Movies">
                    <subcat id="2030" name="SD"/>
                    <subcat id="2040" name="HD"/>
                    <subcat id="2050" name="BluRay"/>
                    <subcat id="2060" name="3D"/>
                </category>
            </categories>
        </caps>
        """
        return Response(xml_caps, mimetype='application/xml')

    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        movies = []
        
        # Process categories - validate movie categories
        movie_categories = ['2000', '2030', '2040', '2050', '2060']
        if categories:
            requested_cats = [cat.strip() for cat in categories.split(',')]
            if not any(cat in movie_categories for cat in requested_cats):
                logging.warning(f"No supported movie categories in request: {categories}")
                xml_feed = generate_torznab_xml([])
                return Response(xml_feed, mimetype='application/xml')
        
        if query_type == 'movie' or query_type == 'movie-search':
            # MOVIE SEARCH (Interactive Search)
            logging.info(f"SYNC Movie search - Query: '{search_query}', IMDB: {imdb_id}, TMDB: {tmdb_id}")
            
            if search_query:
                # Do immediate scraping for movie searches
                scrape_and_store(query=search_query, limit=10)
                
                # Enhanced search with multiple patterns
                search_patterns = [
                    f'%{search_query}%',
                    f'%{search_query.replace(" ", ".")}%',
                    f'%{search_query.replace(" ", "%")}%'
                ]
                
                for pattern in search_patterns:
                    cursor.execute("""
                        SELECT * FROM movies 
                        WHERE title LIKE ? 
                        AND magnet_link IS NOT NULL 
                        ORDER BY scraped_at DESC 
                        LIMIT ? OFFSET ?
                    """, (pattern, limit, offset))
                    
                    movies = [dict(row) for row in cursor.fetchall()]
                    if movies:
                        logging.info(f"SYNC found {len(movies)} movies with pattern: {pattern}")
                        break
            else:
                # ID-based search without query
                cursor.execute("""
                    SELECT * FROM movies 
                    WHERE magnet_link IS NOT NULL 
                    ORDER BY scraped_at DESC 
                    LIMIT ? OFFSET ?
                """, (limit, offset))
                movies = [dict(row) for row in cursor.fetchall()]
                
        elif search_query:
            logging.info(f"Performing SYNC regular search for: '{search_query}'")
            # Regular search
            scrape_and_store(query=search_query, limit=10)
            
            # Enhanced search with multiple patterns
            search_patterns = [
                f'%{search_query}%',
                f'%{search_query.replace(" ", ".")}%',
                f'%{search_query.replace(" ", "%")}%'
            ]
            
            for pattern in search_patterns:
                cursor.execute("""
                    SELECT * FROM movies 
                    WHERE title LIKE ? 
                    AND magnet_link IS NOT NULL 
                    ORDER BY scraped_at DESC 
                    LIMIT ? OFFSET ?
                """, (pattern, limit, offset))
                
                movies = [dict(row) for row in cursor.fetchall()]
                if movies:
                    break
            
        else:
            # RSS feed - return recent movies with magnet links
            cursor.execute("""
                SELECT * FROM movies 
                WHERE magnet_link IS NOT NULL 
                ORDER BY scraped_at DESC 
                LIMIT ? OFFSET ?
            """, (limit, offset))
            movies = [dict(row) for row in cursor.fetchall()]
        
        logging.info(f"Returning {len(movies)} movies in XML feed (SYNC)")
        
    except Exception as e:
        logging.error(f"Database query error: {str(e)}")
        movies = []
    finally:
        conn.close()

    xml_feed = generate_torznab_xml(movies)
    return Response(xml_feed, mimetype='application/xml')

@app.route('/stats')
def stats_endpoint():
    """Statistics endpoint for monitoring"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT COUNT(*) as total FROM movies")
        total = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) as with_magnets FROM movies WHERE magnet_link IS NOT NULL")
        with_magnets = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(*) as recent 
            FROM movies 
            WHERE scraped_at > datetime('now', '-24 hours')
        """)
        recent = cursor.fetchone()[0]
        
        # Async queue stats
        with scraping_lock:
            active_tasks = list(active_scraping_tasks)
        
        stats = {
            'total_movies': total,
            'movies_with_magnets': with_magnets,
            'scraped_last_24h': recent,
            'success_rate': f"{(with_magnets/total*100):.1f}%" if total > 0 else "0%",
            'async_queue_size': scraping_queue.qsize(),
            'active_scraping_tasks': len(active_tasks),
            'worker_alive': any(t.name == 'ScrapingWorker' for t in threading.enumerate())
        }
        
        return Response(json.dumps(stats, indent=2), mimetype='application/json')
        
    except Exception as e:
        return Response(json.dumps({'error': str(e)}), mimetype='application/json', status=500)
    finally:
        conn.close()

@app.route('/queue-status')
def queue_status():
    """Endpoint para monitorear el estado de la cola de scraping"""
    with scraping_lock:
        active_tasks = list(active_scraping_tasks)
    
    status = {
        'queue_size': scraping_queue.qsize(),
        'active_tasks': active_tasks,
        'worker_alive': any(t.name == 'ScrapingWorker' for t in threading.enumerate()),
        'all_threads': [t.name for t in threading.enumerate()]
    }
    
    return Response(json.dumps(status, indent=2), mimetype='application/json')

@app.route('/health')
def health_check():
    """Health check endpoint"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM movies LIMIT 1")
        db_ok = True
        conn.close()
    except Exception:
        db_ok = False
    
    health = {
        'status': 'healthy' if db_ok else 'unhealthy',
        'database': 'ok' if db_ok else 'error',
        'async_worker': any(t.name == 'ScrapingWorker' for t in threading.enumerate()),
        'timestamp': datetime.now(timezone.utc).isoformat()
    }
    
    status_code = 200 if db_ok else 503
    return Response(json.dumps(health, indent=2), mimetype='application/json', status=status_code)

# ... (all of your script's functions go above this line, unchanged) ...

# --- MAIN ENTRY POINT & ARGUMENT HANDLING ---
if __name__ == "__main__":
    # --- Command-Line Argument Parser Setup ---
    parser = argparse.ArgumentParser(
        description="""Advanced All-in-One Scraper and Server for Cinecalidad.

This script implements a complete Prowlarr indexer with advanced features:
- SERVER MODE (default): Starts a web server that instantly responds to Prowlarr's
  requests using a local cache (SQLite database).
- ASYNCHRONOUS SCRAPING: New search queries queue a background scraping task
  without making Prowlarr wait, resulting in a very fast user experience.
- PERIODIC SCRAPING: A background worker periodically scrapes for the latest
  releases to keep the cache fresh.
- MONITORING ENDPOINTS: Includes routes like /stats, /health, and /queue-status
  to monitor the indexer's health and status.
- TEST MODE: Allows for one-off scraper runs for debugging purposes.
""",
        # Formatter to preserve line breaks in the description
        formatter_class=argparse.RawTextHelpFormatter
    )

    # --- Argument Groups for a Cleaner Help Menu ---
    testing_group = parser.add_argument_group('Test Mode Arguments')
    scraping_group = parser.add_argument_group('Scraping Control Arguments')
    timeout_group = parser.add_argument_group('Timeout Configuration Arguments')
    server_group = parser.add_argument_group('Server Configuration Arguments')

    # --- Argument Definitions ---
    testing_group.add_argument(
        '--test-scrape',
        action='store_true',
        help='Activates test mode. The script will run the scraper once and then exit.'
    )
    testing_group.add_argument(
        '-q', '--query',
        type=str,
        default=None,
        help='(For --test-scrape only) The search term for the scraper.'
    )
    testing_group.add_argument(
        '-l', '--limit',
        type=int,
        default=5,
        help='(For --test-scrape only) Limit the number of results to process in test mode.'
    )

    scraping_group.add_argument(
        '--max-pages',
        type=int,
        default=2,
        help='Maximum number of pages to crawl during periodic background scraping.'
    )
    scraping_group.add_argument(
        '--disable-async',
        action='store_true',
        help='Disables the async scraping worker and uses the synchronous endpoint (/api-sync).'
    )
    scraping_group.add_argument(
        '--disable-periodic',
        action='store_true',
        help='Disables the periodic scraping that keeps the cache updated.'
    )
    
    timeout_group.add_argument(
        '--page-timeout', type=int, default=DEFAULT_PAGE_TIMEOUT//1000,
        help=f"Page load timeout in seconds (default: {DEFAULT_PAGE_TIMEOUT//1000}s)."
    )
    timeout_group.add_argument(
        '--element-timeout', type=int, default=DEFAULT_ELEMENT_TIMEOUT//1000,
        help=f"Element operation timeout in seconds (default: {DEFAULT_ELEMENT_TIMEOUT//1000}s)."
    )
    timeout_group.add_argument(
        '--navigation-timeout', type=int, default=DEFAULT_NAVIGATION_TIMEOUT//1000,
        help=f"Navigation timeout in seconds (default: {DEFAULT_NAVIGATION_TIMEOUT//1000}s)."
    )

    server_group.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Sets the console logging verbosity level.'
    )
    
    args = parser.parse_args()

    # Convert timeout arguments to milliseconds for function calls
    page_timeout = args.page_timeout * 1000
    element_timeout = args.element_timeout * 1000
    navigation_timeout = args.navigation_timeout * 1000

    # Setup logging with the specified level
    setup_logging(args.log_level)
    
    # Initialize the database
    init_db()

    if args.test_scrape:
        logging.info("=== SCRAPER TEST MODE ===")
        logging.info(f"Query: {'Latest releases' if not args.query else args.query}")
        logging.info(f"Limit: {args.limit}")
        logging.info(f"Log Level: {args.log_level}")
        logging.info(f"Timeouts - Page: {args.page_timeout}s, Element: {args.element_timeout}s, Navigation: {args.navigation_timeout}s")
        
        # Use the original synchronous function for direct testing
        scrape_and_store(
            query=args.query, limit=args.limit, max_pages=1,
            page_timeout=page_timeout, element_timeout=element_timeout,
            navigation_timeout=navigation_timeout
        )
        logging.info("=== SCRAPER TEST COMPLETED ===")
    else:
        logging.info("=== STARTING ENHANCED ASYNC INDEXER ===")
        logging.info(f"Host: {HOST}:{PORT}")
        logging.info(f"Database: {DB_FILE}")
        logging.info(f"Async Mode: {'Disabled' if args.disable_async else 'Enabled'}")
        logging.info(f"Periodic Scraping: {'Disabled' if args.disable_periodic else 'Enabled'}")
        logging.info(f"Max Pages (periodic): {args.max_pages}")
        logging.info(f"Log Level: {args.log_level}")
        logging.info(f"Timeouts - Page: {args.page_timeout}s, Element: {args.element_timeout}s, Navigation: {args.navigation_timeout}s")
        
        if not args.disable_async:
            start_async_scraping_worker()
        
        if not args.disable_periodic:
            scraper_thread = threading.Thread(
                target=background_scraper_task, 
                args=(args.max_pages, page_timeout, element_timeout, navigation_timeout),
                name="PeriodicScraperThread", 
                daemon=True
            )
            scraper_thread.start()
        
        logging.info(f"Web server starting at http://{HOST}:{PORT}")
        logging.info("Available endpoints:")
        logging.info(f"  - /api ({'Async' if not args.disable_async else 'Sync'} Torznab endpoint)")
        if not args.disable_async:
            logging.info("  - /api-sync (Synchronous Torznab endpoint - fallback)")
            logging.info("  - /queue-status (Async queue monitoring)")
        logging.info("  - /stats (Statistics)")
        logging.info("  - /health (Health check)")
        
        try:
            app.run(host=HOST, port=PORT, debug=False, threaded=True)
        except KeyboardInterrupt:
            logging.info("Shutting down gracefully...")
        except Exception as e:
            logging.error(f"Server error: {str(e)}")
        finally:
            logging.info("Server stopped.")