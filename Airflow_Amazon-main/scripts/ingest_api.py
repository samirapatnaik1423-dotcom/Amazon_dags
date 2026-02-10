"""
API Data Ingestion Module
TEAM 2 - SPRINT 1 (PHASE 1)
Handles REST API and GraphQL data ingestion with authentication and rate limiting
"""

import logging
import requests
import time
import json
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
from pathlib import Path
import pandas as pd
from urllib.parse import urljoin

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class APIIngester:
    """
    Handles REST API data ingestion with support for:
    - REST and GraphQL APIs
    - Multiple authentication methods (API Key, Bearer Token, OAuth)
    - Rate limiting and retry logic
    - Pagination handling
    - Response caching
    """
    
    AUTH_TYPES = ['none', 'api_key', 'bearer', 'basic', 'oauth2']
    
    def __init__(self, base_url: str, auth_type: str = 'none', **auth_params):
        """
        Initialize API ingester
        
        Args:
            base_url: Base URL for API
            auth_type: Authentication type ('none', 'api_key', 'bearer', 'basic', 'oauth2')
            **auth_params: Authentication parameters (api_key, token, username, password, etc.)
        """
        self.base_url = base_url.rstrip('/')
        self.auth_type = auth_type
        self.auth_params = auth_params
        self.session = requests.Session()
        
        # Configure authentication
        self._configure_auth()
        
        # Rate limiting
        rate_limit_config = auth_params.get('rate_limit') or auth_params.get('rate_limit_config') or {}
        self.max_requests_per_minute = rate_limit_config.get(
            'requests_per_minute',
            auth_params.get('max_requests_per_minute', 60)
        )
        self.min_interval_seconds = rate_limit_config.get('min_interval_seconds', 0)
        self.respect_retry_after = rate_limit_config.get('respect_retry_after', True)
        self.request_timestamps = []
        self.last_request_time = None
        
        logger.info(f"✅ APIIngester initialized for {self.base_url}")
    
    def _configure_auth(self) -> None:
        """Configure session authentication based on auth_type"""
        if self.auth_type == 'api_key':
            # API Key in header
            header_name = self.auth_params.get('header_name', 'X-API-Key')
            api_key = self.auth_params.get('api_key')
            if api_key:
                self.session.headers[header_name] = api_key
                logger.info(f"🔐 Configured API Key authentication ({header_name})")
        
        elif self.auth_type == 'bearer':
            # Bearer token
            token = self.auth_params.get('token')
            if token:
                self.session.headers['Authorization'] = f'Bearer {token}'
                logger.info("🔐 Configured Bearer token authentication")
        
        elif self.auth_type == 'basic':
            # Basic auth
            username = self.auth_params.get('username')
            password = self.auth_params.get('password')
            if username and password:
                self.session.auth = (username, password)
                logger.info("🔐 Configured Basic authentication")
        
        elif self.auth_type == 'oauth2':
            logger.warning("⚠️ OAuth2 requires separate token acquisition. Provide 'token' in auth_params.")
        
        # Add custom headers
        custom_headers = self.auth_params.get('headers', {})
        self.session.headers.update(custom_headers)
    
    def _check_rate_limit(self) -> None:
        """Check and enforce rate limiting"""
        current_time = time.time()

        if self.min_interval_seconds and self.last_request_time:
            elapsed = current_time - self.last_request_time
            if elapsed < self.min_interval_seconds:
                sleep_time = self.min_interval_seconds - elapsed
                logger.info(f"⏳ Minimum interval enforced. Sleeping for {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
                current_time = time.time()
        
        # Remove timestamps older than 1 minute
        self.request_timestamps = [ts for ts in self.request_timestamps if current_time - ts < 60]
        
        # Check if we've hit the limit
        if len(self.request_timestamps) >= self.max_requests_per_minute:
            sleep_time = 60 - (current_time - self.request_timestamps[0])
            if sleep_time > 0:
                logger.info(f"⏳ Rate limit reached. Sleeping for {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
                self.request_timestamps = []
        
        self.request_timestamps.append(current_time)
        self.last_request_time = current_time
    
    def get(self, endpoint: str, params: Optional[Dict] = None, 
            retry: int = 3, timeout: int = 30) -> Dict[str, Any]:
        """
        Execute GET request
        
        Args:
            endpoint: API endpoint (relative to base_url)
            params: Query parameters
            retry: Number of retry attempts
            timeout: Request timeout in seconds
            
        Returns:
            JSON response as dictionary
        """
        url = urljoin(self.base_url, endpoint)
        logger.info(f"🌐 GET request: {url}")
        
        for attempt in range(retry):
            try:
                self._check_rate_limit()
                
                response = self.session.get(url, params=params, timeout=timeout)

                if response.status_code == 429 and attempt < retry - 1:
                    retry_after = response.headers.get('Retry-After')
                    if self.respect_retry_after and retry_after and retry_after.isdigit():
                        wait_seconds = int(retry_after)
                    else:
                        wait_seconds = min(60, 2 ** attempt)
                    logger.warning(f"⚠️ Rate limited (429). Retrying after {wait_seconds} seconds...")
                    time.sleep(wait_seconds)
                    continue

                response.raise_for_status()
                
                logger.info(f"✅ Request successful (status: {response.status_code})")
                return response.json()
                
            except requests.exceptions.HTTPError as e:
                logger.error(f"❌ HTTP error (attempt {attempt + 1}/{retry}): {e}")
                if attempt == retry - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
                
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ Request error (attempt {attempt + 1}/{retry}): {e}")
                if attempt == retry - 1:
                    raise
                time.sleep(2 ** attempt)
    
    def post(self, endpoint: str, data: Optional[Dict] = None,
             json_data: Optional[Dict] = None, retry: int = 3, timeout: int = 30) -> Dict[str, Any]:
        """
        Execute POST request
        
        Args:
            endpoint: API endpoint
            data: Form data
            json_data: JSON data
            retry: Number of retry attempts
            timeout: Request timeout in seconds
            
        Returns:
            JSON response as dictionary
        """
        url = urljoin(self.base_url, endpoint)
        logger.info(f"🌐 POST request: {url}")
        
        for attempt in range(retry):
            try:
                self._check_rate_limit()
                
                response = self.session.post(url, data=data, json=json_data, timeout=timeout)

                if response.status_code == 429 and attempt < retry - 1:
                    retry_after = response.headers.get('Retry-After')
                    if self.respect_retry_after and retry_after and retry_after.isdigit():
                        wait_seconds = int(retry_after)
                    else:
                        wait_seconds = min(60, 2 ** attempt)
                    logger.warning(f"⚠️ Rate limited (429). Retrying after {wait_seconds} seconds...")
                    time.sleep(wait_seconds)
                    continue

                response.raise_for_status()
                
                logger.info(f"✅ Request successful (status: {response.status_code})")
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ Request error (attempt {attempt + 1}/{retry}): {e}")
                if attempt == retry - 1:
                    raise
                time.sleep(2 ** attempt)
    
    def _extract_records(self, response: Any, pagination_config: Optional[Dict] = None) -> List[Dict]:
        """Extract records from response using configurable keys."""
        if isinstance(response, list):
            return response

        pagination_config = pagination_config or {}
        keys = []
        if pagination_config.get('results_key'):
            keys.append(pagination_config['results_key'])
        if pagination_config.get('data_key'):
            keys.append(pagination_config['data_key'])

        keys.extend(['results', 'data'])

        for key in keys:
            if isinstance(response, dict) and key in response:
                return response.get(key) or []

        return response if isinstance(response, list) else []

    def get_paginated(self, endpoint: str, params: Optional[Dict] = None,
                     pagination_type: str = 'offset', max_pages: Optional[int] = None,
                     pagination_config: Optional[Dict] = None) -> List[Dict]:
        """
        Handle paginated API responses
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            pagination_type: 'offset', 'page', or 'cursor'
            max_pages: Maximum number of pages to fetch
            
        Returns:
            List of all records from all pages
        """
        pagination_config = pagination_config or {}
        pagination_type = pagination_config.get('type', pagination_type)
        if pagination_type == 'offset' and pagination_config:
            if pagination_config.get('page_param') or pagination_config.get('page_size'):
                pagination_type = 'page'
            elif pagination_config.get('cursor_param') or pagination_config.get('cursor_response_key'):
                pagination_type = 'cursor'
        max_pages = pagination_config.get('max_pages', max_pages)
        logger.info(f"📄 Fetching paginated data from {endpoint} (type: {pagination_type})")
        
        all_records = []
        params = params or {}
        page = 1
        
        if pagination_type == 'offset':
            limit_param = pagination_config.get('limit_param', 'limit')
            offset_param = pagination_config.get('offset_param', 'offset')
            page_size = pagination_config.get('page_size', params.get(limit_param, 100))
            params.setdefault(limit_param, page_size)
            offset = pagination_config.get('start_offset', 0)
            
            while True:
                params[offset_param] = offset
                response = self.get(endpoint, params=params)
                
                records = self._extract_records(response, pagination_config)
                if not records:
                    break
                
                all_records.extend(records)
                logger.info(f"📄 Page {page}: Retrieved {len(records)} records (total: {len(all_records)})")
                
                offset += params[limit_param]
                page += 1
                
                if max_pages and page > max_pages:
                    break
        
        elif pagination_type == 'page':
            page_param = pagination_config.get('page_param', 'page')
            limit_param = pagination_config.get('limit_param', 'per_page')
            page_size = pagination_config.get('page_size', params.get(limit_param, 100))
            params.setdefault(limit_param, page_size)
            params.setdefault(page_param, 1)
            
            while True:
                response = self.get(endpoint, params=params)
                
                records = self._extract_records(response, pagination_config)
                if not records:
                    break
                
                all_records.extend(records)
                logger.info(f"📄 Page {page}: Retrieved {len(records)} records (total: {len(all_records)})")
                
                # Check if there's a next page
                next_key = pagination_config.get('next_key', 'next')
                has_more_key = pagination_config.get('has_more_key', 'has_more')
                if not response.get(next_key) and not response.get(has_more_key):
                    break
                
                params[page_param] += 1
                page += 1
                
                if max_pages and page > max_pages:
                    break
        
        elif pagination_type == 'cursor':
            cursor_param = pagination_config.get('cursor_param', 'cursor')
            cursor_response_key = pagination_config.get('cursor_response_key', 'next_cursor')
            next_cursor = None
            
            while True:
                if next_cursor:
                    params[cursor_param] = next_cursor
                
                response = self.get(endpoint, params=params)
                
                records = self._extract_records(response, pagination_config)
                if not records:
                    break
                
                all_records.extend(records)
                logger.info(f"📄 Page {page}: Retrieved {len(records)} records (total: {len(all_records)})")
                
                next_cursor = response.get(cursor_response_key) or response.get('next_cursor')
                if not next_cursor:
                    break
                
                page += 1
                
                if max_pages and page > max_pages:
                    break
        
        logger.info(f"✅ Pagination complete: {len(all_records)} total records from {page} pages")
        return all_records
    
    def ingest_to_dataframe(self, endpoint: str, params: Optional[Dict] = None,
                           paginated: bool = False, pagination_type: str = 'offset',
                           max_pages: Optional[int] = None,
                           pagination_config: Optional[Dict] = None) -> pd.DataFrame:
        """
        Ingest API data directly to DataFrame
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            paginated: Whether to handle pagination
            pagination_type: Pagination type ('offset', 'page', 'cursor')
            max_pages: Maximum pages to fetch
            
        Returns:
            DataFrame with API data
        """
        logger.info(f"📊 Ingesting API data to DataFrame from {endpoint}")
        
        pagination_config = pagination_config or {}
        if pagination_config.get('enabled'):
            paginated = True
            pagination_type = pagination_config.get('type', pagination_type)
            if pagination_type == 'offset':
                if pagination_config.get('page_param') or pagination_config.get('page_size'):
                    pagination_type = 'page'
                elif pagination_config.get('cursor_param') or pagination_config.get('cursor_response_key'):
                    pagination_type = 'cursor'
            max_pages = pagination_config.get('max_pages', max_pages)

        if paginated:
            records = self.get_paginated(endpoint, params, pagination_type, max_pages, pagination_config)
        else:
            response = self.get(endpoint, params)
            # Handle both dict and list responses
            if isinstance(response, list):
                records = response
            else:
                records = response.get('results', response.get('data', response))
        
        df = pd.DataFrame(records)
        logger.info(f"✅ Created DataFrame: {len(df)} records, {len(df.columns)} columns")
        return df
    
    def graphql_query(self, query: str, variables: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Execute GraphQL query
        
        Args:
            query: GraphQL query string
            variables: Query variables
            
        Returns:
            Query response
        """
        logger.info("🔍 Executing GraphQL query")
        
        endpoint = self.auth_params.get('graphql_endpoint', '/graphql')
        payload = {'query': query}
        if variables:
            payload['variables'] = variables
        
        response = self.post(endpoint, json_data=payload)
        
        if 'errors' in response:
            logger.error(f"❌ GraphQL errors: {response['errors']}")
            raise ValueError(f"GraphQL query failed: {response['errors']}")
        
        logger.info("✅ GraphQL query successful")
        return response.get('data', {})
    
    def save_to_csv(self, df: pd.DataFrame, output_path: Union[str, Path]) -> None:
        """
        Save ingested data to CSV
        
        Args:
            df: DataFrame to save
            output_path: Output CSV path
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"💾 Saving to CSV: {output_path}")
        df.to_csv(output_path, index=False)
        logger.info(f"✅ Successfully saved {len(df)} records")
    
    def get_ingestion_metadata(self, df: pd.DataFrame, endpoint: str) -> Dict[str, Any]:
        """
        Generate metadata about ingested API data
        
        Args:
            df: Ingested DataFrame
            endpoint: API endpoint
            
        Returns:
            Metadata dictionary
        """
        metadata = {
            'ingestion_timestamp': datetime.now().isoformat(),
            'api_base_url': self.base_url,
            'endpoint': endpoint,
            'auth_type': self.auth_type,
            'total_records': len(df),
            'total_columns': len(df.columns),
            'columns': list(df.columns),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024**2,
        }
        
        logger.info(f"📊 Ingestion metadata: {metadata['total_records']} records")
        return metadata


class APIIngestion:
    """Standardized API ingestion wrapper with pagination and rate limit configs."""

    def ingest_from_api(
        self,
        base_url: str,
        endpoint: str,
        table_name: str,
        auth_type: str = 'none',
        auth_params: Optional[Dict] = None,
        pagination_config: Optional[Dict] = None,
        rate_limit_config: Optional[Dict] = None,
        paginated: bool = False,
        pagination_type: str = 'offset',
        max_pages: Optional[int] = None,
    ) -> pd.DataFrame:
        auth_params = auth_params or {}
        if rate_limit_config:
            auth_params = {**auth_params, 'rate_limit': rate_limit_config}

        ingester = APIIngester(base_url=base_url, auth_type=auth_type, **auth_params)
        df = ingester.ingest_to_dataframe(
            endpoint=endpoint,
            paginated=paginated,
            pagination_type=pagination_type,
            max_pages=max_pages,
            pagination_config=pagination_config,
        )

        logger.info(f"✅ API ingestion complete for {table_name}: {len(df)} records")
        return df


# ═══════════════════════════════════════════════════════════
# EXAMPLE USAGE
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Example 1: Public API (no auth)
    ingester = APIIngester(base_url="https://jsonplaceholder.typicode.com")
    
    # Simple GET request
    data = ingester.get('/posts/1')
    print("Single post:", data)
    
    # Ingest to DataFrame
    df = ingester.ingest_to_dataframe('/posts', paginated=False)
    print(f"\nPosts DataFrame: {len(df)} records")
    print(df.head())
    
    # Example 2: API with authentication
    # ingester_auth = APIIngester(
    #     base_url="https://api.example.com",
    #     auth_type='api_key',
    #     api_key='your-api-key-here',
    #     header_name='X-API-Key',
    #     max_requests_per_minute=30
    # )
    # df = ingester_auth.ingest_to_dataframe('/data', paginated=True, pagination_type='page')
