"""
File Watcher Module
TEAM 2 - SPRINT 1 (PHASE 1)
Monitors directories for new files and triggers processing
"""

import logging
import time
from pathlib import Path
from typing import List, Dict, Optional, Callable, Set
from datetime import datetime
import hashlib
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FileWatcher:
    """
    File system watcher for ETL pipeline
    Monitors directories for new files and triggers processing
    
    Features:
    - Watch multiple directories
    - File pattern filtering (*.csv, *.json, etc.)
    - Duplicate detection using file hashes
    - Processing history tracking
    - Callback function on new file detection
    """
    
    def __init__(self, watch_dirs: List[str], file_patterns: List[str] = None,
                 check_interval: int = 10, history_file: str = None):
        """
        Initialize file watcher
        
        Args:
            watch_dirs: List of directories to watch
            file_patterns: List of file patterns to match (e.g., ['*.csv', '*.json'])
            check_interval: Seconds between directory checks
            history_file: Path to processing history JSON file
        """
        self.watch_dirs = [Path(d) for d in watch_dirs]
        self.file_patterns = file_patterns or ['*']
        self.check_interval = check_interval
        self.history_file = Path(history_file) if history_file else Path('data/file_watch_history.json')
        
        # Track processed files
        self.processed_files: Set[str] = set()
        self.file_hashes: Dict[str, str] = {}
        
        # Load history
        self._load_history()
        
        logger.info(f"✅ FileWatcher initialized: {len(self.watch_dirs)} directories, patterns: {self.file_patterns}")
    
    def _load_history(self) -> None:
        """Load processing history from file"""
        if self.history_file.exists():
            try:
                with open(self.history_file, 'r') as f:
                    history = json.load(f)
                    self.processed_files = set(history.get('processed_files', []))
                    self.file_hashes = history.get('file_hashes', {})
                logger.info(f"📋 Loaded history: {len(self.processed_files)} processed files")
            except Exception as e:
                logger.warning(f"⚠️ Could not load history: {e}")
    
    def _save_history(self) -> None:
        """Save processing history to file"""
        try:
            self.history_file.parent.mkdir(parents=True, exist_ok=True)
            history = {
                'processed_files': list(self.processed_files),
                'file_hashes': self.file_hashes,
                'last_updated': datetime.now().isoformat()
            }
            with open(self.history_file, 'w') as f:
                json.dump(history, f, indent=2)
            logger.debug("💾 History saved")
        except Exception as e:
            logger.error(f"❌ Could not save history: {e}")
    
    def _calculate_file_hash(self, file_path: Path) -> str:
        """
        Calculate MD5 hash of file for duplicate detection
        
        Args:
            file_path: Path to file
            
        Returns:
            MD5 hash string
        """
        md5_hash = hashlib.md5()
        try:
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    md5_hash.update(chunk)
            return md5_hash.hexdigest()
        except Exception as e:
            logger.error(f"❌ Could not calculate hash for {file_path}: {e}")
            return ""
    
    def _matches_pattern(self, file_path: Path) -> bool:
        """
        Check if file matches any of the patterns
        
        Args:
            file_path: Path to file
            
        Returns:
            True if matches, False otherwise
        """
        for pattern in self.file_patterns:
            if file_path.match(pattern):
                return True
        return False
    
    def scan_directories(self) -> List[Dict[str, any]]:
        """
        Scan all watch directories for new files
        
        Returns:
            List of dictionaries with new file information
        """
        new_files = []
        
        for watch_dir in self.watch_dirs:
            if not watch_dir.exists():
                logger.warning(f"⚠️ Watch directory does not exist: {watch_dir}")
                continue
            
            logger.debug(f"🔍 Scanning directory: {watch_dir}")
            
            # Find matching files
            for pattern in self.file_patterns:
                for file_path in watch_dir.glob(pattern):
                    if not file_path.is_file():
                        continue
                    
                    file_str = str(file_path.absolute())
                    
                    # Skip if already processed
                    if file_str in self.processed_files:
                        continue
                    
                    # Calculate hash for duplicate detection
                    file_hash = self._calculate_file_hash(file_path)
                    
                    # Check if file with same hash was processed
                    if file_hash in self.file_hashes.values():
                        logger.info(f"⏭️ Skipping duplicate file: {file_path.name}")
                        self.processed_files.add(file_str)
                        continue
                    
                    # New file detected
                    file_info = {
                        'file_path': file_str,
                        'file_name': file_path.name,
                        'file_size_bytes': file_path.stat().st_size,
                        'file_size_mb': file_path.stat().st_size / 1024**2,
                        'modified_time': datetime.fromtimestamp(file_path.stat().st_mtime).isoformat(),
                        'detected_time': datetime.now().isoformat(),
                        'file_extension': file_path.suffix,
                        'directory': str(watch_dir),
                        'file_hash': file_hash
                    }
                    
                    new_files.append(file_info)
                    logger.info(f"🆕 New file detected: {file_path.name} ({file_info['file_size_mb']:.2f} MB)")
        
        return new_files
    
    def mark_as_processed(self, file_path: str, file_hash: str = None) -> None:
        """
        Mark file as processed
        
        Args:
            file_path: Path to processed file
            file_hash: File hash (optional)
        """
        self.processed_files.add(file_path)
        if file_hash:
            self.file_hashes[file_path] = file_hash
        self._save_history()
        logger.debug(f"✅ Marked as processed: {Path(file_path).name}")
    
    def watch(self, callback: Callable[[List[Dict]], None], 
             run_once: bool = False) -> None:
        """
        Start watching directories and call callback when new files detected
        
        Args:
            callback: Function to call with list of new file info dicts
            run_once: If True, scan once and exit. If False, keep watching
        """
        logger.info(f"👀 Starting file watch (interval: {self.check_interval}s)")
        
        try:
            while True:
                new_files = self.scan_directories()
                
                if new_files:
                    logger.info(f"📢 Detected {len(new_files)} new file(s)")
                    
                    # Call callback with new files
                    try:
                        callback(new_files)
                        
                        # Mark files as processed
                        for file_info in new_files:
                            self.mark_as_processed(
                                file_info['file_path'], 
                                file_info.get('file_hash')
                            )
                    except Exception as e:
                        logger.error(f"❌ Callback error: {e}")
                
                if run_once:
                    break
                
                time.sleep(self.check_interval)
                
        except KeyboardInterrupt:
            logger.info("⏹️ File watch stopped by user")
        except Exception as e:
            logger.error(f"❌ File watch error: {e}")
            raise
    
    def reset_history(self) -> None:
        """Clear processing history"""
        self.processed_files.clear()
        self.file_hashes.clear()
        self._save_history()
        logger.info("🔄 Processing history reset")
    
    def get_statistics(self) -> Dict[str, any]:
        """
        Get file watching statistics
        
        Returns:
            Statistics dictionary
        """
        stats = {
            'watch_directories': [str(d) for d in self.watch_dirs],
            'file_patterns': self.file_patterns,
            'total_processed_files': len(self.processed_files),
            'check_interval_seconds': self.check_interval,
            'history_file': str(self.history_file)
        }
        return stats


# ═══════════════════════════════════════════════════════════
# EXAMPLE USAGE
# ═══════════════════════════════════════════════════════════

def example_callback(new_files: List[Dict]) -> None:
    """Example callback function"""
    print(f"\n🔔 Processing {len(new_files)} new file(s):")
    for file_info in new_files:
        print(f"  - {file_info['file_name']} ({file_info['file_size_mb']:.2f} MB)")
        print(f"    Path: {file_info['file_path']}")
        print(f"    Modified: {file_info['modified_time']}")


if __name__ == "__main__":
    # Initialize watcher
    watcher = FileWatcher(
        watch_dirs=['data/raw/dataset'],
        file_patterns=['*.csv', '*.json'],
        check_interval=5,
        history_file='data/file_watch_history.json'
    )
    
    # Get statistics
    stats = watcher.get_statistics()
    print("File Watcher Statistics:")
    print(json.dumps(stats, indent=2))
    
    # Scan once
    print("\n🔍 Scanning for new files...")
    new_files = watcher.scan_directories()
    if new_files:
        example_callback(new_files)
    else:
        print("No new files detected")
    
    # Watch continuously (uncomment to run)
    # watcher.watch(callback=example_callback, run_once=False)
