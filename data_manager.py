"""
Data Manager
Handles CSV logging - DAILY AVERAGES ONLY (no hourly data)
"""

import os
import csv
import logging
import threading
from datetime import datetime, date
from typing import Dict, List, Any

log = logging.getLogger(__name__)

# CSV write lock
_csv_lock = threading.Lock()


def _safe_num(value, cast=float, fallback=None):
    """Safely cast a value"""
    try:
        return cast(value) if value not in (None, "", "—", "N/A") else fallback
    except (ValueError, TypeError):
        return fallback


class DataManager:
    """
    Single-file CSV manager: daily_log.csv
    One row per day - METADATA ONLY (no hourly columns)
    """
    
    def __init__(self, csv_file=None):
        if csv_file is None:
            # Use data directory
            try:
                from data_directory import DataDirectoryManager
                csv_file = DataDirectoryManager.get_csv_path()
            except ImportError:
                csv_file = "daily_log.csv"  # Fallback
        
        self.DAILY_LOG_FILE = csv_file
        
        # All columns (daily averages only)
        self.COLUMNS = [
            "date", "dawn", "dusk", "avg_temp", "avg_cloud", "effective_temp",
            "duration", "first_run", "second_run", "trigger_time", "ha_status",
            "manual_minutes"
        ]
        
        self._cache = None
        self._ensure_file_exists()
    
    def _ensure_file_exists(self):
        """Create daily_log.csv if it doesn't exist"""
        if not os.path.exists(self.DAILY_LOG_FILE):
            with _csv_lock:
                with open(self.DAILY_LOG_FILE, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(self.COLUMNS)
            log.info(f"Created {self.DAILY_LOG_FILE}")
    
    def invalidate(self):
        """Clear cache"""
        self._cache = None
    
    def save_record(self, row_data: Dict[str, Any]) -> bool:
        """
        Save one day's record.
        row_data must contain all COLUMNS fields.
        """
        try:
            # Atomic update
            with _csv_lock:
                existing_rows = self._read_all_rows()
                
                # Remove today's row if exists
                today_str = row_data["date"]
                existing_rows = [r for r in existing_rows if r.get("date") != today_str]
                
                # Append new row
                existing_rows.append(row_data)
                
                # Write back
                self._write_all_rows(existing_rows)
            
            log.info(f"Saved record for {today_str}")
            self.invalidate()
            return True
            
        except Exception as e:
            log.error(f"Failed to save record: {e}")
            return False
    
    def _read_all_rows(self) -> List[Dict[str, str]]:
        """Read all rows from CSV"""
        if not os.path.exists(self.DAILY_LOG_FILE):
            return []
        
        with open(self.DAILY_LOG_FILE, 'r', newline='') as f:
            reader = csv.DictReader(f)
            return list(reader)
    
    def _write_all_rows(self, rows: List[Dict[str, str]]):
        """Write all rows atomically"""
        if not rows:
            with open(self.DAILY_LOG_FILE, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(self.COLUMNS)
            return
        
        # Atomic write via temp file
        tmp_file = self.DAILY_LOG_FILE + ".tmp"
        with open(tmp_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self.COLUMNS)
            writer.writeheader()
            for row in rows:
                # Fill missing columns with empty string
                full_row = {col: row.get(col, "") for col in self.COLUMNS}
                writer.writerow(full_row)
        
        os.replace(tmp_file, self.DAILY_LOG_FILE)  # Atomic on Linux
    
    def all_records(self) -> List[Dict[str, Any]]:
        """Return all daily records, sorted by date"""
        if self._cache is not None:
            return self._cache
        
        rows = self._read_all_rows()
        
        # Sort by date
        try:
            rows.sort(key=lambda x: datetime.strptime(x["date"], "%Y-%m-%d"))
        except ValueError as e:
            log.warning(f"Could not sort by date: {e}")
        
        self._cache = rows
        return self._cache
    
    def retry_ha(self, ha_service):
        """
        Re-send today's values to HA.
        
        Args:
            ha_service: HAService instance
        
        Returns:
            (success: bool, status: str)
        """
        today_str = str(date.today())
        rows = self._read_all_rows()
        
        today_row = None
        for row in rows:
            if row.get("date") == today_str:
                today_row = row
                break
        
        if not today_row:
            log.warning("No record for today - cannot retry HA")
            return False, "No data for today"
        
        first_run = _safe_num(today_row.get("first_run"), cast=int, fallback=0)
        second_run = _safe_num(today_row.get("second_run"), cast=int, fallback=0)
        
        ha_ok = ha_service.send_first_run(first_run)
        ha_status = "OK" if ha_ok else "FAILED"
        
        # Update status and time
        today_row["ha_status"] = ha_status
        today_row["trigger_time"] = datetime.now().strftime("%H:%M:%S")
        
        # Write back
        with _csv_lock:
            all_rows = self._read_all_rows()
            updated_rows = [r if r.get("date") != today_str else today_row for r in all_rows]
            self._write_all_rows(updated_rows)
        
        log.info(f"Retry HA complete: {ha_status}")
        self.invalidate()
        return ha_ok, ha_status
