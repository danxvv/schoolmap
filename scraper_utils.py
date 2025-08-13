#!/usr/bin/env python3
"""
Utility functions for managing the scraping process.

This module provides helper functions for monitoring progress,
managing failed codes, and resetting the scraper state.
"""

import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple


def check_progress(progress_file: str = "scraper_progress.txt") -> Dict[str, int]:
    """Check the current progress of scraping."""
    stats = {
        "total_processed": 0,
        "unique_codes": set(),
        "coordinates_found": 0
    }
    
    if not Path(progress_file).exists():
        print(f"Progress file not found: {progress_file}")
        return stats
    
    with open(progress_file, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                parts = line.split('-')
                if len(parts) >= 2:
                    ct_code = parts[0].strip()
                    stats["unique_codes"].add(ct_code)
                    stats["total_processed"] += 1
                    stats["coordinates_found"] += 1
    
    stats["unique_codes"] = len(stats["unique_codes"])
    return stats


def check_failed_codes(failed_file: str = "failed_ct_codes.txt") -> List[Tuple[str, str, str]]:
    """Check failed codes and their error messages."""
    failed = []
    
    if not Path(failed_file).exists():
        return failed
    
    with open(failed_file, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                parts = line.split('|')
                if len(parts) >= 3:
                    ct_code = parts[0].strip()
                    error = parts[1].strip()
                    timestamp = parts[2].strip()
                    failed.append((ct_code, error, timestamp))
    
    return failed


def reset_progress(
    backup: bool = True,
    progress_file: str = "scraper_progress.txt",
    output_file: str = "ct_codes_coords_googlelinks.txt",
    failed_file: str = "failed_ct_codes.txt",
    log_file: str = "scraper_log.txt"
):
    """Reset all progress files, optionally creating backups."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    files_to_reset = [
        (progress_file, "progress"),
        (output_file, "output"),
        (failed_file, "failed"),
        (log_file, "log")
    ]
    
    for file_path, file_type in files_to_reset:
        path = Path(file_path)
        if path.exists():
            if backup:
                backup_name = f"{path.stem}_{timestamp}{path.suffix}"
                backup_path = path.parent / backup_name
                path.rename(backup_path)
                print(f"✅ Backed up {file_type} to: {backup_path}")
            else:
                path.unlink()
                print(f"✅ Deleted {file_type}: {file_path}")
        else:
            print(f"ℹ️ {file_type.capitalize()} file not found: {file_path}")
    
    print("\n✨ Progress reset complete!")


def merge_results(
    *result_files: str,
    output_file: str = "merged_results.txt"
) -> int:
    """Merge multiple result files, removing duplicates."""
    all_results = {}
    
    for file_path in result_files:
        if Path(file_path).exists():
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        parts = line.split('-', 1)
                        if len(parts) >= 2:
                            ct_code = parts[0].strip()
                            rest = parts[1].strip()
                            all_results[ct_code] = rest
    
    # Write merged results
    with open(output_file, 'w', encoding='utf-8') as f:
        for ct_code, rest in sorted(all_results.items()):
            f.write(f"{ct_code}-{rest}\n")
    
    print(f"✅ Merged {len(all_results)} unique results to: {output_file}")
    return len(all_results)


def extract_coordinates_to_csv(
    input_file: str = "ct_codes_coords_googlelinks.txt",
    output_file: str = "coordinates.csv"
):
    """Extract coordinates to CSV format."""
    import csv
    
    if not Path(input_file).exists():
        print(f"Input file not found: {input_file}")
        return
    
    results = []
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                parts = line.split('-')
                if len(parts) >= 3:
                    ct_code = parts[0].strip()
                    coords = parts[1].strip()
                    google_link = '-'.join(parts[2:]).strip()
                    
                    if ',' in coords:
                        lat, lng = coords.split(',')
                        results.append({
                            'ct_code': ct_code,
                            'latitude': lat,
                            'longitude': lng,
                            'google_maps_url': google_link
                        })
    
    # Write to CSV
    if results:
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['ct_code', 'latitude', 'longitude', 'google_maps_url']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        
        print(f"✅ Exported {len(results)} coordinates to: {output_file}")
    else:
        print("No coordinates found to export.")


def main():
    """Main utility interface."""
    if len(sys.argv) < 2:
        print("Usage: python scraper_utils.py [command]")
        print("\nAvailable commands:")
        print("  progress    - Check current scraping progress")
        print("  failed      - List failed CT codes")
        print("  reset       - Reset all progress (with backup)")
        print("  reset-hard  - Reset all progress (without backup)")
        print("  merge       - Merge multiple result files")
        print("  csv         - Export coordinates to CSV")
        return
    
    command = sys.argv[1].lower()
    
    if command == "progress":
        stats = check_progress()
        print("\n📊 Scraping Progress:")
        print(f"  Total processed: {stats['total_processed']}")
        print(f"  Unique codes: {stats['unique_codes']}")
        print(f"  Coordinates found: {stats['coordinates_found']}")
        
    elif command == "failed":
        failed = check_failed_codes()
        if failed:
            print(f"\n❌ Failed Codes ({len(failed)} total):")
            for ct_code, error, timestamp in failed[:10]:  # Show first 10
                print(f"  {ct_code}: {error[:50]}... [{timestamp[:19]}]")
            if len(failed) > 10:
                print(f"  ... and {len(failed) - 10} more")
        else:
            print("✅ No failed codes found!")
            
    elif command == "reset":
        reset_progress(backup=True)
        
    elif command == "reset-hard":
        reset_progress(backup=False)
        
    elif command == "merge":
        if len(sys.argv) < 3:
            print("Usage: python scraper_utils.py merge file1.txt file2.txt ...")
        else:
            merge_results(*sys.argv[2:])
            
    elif command == "csv":
        extract_coordinates_to_csv()
        
    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    main()