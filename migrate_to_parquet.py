#!/usr/bin/env python3
"""
Migration script to convert existing CSV and Excel files to Parquet format.
This script helps transition the existing data storage to the more efficient Parquet format.

Usage:
    python migrate_to_parquet.py [--backup] [--directory DIR] [--dry-run]
"""
import argparse
import logging
from pathlib import Path
import sys
import os

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from utils.format_converter import (
    migrate_workspace_to_parquet,
    get_conversion_summary,
    batch_convert_directory,
)
from config import (
    INPUT_DATA_PATH,
    OUTPUT_PATH,
    INTERMEDIATE_OUTPUT_PATH,
    MISC_OUTPUT_PATH,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Migrate CSV and Excel files to Parquet format"
    )
    parser.add_argument(
        "--backup", action="store_true", help="Backup original files before conversion"
    )
    parser.add_argument(
        "--directory",
        type=str,
        help="Specific directory to convert (default: all workspace directories)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be converted without actually converting",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Only show conversion summary without converting",
    )

    args = parser.parse_args()

    logger.info("🚀 Starting Parquet migration utility")

    if args.directory:
        # Convert specific directory
        directories = [args.directory]
    else:
        # Convert all workspace directories
        directories = [
            INPUT_DATA_PATH,
            OUTPUT_PATH,
            INTERMEDIATE_OUTPUT_PATH,
            MISC_OUTPUT_PATH,
        ]

    # Show summary for each directory
    total_convertible = 0
    for directory in directories:
        logger.info(f"\n📊 Analysis for directory: {directory}")
        summary = get_conversion_summary(directory)

        if "error" in summary:
            logger.error(f"❌ Error analyzing {directory}: {summary['error']}")
            continue

        logger.info(f"   📁 Excel files: {summary['excel_files']}")
        logger.info(f"   📄 CSV files: {summary['csv_files']}")
        logger.info(f"   ⚡ Parquet files: {summary['parquet_files']}")
        logger.info(f"   🔄 Total convertible: {summary['total_convertible']}")

        total_convertible += summary["total_convertible"]

        if summary["excel_paths"]:
            logger.info("   Excel files to convert:")
            for path in summary["excel_paths"][:5]:  # Show first 5
                logger.info(f"     - {path}")
            if len(summary["excel_paths"]) > 5:
                logger.info(f"     ... and {len(summary['excel_paths']) - 5} more")

        if summary["csv_paths"]:
            logger.info("   CSV files to convert:")
            for path in summary["csv_paths"][:5]:  # Show first 5
                logger.info(f"     - {path}")
            if len(summary["csv_paths"]) > 5:
                logger.info(f"     ... and {len(summary['csv_paths']) - 5} more")

    logger.info(f"\n📈 Total files that can be converted: {total_convertible}")

    if args.summary_only:
        logger.info("✅ Summary completed (no conversion performed)")
        return

    if args.dry_run:
        logger.info("🔍 DRY RUN - No files will be converted")
        return

    if total_convertible == 0:
        logger.info(
            "✅ No files need conversion - all data is already in optimal format!"
        )
        return

    # Confirm conversion
    if not args.directory:
        response = input(
            f"\n🤔 Convert {total_convertible} files to Parquet format? (y/N): "
        )
        if response.lower() != "y":
            logger.info("❌ Conversion cancelled by user")
            return

    # Perform conversion
    if args.directory:
        # Convert specific directory
        logger.info(f"🔄 Converting files in {args.directory}")
        try:
            excel_files = batch_convert_directory(args.directory, "*.xlsx")
            csv_files = batch_convert_directory(args.directory, "*.csv")

            total_converted = len(excel_files) + len(csv_files)
            logger.info(f"✅ Converted {total_converted} files in {args.directory}")

        except Exception as e:
            logger.error(f"❌ Error converting {args.directory}: {e}")
    else:
        # Convert entire workspace
        results = migrate_workspace_to_parquet(backup_originals=args.backup)

        total_converted = sum(len(results[key]) for key in results if key != "errors")
        logger.info(f"✅ Migration completed!")
        logger.info(f"   📈 Total files converted: {total_converted}")
        logger.info(f"   📁 Input files: {len(results['input_files'])}")
        logger.info(f"   📄 Output files: {len(results['output_files'])}")
        logger.info(f"   🔄 Intermediate files: {len(results['intermediate_files'])}")
        logger.info(f"   📋 Misc files: {len(results['misc_files'])}")

        if results["errors"]:
            logger.warning(f"   ⚠️  Errors: {len(results['errors'])}")
            for error in results["errors"]:
                logger.error(f"     - {error}")

    logger.info("\n🎉 Migration to Parquet format completed!")
    logger.info("💡 Benefits of Parquet format:")
    logger.info("   • 🚀 Faster read/write performance")
    logger.info("   • 📦 Better compression (smaller file sizes)")
    logger.info("   • 🔍 Column-based storage for analytics")
    logger.info("   • 🛡️  Built-in schema validation")


if __name__ == "__main__":
    main()
