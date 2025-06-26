#!/usr/bin/env python3
"""
Helper script to set up the SAIL logo as the page icon.

Instructions:
1. Save your SAIL logo image as 'sail_logo.png' in the static/images/ directory
2. The image should ideally be square (e.g., 256x256 pixels) for best results
3. Supported formats: PNG, ICO, or other common image formats
4. Run this script to verify the setup

The logo will be used as:
- Browser tab icon (favicon)
- Page icon in Streamlit
"""

import os
from pathlib import Path


def setup_logo():
    """Check and setup the SAIL logo"""
    project_root = Path(__file__).parent
    static_dir = project_root / "static" / "images"
    logo_path = static_dir / "sail_logo.png"

    print("🚢 SAIL Logo Setup")
    print("=" * 50)

    # Check if static directory exists
    if static_dir.exists():
        print(f"✅ Static directory exists: {static_dir}")
    else:
        print(f"❌ Static directory missing: {static_dir}")
        return

    # Check if logo exists
    if logo_path.exists():
        print(f"✅ SAIL logo found: {logo_path}")

        # Check file size
        file_size = os.path.getsize(logo_path)
        print(f"📏 File size: {file_size:,} bytes")

        if file_size > 1024 * 1024:  # 1MB
            print("⚠️  Warning: Large file size. Consider optimizing for web use.")

        print("\n🎉 Setup complete! Your SAIL logo is ready to use.")
        print("   Restart your Streamlit app to see the new icon.")

    else:
        print(f"❌ SAIL logo not found: {logo_path}")
        print("\n📋 To fix this:")
        print("1. Save your SAIL logo image as 'sail_logo.png'")
        print(f"2. Place it in: {static_dir}")
        print("3. Ensure it's a square image (256x256 recommended)")
        print("4. Run this script again to verify")


if __name__ == "__main__":
    setup_logo()
