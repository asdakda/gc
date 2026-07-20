import sys
import os

print("Starting import tests...")
try:
    import telebot
    print("[OK] telebot (pyTelegramBotAPI) imported successfully.")
except ImportError:
    print("[ERROR] Failed to import telebot. Make sure to run 'pip install pyTelegramBotAPI'")
    sys.exit(1)

try:
    import psycopg2
    print("[OK] psycopg2 imported successfully.")
except ImportError:
    print("[WARN] psycopg2-binary not imported. Fallback database connections to SQLite only.")

try:
    import sqlite3
    print("[OK] sqlite3 imported successfully.")
except ImportError:
    print("[ERROR] Failed to import sqlite3.")
    sys.exit(1)

# Import DBManager from bot to test init
try:
    from bot import DBManager
    db = DBManager()
    print("[OK] DBManager initialized and table verified successfully.")
except Exception as e:
    print(f"[ERROR] Failed to initialize DBManager: {e}")
    sys.exit(1)

print("\nAll import and local database checks passed successfully!")

