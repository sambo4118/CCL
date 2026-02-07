"""
Email Setup for Library System - Gmail Configuration
Run this script to test and configure your email settings
"""

import smtplib
from email.mime.text import MIMEText
import os

def test_gmail_connection():
    print("🔧 Gmail Email Setup Test")
    print("=" * 40)
    
    # Get user input
    email = input("Enter your Gmail address: ").strip()
    
    print("\n📱 You need a Gmail App Password (not your regular password)")
    print("   1. Go to: https://myaccount.google.com/apppasswords")
    print("   2. Generate an app password for 'Library System'")
    print("   3. Use the 16-character password below\n")
    
    app_password = input("Enter your Gmail App Password (16 chars): ").strip()
    
    # Test connection
    try:
        print("\n🔗 Testing connection to Gmail...")
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(email, app_password)
        
        # Send test email
        msg = MIMEText("✅ Your library system email is working!")
        msg['Subject'] = "Library System - Email Test Success"
        msg['From'] = email  
        msg['To'] = email
        
        server.send_message(msg)
        server.quit()
        
        print("✅ SUCCESS! Email configuration works!")
        print("\n🎯 Set these environment variables:")
        print(f'$env:SMTP_SERVER = "smtp.gmail.com"')
        print(f'$env:SMTP_USERNAME = "{email}"') 
        print(f'$env:SMTP_PASSWORD = "{app_password}"')
        print(f'$env:FROM_EMAIL = "{email}"')
        print(f'$env:BASE_URL = "http://localhost:5000"')
        
        # Auto-set for current session
        os.environ['SMTP_SERVER'] = 'smtp.gmail.com'
        os.environ['SMTP_USERNAME'] = email
        os.environ['SMTP_PASSWORD'] = app_password  
        os.environ['FROM_EMAIL'] = email
        os.environ['BASE_URL'] = 'http://localhost:5000'
        
        print("\n🚀 Environment variables set for this session!")
        print("   Restart your Flask app to use email")
        
    except smtplib.SMTPAuthenticationError:
        print("❌ Authentication failed!")
        print("   • Make sure you're using an App Password, not regular password")
        print("   • Enable 2-Factor Authentication first")
        print("   • Generate app password at: https://myaccount.google.com/apppasswords")
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("   • Check your internet connection")
        print("   • Verify Gmail settings")

if __name__ == "__main__":
    test_gmail_connection()