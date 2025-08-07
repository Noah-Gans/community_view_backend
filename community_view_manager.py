#!/usr/bin/env python3
"""
🏗️ COMMUNITY VIEW BACKEND MANAGER 🏗️

This is the MASTER CONTROL script for your entire backend system.
Think of it as the "conductor" of an orchestra - it coordinates all the different pieces.

🎯 WHAT IT MANAGES:
├── 🔍 Search API (FastAPI on port 8000)
├── 🗺️  Tegola Server (Manual startup - optional) 
├── 📊 PostgreSQL Database (with PostGIS)
├── ☁️  Google Cloud Storage uploads
├── 🔄 Daily data updates (download → process → upload → database → search index)
├── 🏥 Health monitoring (every 15 minutes)
└── 📧 Email notifications (daily summaries + alerts)

🎛️ CONTROL MODES:
- start    → Start all services, then exit
- stop     → Stop all services gracefully
- status   → Check if services are running
- update   → Run data pipeline once (manual trigger)
- daemon   → Run forever with 2 AM scheduling + health checks
- health   → Run health checks once

🔄 DAILY WORKFLOW (2 AM automatic):
1. Download county data (Fremont, Teton ID/WY, Lincoln, Sublette)
2. Process & standardize GeoJSON files
3. Upload to Google Cloud Storage  
4. Migrate to PostgreSQL database
5. Rebuild search index (93k+ properties)
6. Hot-reload search API
7. Email summary to noahgans@tetoncountygis.com
"""

# ═══════════════════════════════════════════════════════════════
# 📦 IMPORTS & SETUP
# ═══════════════════════════════════════════════════════════════

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

# Try to import optional dependencies with helpful error messages
try:
    import schedule      # For 2 AM scheduling
    import requests     # For health checks (HTTP requests)
    import psutil       # For system process monitoring
except ImportError as e:
    print(f"❌ Missing dependency: {e}")
    print("💡 Fix with: pip install schedule requests psutil")
    sys.exit(1)

# ═══════════════════════════════════════════════════════════════
# 🏗️ MAIN MANAGER CLASS
# ═══════════════════════════════════════════════════════════════

class CommunityViewManager:
    """
    🎛️ The main controller class that manages everything
    
    INITIALIZATION FLOW:
    1. Load config.json settings
    2. Set up project paths
    3. Configure logging system
    """
    
    def __init__(self, config_path: str = "config.json"):
        """
        🚀 Initialize the manager
        
        Args:
            config_path: Path to config.json file with all settings
        """
        self.config_path = config_path
        self.config = self._load_config()          # Load settings from config.json
        self.project_root = Path(__file__).parent  # Get parent directory path
        self._setup_logging()                      # Set up logging system
        
    def _load_config(self) -> Dict:
        """
        📖 Load configuration from config.json
        
        CONTAINS:
        - Email notification settings
        - Database connection info  
        - List of counties to process
        - Service ports and paths
        - Scheduling settings (2 AM time)
        
        Returns:
            Dict: Configuration dictionary
        """
        try:
            with open(self.config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"❌ Config file {self.config_path} not found")
            print("💡 Make sure config.json exists in the project root")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON in config file: {e}")
            sys.exit(1)
    
    def _setup_logging(self):
        """
        📝 Configure the logging system
        
        CREATES:
        - Daily log files: logs/community_view_manager_YYYYMMDD.log
        - Console output with timestamps
        - INFO level logging (shows operations, not debug spam)
        
        LOG STRUCTURE:
        2025-08-06 14:30:15,123 - __main__ - INFO - 🔍 Starting search...
        """
        log_dir = Path(self.config["paths"]["log_dir"])
        log_dir.mkdir(exist_ok=True)  # Create logs/ directory if it doesn't exist
        
        # Create daily log file with date stamp
        log_file = log_dir / f"community_view_manager_{datetime.now().strftime('%Y%m%d')}.log"
        
        logging.basicConfig(
            level=getattr(logging, self.config["general"]["log_level"]),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),      # Save to file
                logging.StreamHandler(sys.stdout)   # Show on console
            ]
        )
        self.logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
# 🚀 SERVICE MANAGEMENT (Start/Stop APIs)
# ═══════════════════════════════════════════════════════════════

    def start_services(self) -> bool:
        """
        🚀 Start all backend services
        
        WHAT IT DOES:
        1. Runs ./scripts/start_services.sh
        2. That script starts:
           - Search API (Python FastAPI on port 8000)
           - Tegola Server (skipped - start manually if needed)
        3. Saves process IDs to logs/search_api.pid
        4. Verifies services are responding with HTTP health checks
        
        Returns:
            bool: True if all services started successfully
        """
        self.logger.info("🚀 Starting Community View Backend Services")
        
        try:
            # Run the bash script with debug output
            script_path = "./scripts/start_services.sh"
            
            self.logger.info(f"📁 Running script: {script_path}")
            self.logger.info(f"📁 Working directory: {self.project_root}")
            
            # Use Popen for better control over the subprocess
            import subprocess
            process = subprocess.Popen(
                ["bash", "-x", script_path],
                cwd=self.project_root,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # Merge stderr into stdout
                text=True,
                bufsize=1,  # Line buffered
                universal_newlines=True
            )
            
            # Stream output in real-time with timeout
            import time
            start_time = time.time()
            timeout = 120
            output_lines = []
            
            while True:
                # Check if process has finished
                if process.poll() is not None:
                    # Process finished, read any remaining output
                    remaining_output = process.stdout.read()
                    if remaining_output:
                        output_lines.append(remaining_output)
                        self.logger.info(f"📄 Script output: {remaining_output.strip()}")
                    break
                
                # Check for timeout
                if time.time() - start_time > timeout:
                    self.logger.error(f"❌ Script timeout after {timeout} seconds, terminating...")
                    process.terminate()
                    time.sleep(2)
                    if process.poll() is None:
                        process.kill()
                    return False
                
                # Read available output (non-blocking)
                try:
                    output = process.stdout.readline()
                    if output:
                        output_lines.append(output)
                        self.logger.info(f"📄 Script output: {output.strip()}")
                except:
                    pass
                
                time.sleep(0.1)  # Small delay to prevent busy waiting
            
            exit_code = process.returncode
            all_output = ''.join(output_lines)
            
            self.logger.info(f"🔧 Script exit code: {exit_code}")
            
            if exit_code == 0:
                self.logger.info("✅ Services started successfully")
                return True
            else:
                self.logger.error(f"❌ Failed to start services (exit code: {exit_code})")
                # Check for specific port conflict errors
                if "address already in use" in all_output:
                    self.logger.warning("⚠️  Port conflicts detected - services may already be running")
                    # Check if services are actually running despite the error
                    status = self.check_service_status()
                    if status.get('search_api_running') and status.get('tegola_running'):
                        self.logger.info("✅ Services are actually running despite startup errors")
                        return True
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error("❌ Service startup timed out after 120 seconds")
            return False
        except Exception as e:
            self.logger.error(f"❌ Error starting services: {e}")
            return False
    
    def stop_services(self) -> bool:
        """
        🛑 Stop all backend services gracefully
        
        WHAT IT DOES:
        1. Runs ./scripts/stop_services.sh
        2. That script:
           - Reads PID files (logs/search_api.pid, logs/tegola.pid)
           - Sends SIGTERM to processes (graceful shutdown)
           - If they don't stop, sends SIGKILL (force kill)
           - Cleans up PID files
        
        Returns:
            bool: True if services stopped successfully
        """
        self.logger.info("🛑 Stopping Community View Backend Services")
        
        try:
            result = subprocess.run(
                ["./scripts/stop_services.sh"],
                cwd=self.project_root,
                capture_output=True,
                text=True,
                timeout=30  # Stopping should be faster than starting
            )
            
            if result.returncode == 0:
                self.logger.info("✅ Services stopped successfully")
                return True
            else:
                self.logger.error(f"❌ Failed to stop services: {result.stderr}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Error stopping services: {e}")
            return False

# ═══════════════════════════════════════════════════════════════
# 🏥 HEALTH MONITORING (Check if services are alive)
# ═══════════════════════════════════════════════════════════════

    def check_service_status(self) -> Dict[str, bool]:
        """
        🏥 Check if all services are running and healthy
        
        CHECKS:
        - Search API: HTTP GET to localhost:8000/health
                   - Tegola: HTTP GET to localhost:8081/maps  
        - Database: PostgreSQL connection test (if enabled in config)
        
        Returns:
            Dict[str, bool]: {"search_api": True, "tegola": False, "database": True}
        """
        status = {}
        
        # 🔍 CHECK SEARCH API
        try:
            response = requests.get(
                self.config["health_checks"]["search_api_endpoint"],
                timeout=5  # Give up after 5 seconds
            )
            status["search_api"] = response.status_code == 200
        except:
            status["search_api"] = False
        
        # 🗺️ CHECK TEGOLA SERVER (optional - only if manually started)
        try:
            response = requests.get(
                self.config["health_checks"]["tegola_endpoint"],
                timeout=5
            )
            status["tegola"] = response.status_code == 200
        except:
            # Tegola is optional - not considered an error if not running
            status["tegola"] = False
        
        # 🗄️ CHECK DATABASE (if enabled)
        if self.config["health_checks"]["database_check"]:
            status["database"] = self._check_database()
        
        return status
    
    def _check_database(self) -> bool:
        """
        🗄️ Test PostgreSQL database connectivity
        
        WHAT IT DOES:
        1. Attempts to connect to PostgreSQL using config.json settings
        2. Immediately closes connection (just a ping test)
        3. Returns True if connection succeeded
        
        Returns:
            bool: True if database is reachable
        """
        try:
            import psycopg2
            db_config = self.config["database"]
            self.logger.info(f"Attempting database connection to {db_config['host']}:{db_config['port']} as {db_config['user']}")
            conn = psycopg2.connect(
                host=db_config["host"],
                port=db_config["port"],
                database=db_config["database"],
                user=db_config["user"],
                password=db_config.get("password", ""),  # Add password support
                connect_timeout=5  # Quick timeout
            )
            conn.close()
            self.logger.info("Database connection successful!")
            return True
        except Exception as e:
            self.logger.error(f"Database connection failed: {str(e)}")
            return False

# ═══════════════════════════════════════════════════════════════
# �� DATA PROCESSING PIPELINE (The Big Daily Update)
# ═══════════════════════════════════════════════════════════════

    def run_data_update_cycle(self) -> Dict[str, any]:
        """
        🔄 Run the complete data update cycle
        
        THIS IS THE BIG ONE! This is what runs at 2 AM every day.
        
        FULL PIPELINE:
        1. 📥 Download county data for all 5 counties
        2. 🔧 Process & standardize GeoJSON files  
        3. ☁️  Upload to Google Cloud Storage
        4. 🗄️ Migrate to PostgreSQL database with PostGIS
        5. 🔍 Rebuild search index (93k+ properties)
        6. 🔄 Hot-reload search API
        7. 📧 Send email notification with results
        
        COUNTIES PROCESSED:
        - fremont_county_wy
        - teton_county_id  
        - lincoln_county_wy
        - sublette_county_wy
        - teton_county_wy
        
        Returns:
            Dict: Results summary with timing, success/failure counts, errors
        """
        self.logger.info("🔄 Starting data update cycle")
        start_time = datetime.now()
        
        # Initialize results tracking
        results = {
            "start_time": start_time.isoformat(),
            "counties_processed": [],
            "counties_failed": [],
            "search_index_updated": False,
            "gcs_uploads": [],
            "database_migrations": [],
            "errors": []
        }
        
        try:
            # 🐍 ACTIVATE VIRTUAL ENVIRONMENT
            venv_activate = self.project_root / self.config["paths"]["venv_path"] / "bin" / "activate"
            
            # 🏁 PROCESS ALL COUNTIES WITH POSTGIS MIGRATION
            self.logger.info("🏁 Starting ownership pipeline for all counties")
            
            try:
                # 🚀 BUILD COMMAND
                # This runs: python tile_processing/ownership_pipeline.py --all --migrate-to-postgis
                cmd = [
                    "bash", "-c", 
                    f"source {venv_activate} && python tile_processing/ownership_pipeline.py --all --migrate-to-postgis"
                ]
                
                self.logger.info("🔄 Running: tile_processing/ownership_pipeline.py --all --migrate-to-postgis")
                
                # 📺 RUN WITH REAL-TIME OUTPUT STREAMING
                # This lets you see what tile_processing is doing as it happens
                process = subprocess.Popen(
                    cmd,
                    cwd=self.project_root,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,  # Combine stderr into stdout
                    text=True,
                    bufsize=1,                 # Line buffering
                    universal_newlines=True
                )
                
                # 📖 STREAM OUTPUT IN REAL-TIME
                while True:
                    output = process.stdout.readline()
                    if output == '' and process.poll() is not None:
                        break  # Process finished
                    if output:
                        # Show output both in logs and console
                        output_clean = output.strip()
                        self.logger.info(f"📦 {output_clean}")
                        print(f"📦 {output_clean}")  # Real-time console output
                
                # ✅ CHECK RESULTS
                return_code = process.poll()
                
                if return_code == 0:
                    # Success! Mark all counties as processed
                    results["counties_processed"] = self.config["counties"]
                    results["database_migrations"] = self.config["counties"]
                    self.logger.info("✅ Successfully processed all counties with PostGIS migration")
                else:
                    # Failed! Mark all counties as failed
                    results["counties_failed"] = self.config["counties"]
                    error_msg = "Failed to process counties with ownership pipeline"
                    results["errors"].append(error_msg)
                    self.logger.error(error_msg)
                    
            except Exception as e:
                error_msg = f"Error running ownership pipeline: {str(e)}"
                results["counties_failed"] = self.config["counties"]
                results["errors"].append(error_msg)
                self.logger.error(error_msg)
            
            # 🔍 REBUILD SEARCH INDEX
            self.logger.info("🔍 Rebuilding search index")
            try:
                cmd = [
                    "bash", "-c",
                    f"source {venv_activate} && python search_api/search_file_generator.py"
                ]
                
                result = subprocess.run(
                    cmd,
                    cwd=self.project_root,
                    capture_output=True,
                    text=True,
                    timeout=600  # 10 minutes should be enough
                )
                
                if result.returncode == 0:
                    results["search_index_updated"] = True
                    self.logger.info("✅ Search index rebuilt successfully")
                    
                    # 🔄 HOT-RELOAD SEARCH API
                    try:
                        response = requests.post("http://localhost:8000/internal/reload-search-index")
                        if response.status_code == 200:
                            self.logger.info("✅ Search API reloaded successfully")
                        else:
                            self.logger.warning("⚠️ Search API reload returned non-200 status")
                    except Exception as e:
                        self.logger.warning(f"⚠️ Could not reload search API: {e}")
                else:
                    error_msg = f"Failed to rebuild search index: {result.stderr}"
                    results["errors"].append(error_msg)
                    self.logger.error(error_msg)
                    
            except Exception as e:
                error_msg = f"Error rebuilding search index: {str(e)}"
                results["errors"].append(error_msg)
                self.logger.error(error_msg)
        
        except Exception as e:
            error_msg = f"Critical error in data update cycle: {str(e)}"
            results["errors"].append(error_msg)
            self.logger.error(error_msg)
        
        # 📊 CALCULATE FINAL TIMING
        end_time = datetime.now()
        results["end_time"] = end_time.isoformat()
        results["duration_minutes"] = (end_time - start_time).total_seconds() / 60
        
        self.logger.info(f"🎉 Data update cycle completed in {results['duration_minutes']:.1f} minutes")
        return results

# ═══════════════════════════════════════════════════════════════
# 📧 EMAIL NOTIFICATIONS (Keep you informed)
# ═══════════════════════════════════════════════════════════════

    def send_notification_email(self, subject: str, content: str, is_error: bool = False):
        """
        📧 Send email notification
        
        WHEN EMAILS ARE SENT:
        - Daily data update summaries (always)
        - Health check alerts (only when problems detected)
        
        EMAIL CONTENT INCLUDES:
        - Counties processed successfully
        - Counties that failed  
        - Processing duration
        - Error details
        - Service status
        
        Args:
            subject: Email subject line
            content: Email body text  
            is_error: True for alerts, False for normal summaries
        """
        try:
            email = self.config["general"]["notification_email"]
            if not email:
                self.logger.warning("No notification email configured")
                return
            
            # Import email modules only when needed (avoids startup issues)
            import smtplib
            from email.mime.text import MimeText
            from email.mime.multipart import MimeMultipart
            
            # 📝 CREATE EMAIL CONTENT
            msg = MimeMultipart()
            msg['From'] = "Community View Backend <noreply@tetoncountygis.com>"
            msg['To'] = email
            msg['Subject'] = f"{'🚨 ' if is_error else '📊 '}Community View Backend - {subject}"
            
            msg.attach(MimeText(content, 'plain'))
            
            # 📤 SEND EMAIL
            # Gmail SMTP settings for tetoncountygis.com
            smtp_server = "smtp.gmail.com"
            smtp_port = 587
            sender_email = "noahgans@tetoncountygis.com"  # Your Gmail address
            sender_password = "your-app-password"  # You'll need to set this up
            
            # Create SMTP connection
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls()
            server.login(sender_email, sender_password)
            
            # Send email
            server.send_message(msg)
            server.quit()
            
            self.logger.info(f"📧 Email sent successfully: {subject}")
            
        except Exception as e:
            self.logger.error(f"Failed to send email notification: {e}")

# ═══════════════════════════════════════════════════════════════
# 🤖 DAEMON MODE (24/7 Operation with Scheduling)
# ═══════════════════════════════════════════════════════════════

    def run_daemon(self):
        """
        🤖 Run as daemon with scheduled tasks
        
        THIS IS PRODUCTION MODE! Runs forever and handles:
        
        📅 SCHEDULED TASKS:
        - Daily data updates at 2:00 AM
        - Health checks every 15 minutes
        
        🔄 LIFECYCLE:
        1. Start all services (Search API + Tegola)
        2. Schedule recurring tasks
        3. Run initial health check
        4. Enter infinite loop checking for scheduled tasks
        5. Run until manually stopped (Ctrl+C)
        
        PERFECT FOR:
        - Production VM deployment
        - 24/7 automated operation
        - Systemd service integration
        """
        self.logger.info("🤖 Starting Community View Backend Daemon")
        
        # Start services first
        if not self.start_services():
            self.logger.error("❌ Failed to start services")
            return
            
        # Schedule tasks
        update_time = self.config["scheduling"]["data_update_time"]
        schedule.every().day.at(update_time).do(self._scheduled_data_update)
        schedule.every(15).minutes.do(self._scheduled_health_check)
        
        self.logger.info(f"📅 Scheduled daily data update at {update_time}")
        self.logger.info("📅 Scheduled health checks every 15 minutes")
        
        # Run initial health check
        self._scheduled_health_check()
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)
        except KeyboardInterrupt:
            self.logger.info("🛑 Daemon stopped by user")
        except Exception as e:
            self.logger.error(f"❌ Daemon error: {e}")
    
    def _scheduled_data_update(self):
        """🔄 Scheduled data update - called daily at 2 AM"""
        self.logger.info("🔄 Running scheduled data update...")
        try:
            result = self.run_data_update_cycle()
            self.logger.info(f"✅ Scheduled update completed: {result}")
        except Exception as e:
            self.logger.error(f"❌ Scheduled update failed: {e}")
            self.send_notification_email(
                "Community View - Scheduled Update Failed",
                f"Error during scheduled data update: {e}",
                is_error=True
            )
    
    def _scheduled_health_check(self):
        """🏥 Scheduled health check - called every 15 minutes"""
        self.logger.info("🏥 Running scheduled health check...")
        try:
            # Use the existing health check method
            status = self.check_service_status()
            self.logger.info(f"✅ Health check completed: {status}")
        except Exception as e:
            self.logger.error(f"❌ Health check failed: {e}")

# ═══════════════════════════════════════════════════════════════
# 🎯 COMMAND LINE INTERFACE (How you control everything)
# ═══════════════════════════════════════════════════════════════

def main():
    """
    🎯 Main entry point - handles command line arguments
    
    AVAILABLE COMMANDS:
    
    🚀 python community_view_manager.py start
       - Start Search API + Tegola
       - Exit immediately (services keep running)
       - Use for: Manual startup, development
    
    🛑 python community_view_manager.py stop  
       - Stop all services gracefully
       - Clean up PID files
       - Use for: Manual shutdown
    
    📊 python community_view_manager.py status
       - Check if services are running
       - Returns exit code 0 if healthy, 1 if problems
       - Use for: Monitoring scripts, health checks
    
    🔄 python community_view_manager.py update
       - Run full data pipeline once
       - Process all counties, rebuild search index
       - Send email notification
       - Use for: Manual data updates, testing
    
    🤖 python community_view_manager.py daemon
       - Start services + run forever with scheduling
       - Daily 2 AM updates + 15-minute health checks
       - Email notifications for everything
       - Use for: Production deployment
    
    🏥 python community_view_manager.py health
       - Run comprehensive health checks once
       - Check Search API, Tegola, Database
       - Use for: Troubleshooting, monitoring
    """
    parser = argparse.ArgumentParser(description="Community View Backend Manager")
    parser.add_argument("command", choices=["start", "stop", "status", "update", "daemon", "health"],
                      help="Command to execute")
    parser.add_argument("--config", default="config.json", help="Config file path")
    
    args = parser.parse_args()
    
    # 🏗️ CREATE MANAGER INSTANCE
    manager = CommunityViewManager(args.config)
    
    # 🎛️ EXECUTE COMMAND
    if args.command == "start":
        success = manager.start_services()
        sys.exit(0 if success else 1)
        
    elif args.command == "stop":
        success = manager.stop_services()
        sys.exit(0 if success else 1)
        
    elif args.command == "status":
        status = manager.check_service_status()
        print("🏥 Service Status:")
        for service, is_healthy in status.items():
            status_icon = "✅" if is_healthy else "❌"
            print(f"  {status_icon} {service}: {'Running' if is_healthy else 'Not Running'}")
        
        all_healthy = all(status.values())
        print(f"\n{'✅ All services healthy' if all_healthy else '⚠️ Some services have issues'}")
        sys.exit(0 if all_healthy else 1)
        
    elif args.command == "update":
        results = manager.run_data_update_cycle()
        print(f"✅ Data update completed in {results['duration_minutes']:.1f} minutes")
        if results['errors']:
            print(f"⚠️ {len(results['errors'])} errors occurred")
            for error in results['errors']:
                print(f"  - {error}")
        
    elif args.command == "health":
        health_report = manager.run_health_checks()
        if health_report["all_healthy"]:
            print("✅ All systems healthy")
        else:
            print("⚠️ Health issues detected:")
            for issue in health_report["issues"]:
                print(f"  - {issue}")
        sys.exit(0 if health_report["all_healthy"] else 1)
        
    elif args.command == "daemon":
        manager.run_daemon()

if __name__ == "__main__":
    main()