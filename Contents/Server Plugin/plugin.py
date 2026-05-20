# -*- coding: utf-8 -*-
"""
Renault Car Plugin for Indigo
Manages Renault Electric Vehicles via the Renault API
"""

import indigo
import asyncio
import aiohttp
import threading
import time
from datetime import datetime, timedelta
import json

# We'll need to install: pip install renault-api --break-system-packages
try:
    from renault_api.renault_client import RenaultClient
    from renault_api.exceptions import RenaultException, NotAuthenticatedException
    from renault_api.kamereon.exceptions import ForbiddenException
    from renault_api.gigya.exceptions import (
        GigyaResponseException,
        InvalidCredentialsException,
    )
except ImportError as e:
    import sys
    # Will be caught in startup
    RenaultClient = None
    RenaultException = None
    NotAuthenticatedException = None
    ForbiddenException = None
    GigyaResponseException = None
    InvalidCredentialsException = None


class Plugin(indigo.PluginBase):
    
    def __init__(self, pluginId, pluginDisplayName, pluginVersion, pluginPrefs):
        super(Plugin, self).__init__(pluginId, pluginDisplayName, pluginVersion, pluginPrefs)
        
        # Initialize state tracking
        self.accounts = {}  # accountDeviceId -> account info
        self.vehicles = {}  # vehicleDeviceId -> vehicle info
        self.update_threads = {}  # vehicleDeviceId -> update thread
        
        # Async event loop management
        self.loop = None
        self.loop_thread = None
        self.session = None
        
    ########################################
    # Plugin Lifecycle
    ########################################
    
    def startup(self):
        self.debugLog(u"Startup called")
        
        # Check if renault-api is available
        if RenaultClient is None:
            self.logger.error("=" * 60)
            self.logger.error("CRITICAL: renault-api module not found!")
            self.logger.error("This plugin requires the renault-api Python package.")
            self.logger.error("Indigo should have installed it automatically.")
            self.logger.error("Check Event Log for pip install messages.")
            self.logger.error("If not installed, check Contents/Packages/ folder.")
            self.logger.error("=" * 60)
            return
        
        self.logger.info("renault-api module loaded successfully")
        
        # Start async event loop in background thread
        self.loop = asyncio.new_event_loop()
        self.loop_thread = threading.Thread(target=self._run_event_loop, daemon=True)
        self.loop_thread.start()
        
    def shutdown(self):
        self.debugLog(u"Shutdown called")
        
        # Stop all update threads
        for thread in self.update_threads.values():
            if thread and thread.is_alive():
                # Threads are daemon threads so they'll stop automatically
                pass
        
        # Close async resources
        if self.loop:
            asyncio.run_coroutine_threadsafe(self._cleanup_async_resources(), self.loop)
            time.sleep(1)  # Give it time to clean up
            self.loop.call_soon_threadsafe(self.loop.stop)
    
    def _run_event_loop(self):
        """Run the async event loop in a background thread"""
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()
    
    async def _cleanup_async_resources(self):
        """Clean up async resources"""
        if self.session:
            await self.session.close()
    
    ########################################
    # Device Start/Stop
    ########################################
    
    def deviceStartComm(self, dev):
        self.debugLog(f"Starting device: {dev.name}")
        
        if dev.deviceTypeId == "renaultAccount":
            self._start_account_device(dev)
        elif dev.deviceTypeId == "renaultVehicle":
            self._start_vehicle_device(dev)
        elif dev.deviceTypeId in ["renaultChargeStart", "renaultChargeStop", 
                                   "renaultPreconditioning", "renaultChargeMode"]:
            # Control devices don't need startup
            dev.updateStateOnServer("onOffState", False)
    
    def deviceStopComm(self, dev):
        self.debugLog(f"Stopping device: {dev.name}")

        if dev.deviceTypeId == "renaultAccount":
            # Signal any running retry loop to exit
            if dev.id in self.accounts:
                self.accounts[dev.id]['stop_retries'] = True

        if dev.deviceTypeId == "renaultVehicle":
            # Stop update thread
            if dev.id in self.update_threads:
                # Thread will stop on next iteration
                if dev.id in self.vehicles:
                    self.vehicles[dev.id]['stop_updates'] = True
    
    def _start_account_device(self, dev):
        """Initialize a Renault account device.

        The first login attempt is made inline (so successful startups appear
        immediately as Connected). If it fails for a transient reason such as a
        Gigya server outage, a background retry loop takes over and keeps
        attempting with exponential backoff until the device is stopped or a
        terminal error (e.g. invalid credentials) is detected.
        """
        email = dev.pluginProps.get("email", "")
        password = dev.pluginProps.get("password", "")
        locale = dev.pluginProps.get("locale", "en_GB")

        if not email or not password:
            self.logger.error(f"{dev.name}: Email and password required")
            dev.updateStateOnServer("connectionStatus", "Error: Missing credentials")
            return

        # Track retry state per account device
        self.accounts.setdefault(dev.id, {})
        self.accounts[dev.id]['stop_retries'] = False

        # Try once inline so a healthy startup feels instant; non-blocking timeout.
        future = asyncio.run_coroutine_threadsafe(
            self._login_account(dev.id, email, password, locale),
            self.loop
        )

        try:
            result = future.result(timeout=15)
        except Exception as e:
            # Future itself blew up (timeout, cancelled, etc.) — treat as transient
            self.logger.warning(f"{dev.name}: Initial login attempt errored ({e}); will retry in background")
            result = {'status': 'transient', 'message': str(e), 'account_id': None}

        if result['status'] == 'success':
            self._mark_account_connected(dev, result['account_id'])
            return

        if result['status'] == 'terminal':
            # Don't bother retrying — credentials are wrong, account locked, etc.
            self.logger.error(f"{dev.name}: {result['message']}")
            dev.updateStateOnServer("connectionStatus", f"Login failed: {result['message']}")
            return

        # Transient — start background retry loop with exponential backoff
        self.logger.warning(
            f"{dev.name}: {result['message']} - will retry in background"
        )
        dev.updateStateOnServer("connectionStatus", "Login failed (retrying)")

        thread = threading.Thread(
            target=self._account_retry_loop,
            args=(dev.id, email, password, locale),
            daemon=True,
        )
        thread.start()

    def _mark_account_connected(self, dev, account_id):
        """Update device states to reflect a successful login."""
        dev.updateStateOnServer("accountId", account_id)
        dev.updateStateOnServer("connectionStatus", "Connected")
        dev.updateStateOnServer("lastLogin", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        self.logger.info(f"{dev.name}: Successfully connected (Account ID: {account_id})")

    def _account_retry_loop(self, account_dev_id, email, password, locale):
        """Background loop that retries login with exponential backoff.

        Backoff: 60s, 120s, 240s, 480s, 960s, 1800s (cap). Continues until the
        device is stopped, a terminal error is observed, or login succeeds.
        """
        backoff = 60        # seconds
        backoff_max = 1800  # 30 minutes
        attempt = 0
        consecutive_errors = 0

        while True:
            # Bail out if the device has been stopped or removed
            if account_dev_id not in self.accounts:
                return
            if self.accounts[account_dev_id].get('stop_retries'):
                return

            # Sleep first — we've already tried once inline before this loop started
            for _ in range(backoff):
                if account_dev_id not in self.accounts:
                    return
                if self.accounts[account_dev_id].get('stop_retries'):
                    return
                time.sleep(1)

            attempt += 1
            try:
                dev = indigo.devices[account_dev_id]
            except Exception:
                return  # Device deleted

            future = asyncio.run_coroutine_threadsafe(
                self._login_account(account_dev_id, email, password, locale),
                self.loop
            )

            try:
                result = future.result(timeout=30)
            except Exception as e:
                result = {'status': 'transient', 'message': str(e), 'account_id': None}

            if result['status'] == 'success':
                self._mark_account_connected(dev, result['account_id'])
                return

            if result['status'] == 'terminal':
                self.logger.error(f"{dev.name}: {result['message']} - giving up retries")
                dev.updateStateOnServer("connectionStatus", f"Login failed: {result['message']}")
                return

            # Transient - log, escalate to error after several consecutive failures
            consecutive_errors += 1
            if consecutive_errors == 1 or consecutive_errors % 5 == 0:
                self.logger.warning(
                    f"{dev.name}: {result['message']} "
                    f"(attempt {attempt}, retry in {backoff}s)"
                )
            else:
                self.debugLog(
                    f"{dev.name}: {result['message']} "
                    f"(attempt {attempt}, retry in {backoff}s)"
                )

            # Double backoff up to cap
            backoff = min(backoff * 2, backoff_max)

    def _classify_login_error(self, exc):
        """Classify a login exception. Returns (status, friendly_message).

        status is one of:
          'transient' - retry later (server errors, network glitches)
          'terminal'  - don't retry (bad creds, account locked, geographic block)
          'unknown'   - unfamiliar error; treat as transient but flag in logs
        """
        # Invalid credentials - terminal, don't retry
        if InvalidCredentialsException and isinstance(exc, InvalidCredentialsException):
            return ('terminal', "Invalid email or password")

        # Generic Gigya response error - classify by error code
        if GigyaResponseException and isinstance(exc, GigyaResponseException):
            code = exc.error_code
            details = exc.error_details or ''

            # 5xxxxx codes are Gigya server-side issues - transient
            # 500001 = General Server Error (we've seen this during Renault/Gigya outages)
            if code and 500000 <= code < 600000:
                return ('transient', f"Gigya server error {code} (General Server Error)")

            # 403xxx codes are auth/permission failures - terminal
            # 403042 = Invalid loginID/password (caught above if it raises Invalid...Exception)
            # 403041 = Account is disabled
            # 403100 = Account is pending registration
            if code and 403000 <= code < 404000:
                return ('terminal', f"Gigya auth error {code}: {details or 'authentication failed'}")

            # 400xxx - bad request, possibly stale API key - terminal-ish
            if code and 400000 <= code < 500000:
                return ('terminal', f"Gigya request error {code}: {details or 'check API key / locale'}")

            # Anything else - treat as transient with a warning
            return ('unknown', f"Gigya error {code}: {details or 'unknown'}")

        # Network / connection errors from aiohttp - transient
        exc_name = type(exc).__name__
        if exc_name in ('ClientConnectorError', 'ClientConnectionError',
                        'ServerDisconnectedError', 'TimeoutError',
                        'ClientOSError'):
            return ('transient', f"Network error: {exc_name}")

        if isinstance(exc, asyncio.TimeoutError):
            return ('transient', "Request timed out")

        # Unknown - treat as transient but log fully
        return ('unknown', f"{exc_name}: {exc}")

    async def _login_account(self, account_dev_id, email, password, locale):
        """Login to Renault account.

        Returns a dict:
          {'status': 'success'|'transient'|'terminal'|'unknown',
           'message': str,
           'account_id': str|None}
        """
        try:
            self.debugLog(f"Attempting login with locale: {locale}")

            if not self.session:
                self.session = aiohttp.ClientSession()

            client = RenaultClient(websession=self.session, locale=locale)

            await client.session.login(email, password)
            self.debugLog("Login successful, fetching person info...")

            person = await client.get_person()
            self.debugLog(f"Person info retrieved: {person}")

            if not person or not hasattr(person, 'accounts') or not person.accounts:
                return {
                    'status': 'terminal',
                    'message': 'No accounts found on this Renault profile',
                    'account_id': None,
                }

            account_id = person.accounts[0].accountId
            if not account_id:
                return {
                    'status': 'terminal',
                    'message': 'Person record returned without an account ID',
                    'account_id': None,
                }

            account = await client.get_api_account(account_id)
            # Preserve any retry-control flags already stored on this slot
            existing = self.accounts.get(account_dev_id, {})
            existing.update({
                'client': client,
                'account': account,
                'account_id': account_id,
                'email': email,
                'password': password,
                'locale': locale,
            })
            self.accounts[account_dev_id] = existing

            return {
                'status': 'success',
                'message': 'Connected',
                'account_id': account_id,
            }

        except Exception as e:
            status, message = self._classify_login_error(e)
            if status == 'unknown':
                # Unknown errors get the full traceback so we can improve the
                # classifier next time they show up
                import traceback
                self.logger.error(f"Unclassified login error: {message}")
                self.logger.error(f"Full traceback: {traceback.format_exc()}")
            else:
                self.debugLog(f"Login attempt failed ({status}): {message}")
            return {'status': status, 'message': message, 'account_id': None}
    
    def _start_vehicle_device(self, dev):
        """Initialize a vehicle device and start update thread"""
        account_dev_id = int(dev.pluginProps.get("accountDevice", 0))
        vin = dev.pluginProps.get("vin", "")
        
        if not account_dev_id or not vin:
            self.logger.error(f"{dev.name}: Account device and VIN required")
            return
        
        # Start a background thread to wait for account and then initialize
        def wait_and_start():
            # Wait indefinitely for the account device to be connected. Logs an
            # informational reminder every few minutes so the user can see why
            # the vehicle isn't initialized yet, without spamming the log.
            heartbeat_announced = False
            elapsed = 0
            heartbeat_interval = 120  # seconds

            while True:
                # Bail if the vehicle device itself has been stopped
                if dev.id in self.vehicles and self.vehicles[dev.id].get('stop_updates'):
                    return

                # An account is "ready" only when login has populated a client object,
                # not just when a stub exists from the retry tracker.
                account_info = self.accounts.get(account_dev_id)
                if account_info and account_info.get('client'):
                    break

                if not heartbeat_announced:
                    self.logger.info(
                        f"{dev.name}: Waiting for account device to connect..."
                    )
                    heartbeat_announced = True
                elif elapsed and elapsed % heartbeat_interval == 0:
                    self.debugLog(
                        f"{dev.name}: Still waiting for account device "
                        f"({elapsed}s elapsed)"
                    )

                time.sleep(1)
                elapsed += 1

            if heartbeat_announced:
                self.logger.info(f"{dev.name}: Account device connected, initializing vehicle")

            # Store vehicle info
            self.vehicles[dev.id] = {
                'account_dev_id': account_dev_id,
                'vin': vin,
                'stop_updates': False,
                'last_update': None,
                'last_charging_status': None,  # Track charging status changes
                'connection_errors': 0,  # Track consecutive connection errors
                'connection_lost': False  # Flag if connection lost message shown
            }
            
            # Start update thread
            update_interval = int(dev.pluginProps.get("updateInterval", 10)) * 60  # Convert to seconds
            thread = threading.Thread(
                target=self._vehicle_update_loop,
                args=(dev.id, update_interval),
                daemon=True
            )
            thread.start()
            self.update_threads[dev.id] = thread
            
            self.logger.info(f"{dev.name}: Started (Update interval: {update_interval/60} minutes)")
            
            # Trigger an immediate update now that we're ready
            try:
                future = asyncio.run_coroutine_threadsafe(
                    self._update_vehicle_data(dev.id),
                    self.loop
                )
                future.result(timeout=30)
                self.logger.info(f"{dev.name}: Initial data update completed")
            except Exception as e:
                self.logger.error(f"{dev.name}: Initial update error: {str(e)}")
        
        # Start the wait thread (non-blocking)
        threading.Thread(target=wait_and_start, daemon=True).start()
    
    def _vehicle_update_loop(self, vehicle_dev_id, interval):
        """Background thread to periodically update vehicle data"""
        while True:
            if vehicle_dev_id not in self.vehicles:
                break
            
            if self.vehicles[vehicle_dev_id].get('stop_updates', False):
                break
            
            # Update vehicle data
            try:
                future = asyncio.run_coroutine_threadsafe(
                    self._update_vehicle_data(vehicle_dev_id),
                    self.loop
                )
                future.result(timeout=30)
            except Exception as e:
                self.logger.error(f"Vehicle {vehicle_dev_id} update error: {str(e)}")
            
            # Sleep for interval
            time.sleep(interval)
    
    async def _update_vehicle_data(self, vehicle_dev_id):
        """Fetch and update all vehicle data"""
        try:
            dev = indigo.devices[vehicle_dev_id]
            vehicle_info = self.vehicles[vehicle_dev_id]
            account_dev_id = vehicle_info['account_dev_id']
            account_info = self.accounts[account_dev_id]

            account = account_info['account']
            vin = vehicle_info['vin']

            self.logger.debug(f"{dev.name}: Fetching data for VIN {vin}")

            # Get vehicle object - may itself raise NotAuthenticatedException
            try:
                vehicle = await account.get_api_vehicle(vin)
            except Exception as e:
                if self._is_auth_expired_error(e):
                    self.logger.info(
                        f"{dev.name}: Session expired, reauthenticating..."
                    )
                    if await self._reauthenticate_account(account_dev_id):
                        # Refresh local references and try once more
                        account_info = self.accounts[account_dev_id]
                        account = account_info['account']
                        vehicle = await account.get_api_vehicle(vin)
                        self.logger.debug(
                            f"{dev.name}: Session restored, resuming update"
                        )
                    else:
                        # Reauth failed - treat as a connection-level issue
                        self._handle_connection_error(dev, vehicle_dev_id, str(e))
                        return
                else:
                    raise
            
            # Fetch all available data
            updates = []
            
            # Battery status
            try:
                self.logger.debug(f"{dev.name}: Fetching battery status...")
                battery_status = await vehicle.get_battery_status()
                if battery_status:
                    self.logger.debug(f"{dev.name}: Battery status received")
                    
                    # Battery level - update sensorValue, batteryLevel, and batteryLevelInt
                    if hasattr(battery_status, 'batteryLevel') and battery_status.batteryLevel is not None:
                        battery_pct = float(battery_status.batteryLevel)
                        battery_pct_rounded = round(battery_pct, 1)
                        battery_pct_int = int(battery_status.batteryLevel)
                        
                        updates.append(("sensorValue", battery_pct_rounded, None, None))  # Main sensor display (1 decimal)
                        updates.append(("displayStateImageSel", f"{battery_pct_rounded}%", None, None))  # Formatted display
                        updates.append(("batteryLevel", battery_pct_int, None, None))  # Custom state
                        updates.append(("batteryLevelInt", battery_pct_int, None, None))  # Integer custom state
                        updates.append(("batteryLevelString", f"{battery_pct_int}%", None, None))
                    
                    # Battery range/autonomy
                    if hasattr(battery_status, 'batteryAutonomy') and battery_status.batteryAutonomy is not None:
                        distance_unit = dev.pluginProps.get("distanceUnit", "km")
                        range_value = int(round(self._convert_distance(battery_status.batteryAutonomy, distance_unit)))
                        unit_label = "miles" if distance_unit == "miles" else "km"
                        updates.append(("batteryAutonomy", range_value, None, None))
                        updates.append(("batteryAutonomyString", f"{range_value} {unit_label}", None, None))
                    
                    if hasattr(battery_status, 'batteryTemperature') and battery_status.batteryTemperature is not None:
                        updates.append(("batteryTemperature", float(self._convert_temperature(
                            battery_status.batteryTemperature, dev.pluginProps.get("temperatureUnit", "C")
                        )), None, None))
                    if hasattr(battery_status, 'batteryAvailableEnergy') and battery_status.batteryAvailableEnergy is not None:
                        updates.append(("batteryAvailableEnergy", float(battery_status.batteryAvailableEnergy), None, None))
                    
                    # Plug status - handle both string and numeric
                    if hasattr(battery_status, 'plugStatus') and battery_status.plugStatus is not None:
                        updates.append(("plugStatus", str(self._format_plug_status(battery_status.plugStatus)), None, None))
                    
                    # Charging status - handle both string and numeric
                    if hasattr(battery_status, 'chargingStatus') and battery_status.chargingStatus is not None:
                        # Store raw value from API
                        raw_status = str(battery_status.chargingStatus)
                        
                        # Check if charging status has changed and log it
                        last_status = self.vehicles[vehicle_dev_id].get('last_charging_status')
                        if last_status is not None and last_status != raw_status:
                            # Status changed - log the transition
                            last_cp = self._get_charging_status_cp(last_status) if last_status else "unknown"
                            new_cp = self._get_charging_status_cp(battery_status.chargingStatus)
                            last_display = self._format_charging_status(last_status) if last_status else "Unknown"
                            new_display = self._format_charging_status(battery_status.chargingStatus)
                            
                            # Log to Event Log
                            self.logger.info(f"{dev.name}: Charging status changed: {last_status} → {raw_status}")
                            self.logger.info(f"{dev.name}: Control Page: '{last_cp}' → '{new_cp}'")
                            self.logger.info(f"{dev.name}: Display: '{last_display}' → '{new_display}'")
                        
                        # Update last known status
                        self.vehicles[vehicle_dev_id]['last_charging_status'] = raw_status
                        
                        updates.append(("chargingStatusRAW", raw_status, None, None))
                        # Store control page value (consistent strings)
                        cp_status = self._get_charging_status_cp(battery_status.chargingStatus)
                        updates.append(("chargingStatusCP", cp_status, None, None))
                        # Store formatted value for display
                        updates.append(("chargingStatus", str(self._format_charging_status(battery_status.chargingStatus)), None, None))
                    
                    if hasattr(battery_status, 'chargingInstantaneousPower') and battery_status.chargingInstantaneousPower is not None:
                        updates.append(("chargingPower", float(battery_status.chargingInstantaneousPower), None, None))
                    if hasattr(battery_status, 'chargingRemainingTime') and battery_status.chargingRemainingTime is not None:
                        updates.append(("chargingRemainingTime", int(battery_status.chargingRemainingTime), None, None))
            except Exception as e:
                if self._is_auth_expired_error(e):
                    # First section failed with expired token - try to recover
                    # so the remaining sections in this cycle can still succeed
                    self.logger.info(
                        f"{dev.name}: Session expired during battery fetch, reauthenticating..."
                    )
                    if await self._reauthenticate_account(account_dev_id):
                        account_info = self.accounts[account_dev_id]
                        account = account_info['account']
                        try:
                            vehicle = await account.get_api_vehicle(vin)
                            # Retry battery once with the fresh session
                            battery_status = await vehicle.get_battery_status()
                            # If the retry worked, fold it into updates (minimal: just
                            # rerun this entire block by recursing into the same code is
                            # awkward, so just log success and let the next cycle pick
                            # it up cleanly)
                            self.logger.debug(
                                f"{dev.name}: Session restored, battery data will refresh next cycle"
                            )
                        except Exception as retry_exc:
                            self.logger.debug(
                                f"{dev.name}: Post-reauth battery retry failed: {retry_exc}"
                            )
                    else:
                        self._handle_connection_error(dev, vehicle_dev_id, str(e))
                elif self._is_connection_error(str(e)):
                    self._handle_connection_error(dev, vehicle_dev_id, str(e))
                else:
                    self.logger.error(f"{dev.name}: Battery status error: {str(e)}")
            
            # Charge mode - skip if not available for this model
            try:
                self.logger.debug(f"{dev.name}: Fetching charge mode...")
                charge_mode = await vehicle.get_charge_mode()
                if charge_mode:
                    self.logger.debug(f"{dev.name}: Charge mode received")
                    if hasattr(charge_mode, 'chargeMode'):
                        updates.append(("chargingMode", str(charge_mode.chargeMode), None, None))
            except Exception as e:
                # Some models don't support charge-mode endpoint
                if "not available" in str(e):
                    self.logger.debug(f"{dev.name}: Charge mode not supported by this vehicle model")
                elif self._is_auth_expired_error(e):
                    self.debugLog(f"{dev.name}: Charge mode skipped - session expired")
                elif self._is_connection_error(str(e)):
                    self._handle_connection_error(dev, vehicle_dev_id, str(e))
                else:
                    self.logger.error(f"{dev.name}: Charge mode error: {str(e)}")
            
            # Cockpit data (odometer, etc)
            try:
                self.logger.debug(f"{dev.name}: Fetching cockpit data...")
                cockpit = await vehicle.get_cockpit()
                if cockpit:
                    self.logger.debug(f"{dev.name}: Cockpit data received")
                    if hasattr(cockpit, 'totalMileage') and cockpit.totalMileage is not None:
                        updates.append(("odometer", float(self._convert_distance(
                            cockpit.totalMileage, dev.pluginProps.get("distanceUnit", "km")
                        )), None, None))
            except Exception as e:
                if self._is_auth_expired_error(e):
                    self.debugLog(f"{dev.name}: Cockpit skipped - session expired")
                elif self._is_connection_error(str(e)):
                    self._handle_connection_error(dev, vehicle_dev_id, str(e))
                else:
                    self.logger.error(f"{dev.name}: Cockpit error: {str(e)}")
            
            # Location
            try:
                self.logger.debug(f"{dev.name}: Fetching location...")
                location = await vehicle.get_location()
                if location:
                    self.logger.debug(f"{dev.name}: Location received")
                    if hasattr(location, 'gpsLatitude') and location.gpsLatitude is not None:
                        updates.append(("latitude", float(location.gpsLatitude), None, None))
                    if hasattr(location, 'gpsLongitude') and location.gpsLongitude is not None:
                        updates.append(("longitude", float(location.gpsLongitude), None, None))
            except Exception as e:
                if self._is_forbidden_error(e):
                    self.logger.debug(f"{dev.name}: Location not available (privacy mode or unsupported)")
                elif self._is_auth_expired_error(e):
                    self.debugLog(f"{dev.name}: Location skipped - session expired")
                elif self._is_connection_error(str(e)):
                    self._handle_connection_error(dev, vehicle_dev_id, str(e))
                else:
                    self.logger.error(f"{dev.name}: Location error: {str(e)}")
            
            # HVAC status
            try:
                self.logger.debug(f"{dev.name}: Fetching HVAC status...")
                hvac_status = await vehicle.get_hvac_status()
                if hvac_status:
                    self.logger.debug(f"{dev.name}: HVAC status received")
                    if hasattr(hvac_status, 'hvacStatus'):
                        status = "on" if hvac_status.hvacStatus == "on" else "off"
                        updates.append(("hvacStatus", str(status), None, None))
                    if hasattr(hvac_status, 'externalTemperature') and hvac_status.externalTemperature is not None:
                        updates.append(("externalTemperature", float(self._convert_temperature(
                            hvac_status.externalTemperature, dev.pluginProps.get("temperatureUnit", "C")
                        )), None, None))
                    if hasattr(hvac_status, 'socThreshold') and hvac_status.socThreshold is not None:
                        updates.append(("hvacSocThreshold", int(hvac_status.socThreshold), None, None))
            except Exception as e:
                if self._is_auth_expired_error(e):
                    self.debugLog(f"{dev.name}: HVAC skipped - session expired")
                elif self._is_connection_error(str(e)):
                    self._handle_connection_error(dev, vehicle_dev_id, str(e))
                else:
                    self.logger.error(f"{dev.name}: HVAC status error: {str(e)}")
            
            # Update all states - do one at a time to catch any problematic values
            updates.append(("zLastUpdate", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), None, None))
            
            if updates:
                for state_tuple in updates:
                    try:
                        dev.updateStateOnServer(state_tuple[0], state_tuple[1])
                    except Exception as e:
                        self.logger.error(f"{dev.name}: Failed to update state '{state_tuple[0]}' with value '{state_tuple[1]}': {str(e)}")
                
                self.logger.debug(f"{dev.name}: Updated {len(updates)} states")
                
                # If we got here with data updates, connection is good
                self._handle_successful_connection(dev, vehicle_dev_id)
            else:
                self.logger.warning(f"{dev.name}: No data updates available")
            
            # Update any linked preconditioning devices based on HVAC status
            hvac_state = None
            for state_tuple in updates:
                if state_tuple[0] == "hvacStatus":
                    hvac_state = state_tuple[1]
                    break
            
            if hvac_state is not None:
                # Find all preconditioning devices linked to this vehicle
                for precond_dev in indigo.devices.iter("self.renaultPreconditioning"):
                    linked_vehicle = int(precond_dev.pluginProps.get("vehicleDevice", 0))
                    if linked_vehicle == vehicle_dev_id:
                        # Update the preconditioning device state based on HVAC status
                        is_on = (hvac_state.lower() == "on")
                        precond_dev.updateStateOnServer("onOffState", is_on)
            
            self.vehicles[vehicle_dev_id]['last_update'] = datetime.now()
            
        except Exception as e:
            import traceback
            self.logger.error(f"Update vehicle data error: {str(e)}")
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
    
    ########################################
    # Relay Device Actions
    ########################################
    
    def actionControlDevice(self, action, dev):
        """Handle relay device on/off actions"""
        
        if action.deviceAction == indigo.kDeviceAction.TurnOn:
            if dev.deviceTypeId == "renaultChargeStart":
                self._charge_start(dev)
            elif dev.deviceTypeId == "renaultChargeStop":
                self._charge_stop(dev)
            elif dev.deviceTypeId == "renaultPreconditioning":
                self._start_preconditioning(dev)
            elif dev.deviceTypeId == "renaultChargeMode":
                self._set_charge_mode(dev, True)
        
        elif action.deviceAction == indigo.kDeviceAction.TurnOff:
            if dev.deviceTypeId == "renaultChargeMode":
                self._set_charge_mode(dev, False)
            # Other devices are momentary, so turning off does nothing
    
    def actionControlSensor(self, action, dev):
        """Handle sensor device actions including status requests"""
        if action.sensorAction == indigo.kSensorAction.RequestStatus:
            if dev.deviceTypeId == "renaultVehicle":
                self.logger.info(f"{dev.name}: Status request received")
                # Trigger immediate update
                if dev.id in self.vehicles:
                    future = asyncio.run_coroutine_threadsafe(
                        self._update_vehicle_data(dev.id),
                        self.loop
                    )
                    try:
                        future.result(timeout=30)
                        self.logger.info(f"{dev.name}: Status updated")
                    except Exception as e:
                        self.logger.error(f"{dev.name}: Error updating status: {str(e)}")
            elif dev.deviceTypeId == "renaultAccount":
                self.logger.info(f"{dev.name}: Status request received (no action needed)")
    
    def _charge_start(self, dev):
        """Start charging"""
        vehicle_dev_id = int(dev.pluginProps.get("vehicleDevice", 0))
        if not vehicle_dev_id:
            self.logger.error(f"{dev.name}: No vehicle device configured")
            return
        
        vehicle_info = self.vehicles.get(vehicle_dev_id)
        if not vehicle_info:
            self.logger.error(f"{dev.name}: Vehicle device not found")
            return
        
        # Turn on briefly
        dev.updateStateOnServer("onOffState", True)
        
        # Send command
        future = asyncio.run_coroutine_threadsafe(
            self._async_charge_start(vehicle_info),
            self.loop
        )
        
        try:
            future.result(timeout=10)
            self.logger.info(f"{dev.name}: Charge start command sent")
        except Exception as e:
            self.logger.error(f"{dev.name}: Error: {str(e)}")
        
        # Turn off (momentary)
        dev.updateStateOnServer("onOffState", False)
    
    async def _async_charge_start(self, vehicle_info):
        """Async charge start"""
        account_info = self.accounts[vehicle_info['account_dev_id']]
        account = account_info['account']
        vehicle = await account.get_api_vehicle(vehicle_info['vin'])
        await vehicle.set_charge_start()
    
    def _charge_stop(self, dev):
        """Stop charging"""
        vehicle_dev_id = int(dev.pluginProps.get("vehicleDevice", 0))
        if not vehicle_dev_id:
            self.logger.error(f"{dev.name}: No vehicle device configured")
            return
        
        vehicle_info = self.vehicles.get(vehicle_dev_id)
        if not vehicle_info:
            self.logger.error(f"{dev.name}: Vehicle device not found")
            return
        
        dev.updateStateOnServer("onOffState", True)
        
        future = asyncio.run_coroutine_threadsafe(
            self._async_charge_stop(vehicle_info),
            self.loop
        )
        
        try:
            future.result(timeout=10)
            self.logger.info(f"{dev.name}: Charge stop command sent")
        except Exception as e:
            self.logger.error(f"{dev.name}: Error: {str(e)}")
        
        dev.updateStateOnServer("onOffState", False)
    
    async def _async_charge_stop(self, vehicle_info):
        """Async charge stop"""
        account_info = self.accounts[vehicle_info['account_dev_id']]
        account = account_info['account']
        vehicle = await account.get_api_vehicle(vehicle_info['vin'])
        await vehicle.set_charge_stop()
    
    def _start_preconditioning(self, dev):
        """Start HVAC preconditioning"""
        vehicle_dev_id = int(dev.pluginProps.get("vehicleDevice", 0))
        if not vehicle_dev_id:
            self.logger.error(f"{dev.name}: No vehicle device configured")
            return
        
        vehicle_info = self.vehicles.get(vehicle_dev_id)
        if not vehicle_info:
            self.logger.error(f"{dev.name}: Vehicle device not found")
            return
        
        # Get the vehicle device to check battery level and SoC threshold
        try:
            vehicle_dev = indigo.devices[vehicle_dev_id]
        except:
            self.logger.error(f"{dev.name}: Could not access vehicle device")
            return
        
        # Check battery level against HVAC SoC threshold
        battery_level = vehicle_dev.states.get("batteryLevelInt", 0)
        hvac_threshold = vehicle_dev.states.get("hvacSocThreshold", 0)
        
        if hvac_threshold > 0 and battery_level < hvac_threshold:
            self.logger.warning(f"{dev.name}: Pre-conditioning not started - battery level ({battery_level}%) is below HVAC SoC threshold ({hvac_threshold}%)")
            return
        
        temperature = float(dev.pluginProps.get("targetTemperature", 21))
        
        # Immediately set HVAC status to "on" on the vehicle device
        vehicle_dev.updateStateOnServer("hvacStatus", "on")
        
        # Set this device to on
        dev.updateStateOnServer("onOffState", True)
        
        # Send command
        future = asyncio.run_coroutine_threadsafe(
            self._async_start_hvac(vehicle_info, temperature),
            self.loop
        )
        
        try:
            future.result(timeout=10)
            self.logger.info(f"{dev.name}: HVAC start command sent (target: {temperature}°C)")
            
            # Schedule a status update after 30 seconds to verify
            def delayed_update():
                time.sleep(30)
                self.logger.info(f"{dev.name}: Requesting vehicle status update in 30 seconds to verify HVAC state")
                update_future = asyncio.run_coroutine_threadsafe(
                    self._update_vehicle_data(vehicle_dev_id),
                    self.loop
                )
                try:
                    update_future.result(timeout=30)
                    self.logger.info(f"{dev.name}: Vehicle status updated")
                except Exception as e:
                    self.logger.error(f"{dev.name}: Error updating vehicle status: {str(e)}")
            
            # Start the delayed update in a background thread
            threading.Thread(target=delayed_update, daemon=True).start()
            
        except Exception as e:
            self.logger.error(f"{dev.name}: Error: {str(e)}")
            # Revert the HVAC status if command failed
            vehicle_dev.updateStateOnServer("hvacStatus", "off")
            dev.updateStateOnServer("onOffState", False)
    
    async def _async_start_hvac(self, vehicle_info, temperature):
        """Async start HVAC"""
        account_info = self.accounts[vehicle_info['account_dev_id']]
        account = account_info['account']
        vehicle = await account.get_api_vehicle(vehicle_info['vin'])
        await vehicle.set_ac_start(temperature)
    
    def _set_charge_mode(self, dev, always_charging):
        """Set charge mode (toggle relay)"""
        vehicle_dev_id = int(dev.pluginProps.get("vehicleDevice", 0))
        if not vehicle_dev_id:
            self.logger.error(f"{dev.name}: No vehicle device configured")
            return
        
        vehicle_info = self.vehicles.get(vehicle_dev_id)
        if not vehicle_info:
            self.logger.error(f"{dev.name}: Vehicle device not found")
            return
        
        mode = "always_charging" if always_charging else "schedule_mode"
        
        future = asyncio.run_coroutine_threadsafe(
            self._async_set_charge_mode(vehicle_info, mode),
            self.loop
        )
        
        try:
            future.result(timeout=10)
            dev.updateStateOnServer("onOffState", always_charging)
            self.logger.info(f"{dev.name}: Charge mode set to {mode}")
        except Exception as e:
            self.logger.error(f"{dev.name}: Error: {str(e)}")
    
    async def _async_set_charge_mode(self, vehicle_info, mode):
        """Async set charge mode"""
        account_info = self.accounts[vehicle_info['account_dev_id']]
        account = account_info['account']
        vehicle = await account.get_api_vehicle(vehicle_info['vin'])
        await vehicle.set_charge_mode(mode)
    
    ########################################
    # Plugin Actions
    ########################################
    
    def setChargeSchedule(self, action, dev):
        """Set a charge schedule"""
        schedule_id = int(action.props.get("scheduleId", 1))
        activated = action.props.get("activated", True)
        
        # Build schedule data for each day
        schedule = {
            "id": schedule_id,
            "activated": activated
        }
        
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        for day in days:
            if action.props.get(day, False):
                start_time = action.props.get(f"{day}Start", "23:00")
                duration = int(action.props.get(f"{day}Duration", 480))
                
                schedule[day] = {
                    "startTime": f"T{start_time}Z",
                    "duration": duration
                }
        
        # Send to vehicle
        vehicle_info = self.vehicles.get(dev.id)
        if not vehicle_info:
            self.logger.error(f"{dev.name}: Vehicle not initialized")
            return
        
        future = asyncio.run_coroutine_threadsafe(
            self._async_set_charge_schedule(vehicle_info, schedule),
            self.loop
        )
        
        try:
            future.result(timeout=10)
            self.logger.info(f"{dev.name}: Schedule {schedule_id} updated")
        except Exception as e:
            self.logger.error(f"{dev.name}: Error setting schedule: {str(e)}")
    
    async def _async_set_charge_schedule(self, vehicle_info, schedule):
        """Async set charge schedule"""
        account_info = self.accounts[vehicle_info['account_dev_id']]
        account = account_info['account']
        vehicle = await account.get_api_vehicle(vehicle_info['vin'])
        
        # Call set_charge_schedules with the schedule data
        await vehicle.set_charge_schedules([schedule])
    
    def setAllDaysSchedule(self, action, dev):
        """Set same schedule for all days"""
        schedule_id = int(action.props.get("scheduleId", 1))
        activated = action.props.get("activated", True)
        start_time = action.props.get("startTime", "23:00")
        duration = int(action.props.get("duration", 480))
        
        schedule = {
            "id": schedule_id,
            "activated": activated
        }
        
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        for day in days:
            schedule[day] = {
                "startTime": f"T{start_time}Z",
                "duration": duration
            }
        
        vehicle_info = self.vehicles.get(dev.id)
        if not vehicle_info:
            self.logger.error(f"{dev.name}: Vehicle not initialized")
            return
        
        future = asyncio.run_coroutine_threadsafe(
            self._async_set_charge_schedule(vehicle_info, schedule),
            self.loop
        )
        
        try:
            future.result(timeout=10)
            self.logger.info(f"{dev.name}: Schedule {schedule_id} set for all days")
        except Exception as e:
            self.logger.error(f"{dev.name}: Error: {str(e)}")
    
    def refreshSchedules(self, action, dev):
        """Refresh schedules from vehicle"""
        vehicle_info = self.vehicles.get(dev.id)
        if not vehicle_info:
            return
        
        future = asyncio.run_coroutine_threadsafe(
            self._async_refresh_schedules(dev.id, vehicle_info),
            self.loop
        )
        
        try:
            schedules = future.result(timeout=10)
            if schedules:
                self.logger.info(f"{dev.name}: Schedules refreshed from vehicle")
                # Could store in plugin prefs here if desired
        except Exception as e:
            self.logger.error(f"{dev.name}: Error: {str(e)}")
    
    async def _async_refresh_schedules(self, vehicle_dev_id, vehicle_info):
        """Async refresh schedules"""
        account_info = self.accounts[vehicle_info['account_dev_id']]
        account = account_info['account']
        vehicle = await account.get_api_vehicle(vehicle_info['vin'])
        
        schedules = await vehicle.get_charge_schedule()
        return schedules
    
    def refreshVehicleData(self, action, dev):
        """Force immediate update of vehicle data"""
        if dev.id in self.vehicles:
            future = asyncio.run_coroutine_threadsafe(
                self._update_vehicle_data(dev.id),
                self.loop
            )
            
            try:
                future.result(timeout=30)
                self.logger.info(f"{dev.name}: Data refreshed")
            except Exception as e:
                self.logger.error(f"{dev.name}: Error: {str(e)}")
    
    def requestBatteryRefresh(self, action, dev):
        """Request vehicle to refresh battery status"""
        vehicle_info = self.vehicles.get(dev.id)
        if not vehicle_info:
            return
        
        future = asyncio.run_coroutine_threadsafe(
            self._async_request_battery_refresh(vehicle_info),
            self.loop
        )
        
        try:
            future.result(timeout=10)
            self.logger.info(f"{dev.name}: Battery refresh requested")
        except Exception as e:
            self.logger.error(f"{dev.name}: Error: {str(e)}")
    
    async def _async_request_battery_refresh(self, vehicle_info):
        """Async request battery refresh"""
        account_info = self.accounts[vehicle_info['account_dev_id']]
        account = account_info['account']
        vehicle = await account.get_api_vehicle(vehicle_info['vin'])
        
        # This sends a request to the vehicle to update
        await vehicle.set_charge_mode(await vehicle.get_charge_mode())
    
    def cancelHvac(self, action, dev):
        """Cancel HVAC preconditioning"""
        vehicle_info = self.vehicles.get(dev.id)
        if not vehicle_info:
            return
        
        future = asyncio.run_coroutine_threadsafe(
            self._async_cancel_hvac(vehicle_info),
            self.loop
        )
        
        try:
            future.result(timeout=10)
            self.logger.info(f"{dev.name}: HVAC cancel command sent")
        except Exception as e:
            self.logger.error(f"{dev.name}: Error: {str(e)}")
    
    async def _async_cancel_hvac(self, vehicle_info):
        """Async cancel HVAC"""
        account_info = self.accounts[vehicle_info['account_dev_id']]
        account = account_info['account']
        vehicle = await account.get_api_vehicle(vehicle_info['vin'])
        await vehicle.set_ac_stop()
    
    ########################################
    # Menu Callbacks
    ########################################
    
    def getAccountDevices(self, filter="", valuesDict=None, typeId="", targetId=0):
        """Return list of account devices for menu"""
        return [
            (str(dev.id), dev.name)
            for dev in indigo.devices.iter("self.renaultAccount")
        ]
    
    def getVehicleDevices(self, filter="", valuesDict=None, typeId="", targetId=0):
        """Return list of vehicle devices for menu"""
        return [
            (str(dev.id), dev.name)
            for dev in indigo.devices.iter("self.renaultVehicle")
        ]
    
    def getVehiclesForAccount(self, filter="", valuesDict=None, typeId="", targetId=0):
        """Return list of vehicles for selected account - synchronous version"""
        try:
            account_dev_id = int(valuesDict.get("accountDevice", 0)) if valuesDict else 0
            
            self.logger.debug(f"getVehiclesForAccount called for account {account_dev_id}")
            
            if not account_dev_id or account_dev_id not in self.accounts:
                return [("", "Please select an account first")]
            
            # Don't use cache - always fetch fresh
            account_info = self.accounts[account_dev_id]
            account = account_info['account']
            
            # Use asyncio but with proper timeout
            try:
                self.logger.debug(f"Fetching vehicles for account {account_dev_id}...")
                future = asyncio.run_coroutine_threadsafe(
                    account.get_vehicles(),
                    self.loop
                )
                vehicles_response = future.result(timeout=10)
                
                self.logger.debug(f"Vehicles response received")
                
                result = []
                if vehicles_response and hasattr(vehicles_response, 'vehicleLinks'):
                    self.logger.debug(f"Found {len(vehicles_response.vehicleLinks)} vehicle(s)")
                    for vehicle in vehicles_response.vehicleLinks:
                        vin = vehicle.vin
                        self.logger.debug(f"Processing vehicle VIN: {vin}")
                        
                        # Simple label - just use VIN
                        result.append((str(vin), str(vin)))
                
                if not result:
                    self.logger.warning("No vehicles found in account")
                    result = [("", "No vehicles found")]
                else:
                    self.logger.debug(f"Returning {len(result)} vehicles")
                
                return result
                
            except Exception as e:
                self.logger.error(f"Error fetching vehicles: {str(e)}")
                import traceback
                self.logger.error(f"Traceback: {traceback.format_exc()}")
                return [("", "Error loading vehicles")]
                
        except Exception as e:
            self.logger.error(f"getVehiclesForAccount error: {str(e)}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return [("", "Error loading vehicles")]
    
    def accountDeviceChanged(self, valuesDict, typeId, devId):
        """Callback when account device selection changes"""
        self.logger.info(f"Account device changed to: {valuesDict.get('accountDevice', 'None')}")
        # Clear any vehicle cache
        if hasattr(self, '_vehicle_cache'):
            self._vehicle_cache = {}
        # Clear the VIN field when account changes
        valuesDict['vin'] = ""
        return valuesDict
    
    ########################################
    # Helper Methods
    ########################################
    
    def _convert_distance(self, km, unit):
        """Convert kilometers to desired unit"""
        if unit == "miles":
            return round(km * 0.621371, 1)
        return km
    
    def _convert_temperature(self, celsius, unit):
        """Convert Celsius to desired unit"""
        if unit == "F":
            return round(celsius * 9/5 + 32, 1)
        return celsius
    
    def _is_connection_error(self, error_str):
        """Check if error is a connection/DNS error"""
        connection_indicators = [
            "Cannot connect to host",
            "nodename nor servname provided",
            "Connection refused",
            "Connection reset",
            "Temporary failure in name resolution",
            "Name or service not known",
            "Network is unreachable"
        ]
        return any(indicator in str(error_str) for indicator in connection_indicators)

    def _is_auth_expired_error(self, exc):
        """Check if exception indicates an expired or missing Renault session token.

        Renault tokens expire periodically. The bundled renault_api library raises
        NotAuthenticatedException in two cases:
          - "Gigya login token not available." (the cached login token is gone)
          - "Authentication expired." (JWT refresh hit a 403005/403013)
        Both are recoverable by logging in again with the stored credentials.
        """
        if NotAuthenticatedException and isinstance(exc, NotAuthenticatedException):
            return True
        # Fall back to string match in case the exception class isn't loaded
        msg = str(exc)
        return ("Gigya login token not available" in msg
                or "Authentication expired" in msg)

    async def _reauthenticate_account(self, account_dev_id):
        """Re-login an account whose session token has expired.

        Returns True on success, False on failure. On success the cached client
        in self.accounts[account_dev_id] is replaced with a fresh one.
        """
        account_info = self.accounts.get(account_dev_id)
        if not account_info:
            return False

        email = account_info.get('email')
        password = account_info.get('password')
        locale = account_info.get('locale', 'en_GB')

        # Email was always available; password may not have been preserved on
        # earlier client objects, so guard against that.
        if not email or not password:
            self.logger.warning(
                "Cannot silently reauthenticate - credentials not cached. "
                "Reload the account device to re-establish a session."
            )
            return False

        result = await self._login_account(account_dev_id, email, password, locale)
        if result['status'] == 'success':
            try:
                dev = indigo.devices[account_dev_id]
                self._mark_account_connected(dev, result['account_id'])
            except Exception:
                pass  # Device may have been deleted in the meantime
            return True

        # Reauth failed - log appropriately
        if result['status'] == 'terminal':
            self.logger.error(
                f"Reauthentication failed (terminal): {result['message']}"
            )
        else:
            self.debugLog(
                f"Reauthentication failed ({result['status']}): {result['message']}"
            )
        return False
    
    def _handle_connection_error(self, dev, vehicle_dev_id, error_msg):
        """Handle connection errors with smart logging"""
        if vehicle_dev_id not in self.vehicles:
            return
        
        vehicle_info = self.vehicles[vehicle_dev_id]
        vehicle_info['connection_errors'] += 1
        error_count = vehicle_info['connection_errors']
        
        # Silent for first 3 errors
        if error_count <= 3:
            self.logger.debug(f"{dev.name}: Connection error {error_count}/3 (silent)")
            return
        
        # 4th error - show consolidated message
        if not vehicle_info['connection_lost']:
            self.logger.error(f"{dev.name}: Communication with Renault servers lost. Will keep retrying...")
            vehicle_info['connection_lost'] = True
        
        # After that, silent until reconnection
        return
    
    def _handle_successful_connection(self, dev, vehicle_dev_id):
        """Handle successful connection after errors"""
        if vehicle_dev_id not in self.vehicles:
            return
        
        vehicle_info = self.vehicles[vehicle_dev_id]
        
        # If we were in error state, log recovery
        if vehicle_info['connection_lost']:
            self.logger.info(f"{dev.name}: Renault communications re-established")
        
        # Reset error tracking
        vehicle_info['connection_errors'] = 0
        vehicle_info['connection_lost'] = False
    
    def _is_forbidden_error(self, error):
        """Check if error is a ForbiddenException (privacy mode, unsupported feature)"""
        if ForbiddenException and isinstance(error, ForbiddenException):
            return True
        # Also check string for backwards compatibility
        error_str = str(error).lower()
        return any(indicator in error_str for indicator in [
            'forbidden',
            'privacy mode',
            'operation not supported',
            'err.func.403'
        ])
    
    def _format_plug_status(self, status):
        """Format plug status for display"""
        # Handle both numeric and string status values
        if isinstance(status, str):
            status_lower = status.lower()
            status_map = {
                "unplugged": "Unplugged",
                "plugged": "Plugged",
                "plugged_waiting_for_charge": "Plugged (Waiting)",
                "plug_error": "Plug Error",
                "plug_unknown": "Unknown"
            }
            return status_map.get(status_lower, status)
        else:
            # Numeric values
            status_map = {
                0: "Unplugged",
                1: "Plugged",
                -1: "Unknown"
            }
            return status_map.get(status, f"Status {status}")
    
    def _format_charging_status(self, status):
        """Format charging status for display"""
        # Handle both numeric and string status values
        if isinstance(status, str):
            # Try to convert to float first (for numeric strings like "0.0")
            try:
                status_float = float(status)
                if status_float == 1.0:
                    return "Charging"
                elif status_float == 0.0:
                    return "Not Charging"
                elif status_float == 0.1 or status_float == 0.4:
                    return "Waiting"
                elif status_float == 0.2:
                    return "Charge Complete"
                elif status_float == -1.0:
                    return "Unknown"
                else:
                    return f"UNKNOWN({status_float})"
            except (ValueError, TypeError):
                # Not a numeric string, treat as actual string status
                status_lower = status.lower()
                status_map = {
                    "not_in_charge": "Not Charging",
                    "waiting_for_a_planned_charge": "Waiting (Planned)",
                    "charge_ended": "Charge Ended",
                    "waiting_for_current_charge": "Waiting (Current)",
                    "energy_flap_opened": "Energy Flap Open",
                    "charge_in_progress": "Charging",
                    "charge_error": "Charge Error",
                    "unavailable": "Unavailable"
                }
                return status_map.get(status_lower, f"UNKNOWN({status})")
        else:
            # Numeric values - handle as float
            try:
                status_float = float(status)
                if status_float == 1.0:
                    return "Charging"
                elif status_float == 0.0:
                    return "Not Charging"
                elif status_float == 0.1 or status_float == 0.4:
                    return "Waiting"
                elif status_float == 0.2:
                    return "Charge Complete"
                elif status_float == -1.0:
                    return "Unknown"
                else:
                    return f"UNKNOWN({status_float})"
            except (ValueError, TypeError):
                return f"UNKNOWN({status})"
    
    def _get_charging_status_cp(self, status):
        """Get control page string for charging status"""
        # Handle both numeric and string status values
        if isinstance(status, str):
            # Try to convert to float first (for numeric strings like "0.0")
            try:
                status_float = float(status)
                if status_float == 1.0:
                    return "charging"
                elif status_float == 0.0:
                    return "not_charging"
                elif status_float == 0.1 or status_float == 0.4:
                    return "waiting"
                elif status_float == 0.2:
                    return "charge_complete"
                elif status_float == -1.0:
                    return "unknown"
                else:
                    return f"unknown_{status_float}"
            except (ValueError, TypeError):
                # Not a numeric string, treat as actual string status
                status_lower = status.lower()
                # Map string statuses to control page values
                cp_map = {
                    "not_in_charge": "not_charging",
                    "waiting_for_a_planned_charge": "waiting_planned",
                    "charge_ended": "charge_ended",
                    "waiting_for_current_charge": "waiting_current",
                    "energy_flap_opened": "flap_opened",
                    "charge_in_progress": "charging",
                    "charge_error": "error",
                    "unavailable": "unavailable"
                }
                return cp_map.get(status_lower, f"unknown_{status_lower}")
        else:
            # Numeric values - convert to consistent strings
            try:
                status_float = float(status)
                if status_float == 1.0:
                    return "charging"
                elif status_float == 0.0:
                    return "not_charging"
                elif status_float == 0.1 or status_float == 0.4:
                    return "waiting"
                elif status_float == 0.2:
                    return "charge_complete"
                elif status_float == -1.0:
                    return "unknown"
                else:
                    return f"unknown_{status_float}"
            except (ValueError, TypeError):
                return f"unknown"
    
    ########################################
    # Plugin Actions
    ########################################
    
    def actionStartCharging(self, action, dev):
        """Action to start charging"""
        try:
            self.logger.info(f"{dev.name}: Start charging action triggered")
            asyncio.run_coroutine_threadsafe(
                self._start_charging(dev.id),
                self.loop
            ).result(timeout=30)
        except Exception as e:
            self.logger.error(f"{dev.name}: Start charging action error: {str(e)}")
    
    def actionStopCharging(self, action, dev):
        """Action to stop charging"""
        try:
            self.logger.info(f"{dev.name}: Stop charging action triggered")
            asyncio.run_coroutine_threadsafe(
                self._stop_charging(dev.id),
                self.loop
            ).result(timeout=30)
        except Exception as e:
            self.logger.error(f"{dev.name}: Stop charging action error: {str(e)}")
    
    def actionStartPreconditioning(self, action, dev):
        """Action to start pre-conditioning"""
        try:
            temp = int(action.props.get("targetTemperature", 21))
            self.logger.info(f"{dev.name}: Start pre-conditioning action triggered (temp: {temp}°C)")
            asyncio.run_coroutine_threadsafe(
                self._start_preconditioning(dev.id, temp),
                self.loop
            ).result(timeout=30)
        except Exception as e:
            self.logger.error(f"{dev.name}: Start pre-conditioning action error: {str(e)}")
    
    def actionSetChargeMode(self, action, dev):
        """Action to set charge mode"""
        try:
            mode = action.props.get("chargeMode", "always_charging")
            self.logger.info(f"{dev.name}: Set charge mode action triggered (mode: {mode})")
            asyncio.run_coroutine_threadsafe(
                self._set_charge_mode(dev.id, mode),
                self.loop
            ).result(timeout=30)
        except Exception as e:
            self.logger.error(f"{dev.name}: Set charge mode action error: {str(e)}")
    
    def actionSetChargeSchedule(self, action, dev):
        """Action to set charge schedule"""
        try:
            schedule_id = int(action.props.get("scheduleId", 1))
            activated = action.props.get("activated", True)
            
            self.logger.info(f"{dev.name}: Set charge schedule action triggered (schedule {schedule_id})")
            
            # Build schedule data based on UI inputs
            schedules = {}
            
            if action.props.get("useAllDays", False):
                # Same time for all days
                start_time = action.props.get("allDaysStart", "23:00")
                duration = int(action.props.get("allDaysDuration", 480))
                
                for day in ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]:
                    schedules[day] = {
                        "startTime": start_time,
                        "duration": duration
                    }
            else:
                # Individual days
                for day in ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]:
                    if action.props.get(day, False):
                        start_time = action.props.get(f"{day}Start", "23:00")
                        duration = int(action.props.get(f"{day}Duration", 480))
                        schedules[day] = {
                            "startTime": start_time,
                            "duration": duration
                        }
            
            asyncio.run_coroutine_threadsafe(
                self._set_charge_schedule(dev.id, schedule_id, activated, schedules),
                self.loop
            ).result(timeout=30)
            
        except Exception as e:
            self.logger.error(f"{dev.name}: Set charge schedule action error: {str(e)}")
    
    def actionRefreshVehicleData(self, action, dev):
        """Action to refresh vehicle data immediately"""
        try:
            self.logger.info(f"{dev.name}: Refresh vehicle data action triggered")
            asyncio.run_coroutine_threadsafe(
                self._update_vehicle_data(dev.id),
                self.loop
            ).result(timeout=30)
        except Exception as e:
            self.logger.error(f"{dev.name}: Refresh vehicle data action error: {str(e)}")
    
    def actionRequestBatteryRefresh(self, action, dev):
        """Action to request battery refresh from vehicle"""
        try:
            self.logger.info(f"{dev.name}: Request battery refresh action triggered")
            
            if dev.id not in self.vehicles:
                self.logger.error(f"{dev.name}: Vehicle not found")
                return
            
            vehicle_info = self.vehicles[dev.id]
            account_dev_id = vehicle_info['account_dev_id']
            vin = vehicle_info['vin']
            
            if account_dev_id not in self.accounts:
                self.logger.error(f"{dev.name}: Account not found")
                return
            
            async def request_refresh():
                account_info = self.accounts[account_dev_id]
                account = account_info['account']
                vehicle = await account.get_api_vehicle(vin)
                
                # Request battery refresh
                await vehicle.get_battery_status(cached=False)
                self.logger.info(f"{dev.name}: Battery refresh requested")
            
            asyncio.run_coroutine_threadsafe(
                request_refresh(),
                self.loop
            ).result(timeout=30)
            
        except Exception as e:
            self.logger.error(f"{dev.name}: Request battery refresh action error: {str(e)}")
    
    def actionStopHvac(self, action, dev):
        """Action to stop HVAC/pre-conditioning"""
        try:
            self.logger.info(f"{dev.name}: Stop HVAC action triggered")
            
            if dev.id not in self.vehicles:
                self.logger.error(f"{dev.name}: Vehicle not found")
                return
            
            vehicle_info = self.vehicles[dev.id]
            account_dev_id = vehicle_info['account_dev_id']
            vin = vehicle_info['vin']
            
            if account_dev_id not in self.accounts:
                self.logger.error(f"{dev.name}: Account not found")
                return
            
            async def stop_hvac():
                account_info = self.accounts[account_dev_id]
                account = account_info['account']
                vehicle = await account.get_api_vehicle(vin)
                
                # Stop HVAC
                result = await vehicle.set_ac_stop()
                self.logger.info(f"{dev.name}: HVAC stopped: {result}")
            
            asyncio.run_coroutine_threadsafe(
                stop_hvac(),
                self.loop
            ).result(timeout=30)
            
        except Exception as e:
            self.logger.error(f"{dev.name}: Stop HVAC action error: {str(e)}")
    
    def actionSetBatterySocLimits(self, action, dev):
        """Action to set battery SoC limits"""
        try:
            soc_limit = int(action.props.get("socLimit", 80))
            self.logger.info(f"{dev.name}: Set battery SoC limit action triggered ({soc_limit}%)")
            
            if dev.id not in self.vehicles:
                self.logger.error(f"{dev.name}: Vehicle not found")
                return
            
            vehicle_info = self.vehicles[dev.id]
            account_dev_id = vehicle_info['account_dev_id']
            vin = vehicle_info['vin']
            
            if account_dev_id not in self.accounts:
                self.logger.error(f"{dev.name}: Account not found")
                return
            
            async def set_soc_limits():
                account_info = self.accounts[account_dev_id]
                account = account_info['account']
                vehicle = await account.get_api_vehicle(vin)
                
                # Set SoC limits (charge limit)
                result = await vehicle.set_charge_limit(soc_limit)
                self.logger.info(f"{dev.name}: Battery charge limit set to {soc_limit}%: {result}")
            
            asyncio.run_coroutine_threadsafe(
                set_soc_limits(),
                self.loop
            ).result(timeout=30)
            
        except Exception as e:
            self.logger.error(f"{dev.name}: Set battery SoC limit action error: {str(e)}")
    
    def actionSoundHorn(self, action, dev):
        """Action to sound the horn"""
        try:
            self.logger.info(f"{dev.name}: Sound horn action triggered")
            
            if dev.id not in self.vehicles:
                self.logger.error(f"{dev.name}: Vehicle not found")
                return
            
            vehicle_info = self.vehicles[dev.id]
            account_dev_id = vehicle_info['account_dev_id']
            vin = vehicle_info['vin']
            
            if account_dev_id not in self.accounts:
                self.logger.error(f"{dev.name}: Account not found")
                return
            
            async def sound_horn():
                account_info = self.accounts[account_dev_id]
                account = account_info['account']
                vehicle = await account.get_api_vehicle(vin)
                
                # Sound horn
                result = await vehicle.set_horn_start()
                self.logger.info(f"{dev.name}: Horn sounded: {result}")
            
            asyncio.run_coroutine_threadsafe(
                sound_horn(),
                self.loop
            ).result(timeout=30)
            
        except Exception as e:
            self.logger.error(f"{dev.name}: Sound horn action error: {str(e)}")
    
    def actionFlashLights(self, action, dev):
        """Action to flash lights"""
        try:
            self.logger.info(f"{dev.name}: Flash lights action triggered")
            
            if dev.id not in self.vehicles:
                self.logger.error(f"{dev.name}: Vehicle not found")
                return
            
            vehicle_info = self.vehicles[dev.id]
            account_dev_id = vehicle_info['account_dev_id']
            vin = vehicle_info['vin']
            
            if account_dev_id not in self.accounts:
                self.logger.error(f"{dev.name}: Account not found")
                return
            
            async def flash_lights():
                account_info = self.accounts[account_dev_id]
                account = account_info['account']
                vehicle = await account.get_api_vehicle(vin)
                
                # Flash lights
                result = await vehicle.set_lights_start()
                self.logger.info(f"{dev.name}: Lights flashed: {result}")
            
            asyncio.run_coroutine_threadsafe(
                flash_lights(),
                self.loop
            ).result(timeout=30)
            
        except Exception as e:
            self.logger.error(f"{dev.name}: Flash lights action error: {str(e)}")
    
    ########################################
    # Event Triggers
    ########################################
    
    def triggerCheck(self, event):
        """Check if a trigger should fire"""
        if event.pluginTypeId == "chargingStatusChanged":
            return self._check_charging_status_trigger(event)
        elif event.pluginTypeId == "batteryLevelChanged":
            return self._check_battery_level_trigger(event)
        elif event.pluginTypeId == "plugStatusChanged":
            return self._check_plug_status_trigger(event)
        elif event.pluginTypeId == "hvacStatusChanged":
            return self._check_hvac_status_trigger(event)
        elif event.pluginTypeId == "chargingPowerChanged":
            return self._check_charging_power_trigger(event)
        return False
    
    def _check_charging_status_trigger(self, event):
        """Check if charging status changed trigger should fire"""
        dev_id = int(event.pluginProps.get("device", 0))
        if dev_id == 0:
            return False
        
        dev = indigo.devices.get(dev_id)
        if not dev:
            return False
        
        condition = event.pluginProps.get("statusCondition", "any")
        current_status = dev.states.get("chargingStatusCP", "")
        
        if condition == "any":
            return True
        elif condition == "specific":
            target_status = event.pluginProps.get("specificStatus", "")
            return current_status == target_status
        elif condition == "to_charging":
            return current_status == "charging"
        elif condition == "to_not_charging":
            return current_status == "not_charging"
        elif condition == "to_waiting":
            return current_status == "waiting"
        elif condition == "to_error":
            return current_status == "error"
        
        return False
    
    def _check_battery_level_trigger(self, event):
        """Check if battery level changed trigger should fire"""
        dev_id = int(event.pluginProps.get("device", 0))
        if dev_id == 0:
            return False
        
        dev = indigo.devices.get(dev_id)
        if not dev:
            return False
        
        condition = event.pluginProps.get("levelCondition", "any")
        current_level = dev.states.get("batteryLevelInt", 0)
        
        if condition == "any":
            return True
        elif condition == "above":
            target_level = int(event.pluginProps.get("levelValue", 80))
            return current_level > target_level
        elif condition == "below":
            target_level = int(event.pluginProps.get("levelValue", 80))
            return current_level < target_level
        elif condition == "equals":
            target_level = int(event.pluginProps.get("levelValue", 80))
            return current_level == target_level
        
        return False
    
    def _check_plug_status_trigger(self, event):
        """Check if plug status changed trigger should fire"""
        dev_id = int(event.pluginProps.get("device", 0))
        if dev_id == 0:
            return False
        
        dev = indigo.devices.get(dev_id)
        if not dev:
            return False
        
        condition = event.pluginProps.get("plugCondition", "any")
        current_status = dev.states.get("plugStatus", "")
        
        if condition == "any":
            return True
        elif condition == "plugged":
            return "Plugged" in current_status
        elif condition == "unplugged":
            return "Unplugged" in current_status
        
        return False
    
    def _check_hvac_status_trigger(self, event):
        """Check if HVAC status changed trigger should fire"""
        dev_id = int(event.pluginProps.get("device", 0))
        if dev_id == 0:
            return False
        
        dev = indigo.devices.get(dev_id)
        if not dev:
            return False
        
        condition = event.pluginProps.get("hvacCondition", "any")
        current_status = dev.states.get("hvacStatus", "")
        
        if condition == "any":
            return True
        elif condition == "on":
            return current_status == "on"
        elif condition == "off":
            return current_status == "off"
        
        return False
    
    def _check_charging_power_trigger(self, event):
        """Check if charging power changed trigger should fire"""
        dev_id = int(event.pluginProps.get("device", 0))
        if dev_id == 0:
            return False
        
        dev = indigo.devices.get(dev_id)
        if not dev:
            return False
        
        condition = event.pluginProps.get("powerCondition", "any")
        current_power = dev.states.get("chargingPower", 0)
        
        if condition == "any":
            return True
        elif condition == "above":
            target_power = float(event.pluginProps.get("powerValue", 5))
            return current_power > target_power
        elif condition == "below":
            target_power = float(event.pluginProps.get("powerValue", 5))
            return current_power < target_power
        elif condition == "zero":
            return current_power == 0
        elif condition == "nonzero":
            return current_power > 0
        
        return False

