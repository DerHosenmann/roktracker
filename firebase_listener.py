"""Listen to Firestore and launch kingdom scans when jobs appear."""

import time
import threading

import firebase_admin
from firebase_admin import credentials, firestore

from roktracker.kingdom.governor_printer import print_gov_state
from roktracker.kingdom.scanner import KingdomScanner
from roktracker.utils.general import load_config
from roktracker.utils.output_formats import OutputFormats

# --- Configuration ---

# 1. Update this with the path to the JSON file you downloaded.
SERVICE_ACCOUNT_KEY_PATH = "serviceAccountKey.json"

# 2. Update this with your User ID from the Firebase Authentication console.
USER_ID = "YOUR_USER_ID_HERE"

# --- Bot Logic ---


DEFAULT_SCAN_OPTIONS_FULL = {
    "ID": True,
    "Name": True,
    "Power": True,
    "Killpoints": True,
    "Alliance": True,
    "T1 Kills": True,
    "T2 Kills": True,
    "T3 Kills": True,
    "T4 Kills": True,
    "T5 Kills": True,
    "Ranged": True,
    "Deads": True,
    "Rss Assistance": True,
    "Rss Gathered": True,
    "Helps": True,
}

DEFAULT_SCAN_OPTIONS_SEED = {
    **{k: v for k, v in DEFAULT_SCAN_OPTIONS_FULL.items()},
    "T1 Kills": False,
    "T2 Kills": False,
    "T3 Kills": False,
    "T4 Kills": False,
    "T5 Kills": False,
    "Ranged": False,
    "Deads": False,
    "Rss Assistance": False,
    "Rss Gathered": False,
    "Helps": False,
}


def _build_scan_options(mode: str, overrides: dict | None = None) -> dict:
    """Return scan options for the given mode with optional overrides."""
    options = DEFAULT_SCAN_OPTIONS_SEED if mode == "seed" else DEFAULT_SCAN_OPTIONS_FULL
    options = {k: v for k, v in options.items()}

    if overrides:
        for key in options:
            if key in overrides:
                options[key] = bool(overrides[key])
    return options


def run_scan_bot(scan_doc_ref, scan_data) -> None:
    """Run a kingdom scan based on Firestore document settings."""

    print(f"Thread started for scan job: {scan_doc_ref.id}")

    # Update job status
    scan_doc_ref.update(
        {
            "status": "running",
            "progress": 0,
            "logs": firestore.ArrayUnion(["[BOT] Job received. Starting scanner..."]),
        }
    )

    def log_to_firestore(msg: str) -> None:
        print(msg)
        scan_doc_ref.update({"logs": firestore.ArrayUnion([msg])})

    def state_callback(state: str) -> None:
        log_to_firestore(f"[STATE] {state}")

    def gov_callback(_, extra) -> None:
        progress = int(extra.current_governor / extra.target_governor * 100)
        scan_doc_ref.update({"progress": progress})

    try:
        config = load_config()

        bluestacks_port = int(
            scan_data.get("adbPort", config["general"]["adb_port"])
        )
        kingdom = scan_data.get("kingdom", config["scan"]["kingdom_name"]) or ""
        amount = int(
            scan_data.get("amount", config["scan"]["people_to_scan"] or 0)
        )
        mode = scan_data.get("mode", "full")

        scan_options = _build_scan_options(mode, scan_data.get("scanOptions"))

        resume = bool(scan_data.get("resume", config["scan"]["resume"]))
        advanced_scroll = bool(
            scan_data.get("advancedScroll", config["scan"]["advanced_scroll"])
        )
        track_inactives = bool(
            scan_data.get("trackInactives", config["scan"]["track_inactives"])
        )
        validate_kills = bool(
            scan_data.get("validateKills", config["scan"]["validate_kills"])
        )
        reconstruct_kills = bool(
            scan_data.get("reconstructKills", config["scan"]["reconstruct_kills"])
        )
        validate_power = bool(
            scan_data.get("validatePower", config["scan"]["validate_power"])
        )
        power_threshold = int(
            scan_data.get("powerThreshold", config["scan"]["power_threshold"])
        )
        info_close = float(
            scan_data.get("infoTime", config["scan"]["timings"]["info_close"])
        )
        gov_close = float(
            scan_data.get("govTime", config["scan"]["timings"]["gov_close"])
        )

        output_formats = OutputFormats()
        if isinstance(scan_data.get("formats"), dict):
            output_formats.from_dict(scan_data["formats"])
        else:
            output_formats.from_dict(config["scan"]["formats"])

        config["scan"]["timings"]["info_close"] = info_close
        config["scan"]["timings"]["gov_close"] = gov_close
        config["scan"]["advanced_scroll"] = advanced_scroll

        scanner = KingdomScanner(config, scan_options, bluestacks_port)
        scanner.set_governor_callback(gov_callback)
        scanner.set_state_callback(state_callback)
        scanner.set_output_handler(log_to_firestore)

        scanner.start_scan(
            kingdom,
            amount,
            resume,
            track_inactives,
            validate_kills,
            reconstruct_kills,
            validate_power,
            power_threshold,
            output_formats,
        )

        log_to_firestore("[BOT] Scan completed successfully.")
        scan_doc_ref.update({"status": "completed", "progress": 100})

    except Exception as e:  # noqa: BLE001
        error_log = f"[BOT] Error: {e}"
        log_to_firestore(error_log)
        scan_doc_ref.update({"status": "failed"})

# --- Firestore Listener ---


def on_snapshot(doc_snapshot, changes, read_time):
    """Called whenever there's a change in the collection."""
    for change in changes:
        if change.type.name == 'ADDED':
            scan_doc = change.document
            scan_data = scan_doc.to_dict()

            if scan_data.get('status') == 'pending':
                print(f"New pending scan job found: {scan_doc.id}")
                bot_thread = threading.Thread(
                    target=run_scan_bot,
                    args=(scan_doc.reference, scan_data)
                )
                bot_thread.start()


def main():
    """Initialize Firebase and start the listener."""
    cred = credentials.Certificate(SERVICE_ACCOUNT_KEY_PATH)
    firebase_admin.initialize_app(cred)
    db = firestore.client()

    scans_ref = db.collection(u'users').document(USER_ID).collection(u'scans')
    query_watch = scans_ref.on_snapshot(on_snapshot)

    print(f"Bot is running. Listening for new scan jobs for user: {USER_ID}")
    print("Create a new scan from the web app to test.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down bot listener.")
        query_watch.unsubscribe()


if __name__ == '__main__':
    main()
