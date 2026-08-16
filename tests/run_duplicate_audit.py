import os
import sys
import time
import httpx
import hmac
import hashlib
import json
import sqlite3
import subprocess
import asyncio

BASE_URL = "http://127.0.0.1:8001"
API_KEY = "test-secret-key-1234"
DB_URL = "sqlite:///./duplicate_audit.db"

def get_signature(body_bytes: bytes, secret: str) -> str:
    h = hmac.new(secret.encode("utf-8"), body_bytes, hashlib.sha256)
    return f"sha256={h.hexdigest()}"

async def main():
    # 1. Clean up databases
    for db_file in ["./duplicate_audit.db", "./duplicate_audit.db-wal", "./duplicate_audit.db-shm"]:
        if os.path.exists(db_file):
            try:
                os.remove(db_file)
            except Exception:
                pass

    # 2. Start the patched server
    env = os.environ.copy()
    env["DATABASE_URL"] = DB_URL
    env["PSEUDOGRAM_API_KEY"] = API_KEY
    env["PSEUDOGRAM_BASE_URL"] = "https://pseudogram-api.onrender.com"
    
    print("Starting patched Uvicorn server...")
    proc = subprocess.Popen(
        ["python", "tests/load_test_server.py"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    
    # Wait for startup
    time.sleep(3.0)
    
    client = httpx.AsyncClient(timeout=30.0)
    try:
        # 3. Create one PRICE rule
        print("Creating rule 'PRICE'...")
        rule_payload = {
            "keyword": "PRICE",
            "dm_message": "Hello! Here is the price list!"
        }
        res = await client.post(f"{BASE_URL}/rules", json=rule_payload)
        assert res.status_code == 201
        print("Rule created successfully.")

        # 4. Fire 500 comment.created events from the SAME user
        num_requests = 500
        print(f"Firing {num_requests} webhooks for the same user (usr_audit)...")
        tasks = []
        for i in range(num_requests):
            payload = {
                "event_id": f"evt_audit_{i}",
                "event_type": "comment.created",
                "sent_at": "2026-08-10T09:14:22Z",
                "data": {
                    "comment_id": f"cmt_audit_{i}",
                    "post_id": "post_1",
                    "text": "PRICE list please",
                    "created_at": "2026-08-10T09:14:21.900Z",
                    "from": {
                        "user_id": "usr_audit",
                        "username": "user.audit"
                    }
                }
            }
            body_bytes = json.dumps(payload).encode("utf-8")
            sig = get_signature(body_bytes, API_KEY)
            headers = {"X-PseudoGram-Signature": sig}
            tasks.append(
                client.post(f"{BASE_URL}/webhook", content=body_bytes, headers=headers)
            )
            
        results = await asyncio.gather(*tasks)
        print(f"Ingested {num_requests} webhooks. HTTP 200 count: {sum(1 for r in results if r.status_code == 200)}")

        # 5. Poll /stats until queued == 0 and all events are processed
        print("Waiting for queue to drain...")
        drain_start = time.time()
        while True:
            stats_res = await client.get(f"{BASE_URL}/stats")
            stats = stats_res.json()
            
            # Check db unprocessed count
            conn = sqlite3.connect("./duplicate_audit.db")
            cursor = conn.cursor()
            cursor.execute("SELECT count(*) FROM events WHERE processed_at IS NULL")
            unprocessed = cursor.fetchone()[0]
            conn.close()
            
            print(f"Stats: {stats} | Unprocessed events in DB: {unprocessed}")
            if stats.get("queued", 0) == 0 and unprocessed == 0:
                break
            await asyncio.sleep(1.0)
            
        print(f"Queue successfully drained in {time.time() - drain_start:.2f} seconds.")

        # 6. Fetch final stats
        stats_res = await client.get(f"{BASE_URL}/stats")
        final_stats = stats_res.json()
        
        # 7. Verify directly via DB
        conn = sqlite3.connect("./duplicate_audit.db")
        cursor = conn.cursor()
        cursor.execute("SELECT count(*) FROM dm_jobs")
        jobs_count = cursor.fetchone()[0]
        cursor.execute("SELECT count(*) FROM blocked_attempts")
        blocked_count = cursor.fetchone()[0]
        conn.close()

        print("\n--- TEST RESULT REPORT ---")
        print(f"Final /stats: {final_stats}")
        print(f"Number of DM jobs created in database: {jobs_count}")
        print(f"Number of DMs sent (delivered/queued): {final_stats['sent']}")
        print(f"Duplicates Blocked (stats): {final_stats['duplicates_blocked']}")
        print(f"Blocked attempts count in database: {blocked_count}")
        
        if jobs_count == 1 and blocked_count == 499:
            print("\nDUPLICATE AUDIT TEST PASSED SUCCESSFULLY!")
        else:
            print("\nDUPLICATE AUDIT TEST FAILED!")

    finally:
        await client.aclose()
        print("Terminating server...")
        proc.terminate()
        proc.wait()
        
        # Keep database for diagnostics

if __name__ == "__main__":
    asyncio.run(main())
