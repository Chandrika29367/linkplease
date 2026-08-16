import asyncio
import hmac
import hashlib
import json
import os
import sys
import subprocess
import time
import httpx

# Settings for the load test
BASE_URL = "http://127.0.0.1:8001"
API_KEY = "test-secret-key-1234"
DB_URL = "sqlite:///./load_test.db"

def get_signature(body_bytes: bytes, secret: str) -> str:
    h = hmac.new(secret.encode("utf-8"), body_bytes, hashlib.sha256)
    return f"sha256={h.hexdigest()}"

async def send_webhook(client: httpx.AsyncClient, user_id: str, comment_id: str, event_id: str):
    payload = {
        "event_id": event_id,
        "event_type": "comment.created",
        "sent_at": "2026-08-10T09:14:22.481Z",
        "data": {
            "comment_id": comment_id,
            "post_id": "post_load",
            "text": "PRICE list please!",
            "from": {
                "user_id": user_id,
                "username": f"user.{user_id}"
            }
        }
    }
    body_bytes = json.dumps(payload).encode("utf-8")
    sig = get_signature(body_bytes, API_KEY)
    headers = {
        "X-PseudoGram-Signature": sig,
        "Content-Type": "application/json"
    }
    
    try:
        start_time = time.time()
        response = await client.post(f"{BASE_URL}/webhook", content=body_bytes, headers=headers, timeout=10.0)
        elapsed = time.time() - start_time
        return response.status_code, elapsed
    except Exception as e:
        return 0, str(e)

async def main():
    # Clean up prior database
    if os.path.exists("./load_test.db"):
        try:
            os.remove("./load_test.db")
        except Exception:
            pass
    if os.path.exists("./load_test.db-wal"):
        try:
            os.remove("./load_test.db-wal")
        except Exception:
            pass

    # Start Uvicorn subprocess
    env = os.environ.copy()
    env["PSEUDOGRAM_API_KEY"] = API_KEY
    env["DATABASE_URL"] = DB_URL
    
    print("Starting Uvicorn server for load test...")
    proc = subprocess.Popen(
        ["python", "tests/load_test_server.py"],
        env=env,
        stdout=sys.stdout,
        stderr=sys.stderr
    )
    
    # Wait for server to start up
    time.sleep(3.0)
    
    limits = httpx.Limits(max_keepalive_connections=None, max_connections=None)
    async with httpx.AsyncClient(limits=limits) as client:
        # 1. Create a Rule
        print("Creating rule 'PRICE'...")
        rule_payload = {
            "keyword": "PRICE",
            "dm_message": "Load test DM: Here is the price list!"
        }
        res = await client.post(f"{BASE_URL}/rules", json=rule_payload)
        if res.status_code != 201:
            print(f"Failed to create rule: {res.status_code} {res.text}")
            proc.terminate()
            return
        print("Rule created successfully.")

        # 2. Fire 500 webhook events concurrently with a tiny stagger
        num_requests = 500
        print(f"Firing {num_requests} webhooks (12ms stagger)...")
        tasks = []
        start_all = time.time()
        for i in range(num_requests):
            task = asyncio.create_task(
                send_webhook(
                    client,
                    user_id=f"usr_load_{i}",
                    comment_id=f"cmt_load_{i}",
                    event_id=f"evt_load_{i}"
                )
            )
            tasks.append(task)
            await asyncio.sleep(0.012)
        
        results = await asyncio.gather(*tasks)
        total_time = time.time() - start_all
        print(f"Finished sending {num_requests} webhooks in {total_time:.2f} seconds.")

        # 3. Analyze results
        success_count = sum(1 for status, _ in results if status == 200)
        failure_count = len(results) - success_count
        slow_responses = sum(1 for _, elapsed in results if isinstance(elapsed, float) and elapsed > 5.0)

        print(f"Results:")
        print(f"  - HTTP 200 count: {success_count} / {num_requests}")
        print(f"  - Failures count: {failure_count}")
        if failure_count > 0:
            print("First 10 failures:")
            failures = [(status, err) for status, err in results if status != 200][:10]
            for f in failures:
                print(f"    {f}")
        print(f"  - Responses slower than 5 seconds: {slow_responses}")
        
        # 4. Wait until queued == 0 (with a 5 minutes timeout)
        print("Waiting for queue to drain (queued == 0)...")
        drain_start = time.time()
        timeout_seconds = 300
        queue_is_zero = False
        
        while time.time() - drain_start < timeout_seconds:
            try:
                stats_res = await client.get(f"{BASE_URL}/stats")
                if stats_res.status_code == 200:
                    stats = stats_res.json()
                    print(f"Stats: {stats}")
                    if stats.get("queued", 0) == 0:
                        queue_is_zero = True
                        break
            except Exception as e:
                print(f"Error checking stats: {e}")
            await asyncio.sleep(2.0)
            
        drain_time = time.time() - drain_start
        if queue_is_zero:
            print(f"Queue successfully drained in {drain_time:.2f} seconds.")
        else:
            print(f"Queue failed to drain within 5 minutes. Time elapsed: {drain_time:.2f} seconds.")
            
        # 5. Print final stats
        final_stats_res = await client.get(f"{BASE_URL}/stats")
        final_stats = final_stats_res.json()
        print("\nFINAL STATS:")
        print(f"  sent: {final_stats.get('sent')}")
        print(f"  failed: {final_stats.get('failed')}")
        print(f"  queued: {final_stats.get('queued')}")
        print(f"  duplicates_blocked: {final_stats.get('duplicates_blocked')}")

    # Terminate server
    print("Terminating server...")
    proc.terminate()
    proc.wait()
    
    # Clean up database
    if os.path.exists("./load_test.db"):
        try:
            os.remove("./load_test.db")
        except Exception:
            pass
    if os.path.exists("./load_test.db-wal"):
        try:
            os.remove("./load_test.db-wal")
        except Exception:
            pass

    if success_count == num_requests and slow_responses < 15 and queue_is_zero:
        print("LOAD TEST PASSED SUCCESSFULLY!")
    else:
        print("LOAD TEST FAILED.")

if __name__ == "__main__":
    asyncio.run(main())
