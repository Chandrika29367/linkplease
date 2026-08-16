import time
import httpx
import hmac
import hashlib
import json
import sqlite3

BASE_LOCAL_URL = "http://127.0.0.1:8000"
PSEUDOGRAM_BASE_URL = "https://pseudogram-api.onrender.com"
API_KEY = "mock-api-key"
WEBHOOK_PUBLIC_URL = "https://6c791fa5571f27.lhr.life/webhook"

def get_signature(body_bytes: bytes, secret: str) -> str:
    h = hmac.new(secret.encode("utf-8"), body_bytes, hashlib.sha256)
    return f"sha256={h.hexdigest()}"

def main():
    client = httpx.Client(timeout=30.0)
    
    # 1. Show starting stats
    stats_before = client.get(f"{BASE_LOCAL_URL}/stats").json()
    print(f"Initial Stats: {stats_before}")
    
    # 2. Create the PRICE rule
    print("Creating rule 'PRICE'...")
    rule_payload = {
        "keyword": "PRICE",
        "dm_message": "Hello from LinkPlease! Here is the price list!"
    }
    res = client.post(f"{BASE_LOCAL_URL}/rules", json=rule_payload)
    print(f"Rule created: {res.status_code} {res.text}")
    
    # 3. Run the 500-event simulation on PseudoGram
    print(f"Starting PseudoGram simulation with webhook: {WEBHOOK_PUBLIC_URL}")
    sim_payload = {
        "webhook_url": WEBHOOK_PUBLIC_URL
    }
    headers = {"X-API-Key": API_KEY}
    sim_res = client.post(f"{PSEUDOGRAM_BASE_URL}/v1/simulate/start", json=sim_payload, headers=headers)
    if sim_res.status_code not in [200, 201]:
        print(f"Failed to start simulation: {sim_res.status_code} {sim_res.text}")
        return
        
    sim_data = sim_res.json()
    run_id = sim_data.get("run_id")
    print(f"Captured run_id: {run_id}")
    
    # 4. Wait until local /stats has queued=0
    print("Waiting for local queue to drain (queued=0)...")
    while True:
        stats = client.get(f"{BASE_LOCAL_URL}/stats").json()
        print(f"Current Stats: {stats}")
        if stats.get("queued", 0) == 0:
            # Check simulation status on PseudoGram to make sure the run is completed
            status_res = client.get(f"{PSEUDOGRAM_BASE_URL}/v1/simulate/{run_id}/truth", headers=headers)
            if status_res.status_code == 200:
                truth = status_res.json()
                print(f"Simulator status: {truth.get('status')}")
                if truth.get('status') in ['completed', 'finished', 'failed', 'done'] or truth.get('status') is None:
                    break
            else:
                print(f"Failed to check simulator status: {status_res.status_code}")
            time.sleep(5)
        else:
            time.sleep(5)
            
    # 5. Query /stats and show final values
    final_local_stats = client.get(f"{BASE_LOCAL_URL}/stats").json()
    print(f"\nFinal Local Stats: {final_local_stats}")
    
    # 6. Query PseudoGram truth
    truth_res = client.get(f"{PSEUDOGRAM_BASE_URL}/v1/simulate/{run_id}/truth", headers=headers)
    truth = truth_res.json()
    print(f"\nPseudoGram Truth: {truth}")
    
    # Compare
    print("\n--- COMPARISON ---")
    print(f"Local sent: {final_local_stats.get('sent')} | Truth delivered: {truth.get('delivered')}")
    print(f"Local failed: {final_local_stats.get('failed')} | Truth failed: {truth.get('failed')}")
    print(f"Local blocked: {final_local_stats.get('duplicates_blocked')} | Truth blocked: {truth.get('blocked_duplicates')}")
    
    # 7. Second concurrency test with 500 comments from the SAME user_id for the SAME rule
    print("\n--- TEST 2: Concurrency test with 500 duplicate webhooks from SAME user ---")
    
    import asyncio
    
    async def send_duplicate_webhooks():
        async with httpx.AsyncClient() as acl:
            tasks = []
            for i in range(500):
                payload = {
                    "event_id": f"evt_dup_audit_{i}_{time.time()}",
                    "event_type": "comment.created",
                    "sent_at": "2026-08-10T09:14:22.481Z",
                    "data": {
                        "comment_id": f"cmt_dup_audit_{i}",
                        "post_id": "post_1",
                        "text": "PRICE list",
                        "created_at": "2026-08-10T09:14:21.900Z",
                        "from": {
                            "user_id": "usr_audit_same",
                            "username": "user.same"
                        }
                    }
                }
                body_bytes = json.dumps(payload).encode("utf-8")
                sig = get_signature(body_bytes, API_KEY)
                headers = {"X-PseudoGram-Signature": sig}
                tasks.append(
                    acl.post(f"{BASE_LOCAL_URL}/webhook", content=body_bytes, headers=headers)
                )
            
            print("Firing 500 duplicate webhooks concurrently...")
            results = await asyncio.gather(*tasks)
            print(f"Finished firing. HTTP 200 counts: {sum(1 for r in results if r.status_code == 200)}")
            
    asyncio.run(send_duplicate_webhooks())
    
    # Wait until queued = 0
    print("Waiting for queue to drain to 0...")
    while True:
        stats = client.get(f"{BASE_LOCAL_URL}/stats").json()
        print(f"Current Stats: {stats}")
        if stats.get("queued", 0) == 0:
            break
        time.sleep(3)
        
    final_stats_2 = client.get(f"{BASE_LOCAL_URL}/stats").json()
    print(f"\nFinal Stats after Concurrency Test: {final_stats_2}")
    
    # Count in the database
    conn = sqlite3.connect("./linkplease.db")
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM dm_jobs WHERE user_id = 'usr_audit_same'")
    jobs_count = cursor.fetchone()[0]
    cursor.execute("SELECT count(*) FROM blocked_attempts WHERE user_id = 'usr_audit_same'")
    blocked_count = cursor.fetchone()[0]
    conn.close()
    
    print(f"\nVerification:")
    print(f"  - DM jobs created for usr_audit_same: {jobs_count} (Expected: 1)")
    print(f"  - Blocked attempts for usr_audit_same: {blocked_count} (Expected: 499)")
    
    if jobs_count == 1 and blocked_count == 499:
        print("\nCONCURRENCY TEST 2 PASSED SUCCESSFULLY!")
    else:
        print("\nCONCURRENCY TEST 2 FAILED!")

if __name__ == "__main__":
    main()
