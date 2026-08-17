import httpx
import json
import sys

def main():
    print("=== LinkPlease Assignment Submission Helper ===")
    
    # 1. Collect inputs
    email = input("1. Enter the email you applied with: ").strip()
    if not email:
        print("Email cannot be empty!")
        sys.exit(1)
        
    working_url = input("2. Enter your Render deployed URL (e.g. https://linkplease-xxx.onrender.com): ").strip()
    if not working_url:
        print("Deployed URL cannot be empty!")
        sys.exit(1)
    if not working_url.startswith("http"):
        working_url = "https://" + working_url
        
    loom_url = input("3. Enter your Zoom/Loom recording URL: ").strip()
    if not loom_url:
        print("Loom URL cannot be empty!")
        sys.exit(1)
        
    start_date = input("4. Enter your start date (default is 2026-08-11): ").strip()
    if not start_date:
        start_date = "2026-08-11"

    # 2. Build payload
    payload = {
        "email": email,
        "github_repo": "https://github.com/Chandrika29367/linkplease",
        "working_url": working_url,
        "loom_url": loom_url,
        "parts_completed": "A+B+C",
        "start_date": start_date
    }
    
    print("\n--- Submission Payload ---")
    print(json.dumps(payload, indent=2))
    
    confirm = input("\nDo you want to submit this now? (yes/no): ").strip().lower()
    if confirm != 'yes':
        print("Submission cancelled.")
        sys.exit(0)
        
    # 3. Send request
    print("Sending submission to PseudoGram...")
    try:
        response = httpx.post("https://pseudogram-api.onrender.com/v1/submit", json=payload, timeout=30.0)
        print(f"\nResponse Status: {response.status_code}")
        print("Response Body:")
        print(response.text)
        if response.status_code in [200, 201]:
            print("\nSUBMISSION COMPLETED SUCCESSFULLY!")
        else:
            print("\nSubmission failed. Please check the error above.")
    except Exception as e:
        print(f"\nError communicating with submission server: {e}")

if __name__ == "__main__":
    main()
