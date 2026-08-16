from typing import Dict, Any, Tuple
import httpx
from app.config import settings

class PseudoGramException(Exception):
    """Base exception for PseudoGram API errors."""
    pass

class RateLimitException(PseudoGramException):
    """429 Rate Limit error."""
    def __init__(self, retry_after: int):
        self.retry_after = retry_after
        super().__init__(f"Rate limited. Retry after {retry_after} seconds.")

class InternalErrorException(PseudoGramException):
    """500 Internal Server error."""
    pass

class InvalidRequestException(PseudoGramException):
    """400 Invalid Request error."""
    def __init__(self, detail: str):
        self.detail = detail
        super().__init__(f"Invalid request: {detail}")

class PseudoGramClient:
    def __init__(self):
        self.base_url = settings.PSEUDOGRAM_BASE_URL.rstrip("/")
        self.headers = {
            "X-API-Key": settings.PSEUDOGRAM_API_KEY,
            "Content-Type": "application/json"
        }

    async def send_dm(
        self,
        recipient_user_id: str,
        message: str,
        comment_id: str,
        idempotency_key: str
    ) -> Tuple[str, str]:
        """
        Sends a DM request to PseudoGram POST /v1/dm/send.
        Returns (dm_id, status).
        """
        url = f"{self.base_url}/v1/dm/send"
        payload = {
            "recipient_user_id": recipient_user_id,
            "message": message,
            "comment_id": comment_id
        }
        
        headers = {**self.headers, "Idempotency-Key": idempotency_key}
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                response = await client.post(url, json=payload, headers=headers)
            except httpx.RequestError as e:
                # Treat network/timeout issues as transient 500-like errors
                raise InternalErrorException(f"Network error: {str(e)}")
            
            if response.status_code == 429:
                retry_after_str = response.headers.get("Retry-After", "30")
                try:
                    retry_after = int(retry_after_str)
                except ValueError:
                    retry_after = 30
                raise RateLimitException(retry_after)
            
            elif response.status_code == 500:
                raise InternalErrorException("PseudoGram internal server error")
            
            elif response.status_code == 400:
                detail = "Bad request"
                try:
                    detail = response.json().get("detail", response.json().get("error", "Bad request"))
                except Exception:
                    pass
                raise InvalidRequestException(detail)
            
            elif response.status_code == 202:
                try:
                    data = response.json()
                    return data["dm_id"], data["status"]
                except (KeyError, ValueError) as e:
                    raise InternalErrorException(f"Malformed response: {str(e)}")
            
            else:
                raise PseudoGramException(f"Unexpected status code: {response.status_code}")

    async def get_dm_status(self, dm_id: str) -> str:
        """
        Checks status of DM via GET /v1/dm/{dm_id}.
        Returns status (e.g. 'queued', 'delivered', 'failed').
        """
        url = f"{self.base_url}/v1/dm/{dm_id}"
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                response = await client.get(url, headers=self.headers)
            except httpx.RequestError as e:
                raise InternalErrorException(f"Network error checking status: {str(e)}")
            
            if response.status_code == 429:
                # GET status does not count toward rate limit, but mock API can still throttle if overloaded
                raise RateLimitException(30)
            elif response.status_code == 500:
                raise InternalErrorException("PseudoGram internal server error")
            elif response.status_code == 400 or response.status_code == 404:
                raise InvalidRequestException(f"Invalid dm_id or not found: {dm_id}")
            elif response.status_code == 200:
                try:
                    data = response.json()
                    return data["status"]
                except (KeyError, ValueError) as e:
                    raise InternalErrorException(f"Malformed status response: {str(e)}")
            else:
                raise PseudoGramException(f"Unexpected status code on GET: {response.status_code}")
