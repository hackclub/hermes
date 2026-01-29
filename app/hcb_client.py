import logging
from typing import Any, cast

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)


class HCBAPIError(Exception):
    def __init__(self, message: str, status_code: int | None = None):
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)


class HCBClient:
    """
    Client for HCB V4 API using OAuth2.

    Requires:
    - HCB_CLIENT_ID: OAuth2 client ID (UID)
    - HCB_CLIENT_SECRET: OAuth2 client secret
    - HCB_ACCESS_TOKEN: Initial access token (obtained via OAuth flow)
    - HCB_REFRESH_TOKEN: Refresh token (obtained via OAuth flow)

    The client automatically refreshes expired tokens using the refresh_token grant.
    """

    def __init__(self):
        self.settings = get_settings()
        self.base_url = self.settings.hcb_base_url.rstrip("/")
        self._access_token: str = self.settings.hcb_access_token
        self._refresh_token: str = self.settings.hcb_refresh_token
        self._token_url = "https://hcb.hackclub.com/api/v4/oauth/token"

    async def _refresh_access_token(self) -> str:
        """
        Refresh the access token using OAuth2 refresh_token grant.
        """
        if not self._refresh_token:
            raise HCBAPIError("No refresh token available - need to re-authorize")

        if not self.settings.hcb_client_id or not self.settings.hcb_client_secret:
            raise HCBAPIError("HCB_CLIENT_ID and HCB_CLIENT_SECRET required for token refresh")

        logger.info("Refreshing HCB access token...")

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    self._token_url,
                    data={
                        "grant_type": "refresh_token",
                        "refresh_token": self._refresh_token,
                        "client_id": self.settings.hcb_client_id,
                        "client_secret": self.settings.hcb_client_secret,
                    },
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    timeout=30.0,
                )

                if response.status_code != 200:
                    logger.error(f"HCB token refresh failed: {response.status_code} - {response.text}")
                    raise HCBAPIError(
                        f"Failed to refresh HCB access token: {response.status_code}",
                        status_code=response.status_code,
                    )

                token_data = response.json()
                self._access_token = token_data["access_token"]
                # Update refresh token if a new one is provided
                if "refresh_token" in token_data:
                    self._refresh_token = token_data["refresh_token"]
                    logger.info("HCB refresh token also updated")

                logger.info("Successfully refreshed HCB access token")
                return self._access_token

            except httpx.TimeoutException:
                logger.error("HCB token refresh timed out")
                raise HCBAPIError("HCB token refresh timeout")
            except httpx.RequestError as e:
                logger.error(f"HCB token refresh error: {e}")
                raise HCBAPIError(f"Failed to refresh HCB token: {str(e)}")

    def _get_headers(self) -> dict:
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._access_token}",
        }

    async def _request(
        self,
        method: str,
        url: str,
        retry_on_401: bool = True,
        **kwargs
    ) -> httpx.Response:
        """Make a request, refreshing token on 401 and retrying once."""
        async with httpx.AsyncClient() as client:
            response = await client.request(
                method,
                url,
                headers=self._get_headers(),
                timeout=30.0,
                **kwargs
            )

            # If unauthorized and we have refresh capability, try refreshing
            if response.status_code == 401 and retry_on_401 and self._refresh_token:
                logger.info("Access token expired (401), attempting refresh...")
                await self._refresh_access_token()

                # Retry with new token
                response = await client.request(
                    method,
                    url,
                    headers=self._get_headers(),
                    timeout=30.0,
                    **kwargs
                )

            return response

    async def get_organization(self, org_slug_or_id: str) -> dict[str, Any]:
        """
        Gets organization details by slug or ID.
        """
        url = f"{self.base_url}/organizations/{org_slug_or_id}"

        try:
            response = await self._request("GET", url)

            if response.status_code == 404:
                raise HCBAPIError(f"Organization not found: {org_slug_or_id}", status_code=404)

            if response.status_code != 200:
                logger.error(f"HCB API error getting organization: status {response.status_code}")
                raise HCBAPIError(
                    f"HCB API error: status {response.status_code}",
                    status_code=response.status_code
                )

            return cast(dict[str, Any], response.json())

        except httpx.TimeoutException:
            logger.error("HCB API timeout")
            raise HCBAPIError("HCB API timeout")
        except httpx.RequestError as e:
            logger.error(f"HCB API request error: {e}")
            raise HCBAPIError(f"Failed to connect to HCB API: {str(e)}")

    async def create_disbursement(
        self,
        source_org_slug: str,
        destination_org_slug: str,
        amount_cents: int,
        name: str
    ) -> dict[str, Any]:
        """
        Creates a disbursement (transfer) between two HCB organizations.

        Args:
            source_org_slug: Source organization slug (the org paying)
            destination_org_slug: Destination organization slug (hermes-fulfillment)
            amount_cents: Amount in cents
            name: Name/memo for the disbursement

        Returns:
            Dict with disbursement details

        Raises:
            HCBAPIError: If API call fails
        """
        url = f"{self.base_url}/organizations/{source_org_slug}/transfers"

        payload = {
            "to_organization_id": destination_org_slug,
            "amount_cents": amount_cents,
            "name": name
        }

        logger.info(f"Creating HCB disbursement: {source_org_slug} -> {destination_org_slug}, ${amount_cents/100:.2f}")

        try:
            response = await self._request("POST", url, json=payload)

            if response.status_code == 404:
                raise HCBAPIError(f"Organization not found: {source_org_slug}", status_code=404)

            if response.status_code == 403:
                raise HCBAPIError("Not authorized to create disbursement from this organization", status_code=403)

            if response.status_code not in (200, 201):
                error_msg = response.text
                logger.error(f"HCB API error creating disbursement: status {response.status_code}, body: {error_msg}")
                raise HCBAPIError(
                    f"HCB API error: status {response.status_code} - {error_msg}",
                    status_code=response.status_code
                )

            result = cast(dict[str, Any], response.json())
            logger.info(f"Disbursement created successfully: {result.get('id')}")
            return result

        except httpx.TimeoutException:
            logger.error("HCB API timeout")
            raise HCBAPIError("HCB API timeout")
        except httpx.RequestError as e:
            logger.error(f"HCB API request error: {e}")
            raise HCBAPIError(f"Failed to connect to HCB API: {str(e)}")

    async def list_transfers(
        self,
        org_slug: str,
        limit: int = 100
    ) -> list[dict[str, Any]]:
        """
        Lists recent transfers for an organization.

        Args:
            org_slug: Organization slug
            limit: Maximum number of transfers to return

        Returns:
            List of transfer objects

        Raises:
            HCBAPIError: If API call fails
        """
        url = f"{self.base_url}/organizations/{org_slug}/transfers"

        try:
            response = await self._request("GET", url, params={"per_page": limit})

            if response.status_code == 404:
                raise HCBAPIError(f"Organization not found: {org_slug}", status_code=404)

            if response.status_code != 200:
                logger.error(f"HCB API error listing transfers: status {response.status_code}")
                raise HCBAPIError(
                    f"HCB API error: status {response.status_code}",
                    status_code=response.status_code
                )

            return cast(list[dict[str, Any]], response.json())

        except httpx.TimeoutException:
            logger.error("HCB API timeout listing transfers")
            raise HCBAPIError("HCB API timeout")
        except httpx.RequestError as e:
            logger.error(f"HCB API request error: {e}")
            raise HCBAPIError(f"Failed to connect to HCB API: {str(e)}")

    async def find_transfer_by_reference(
        self,
        org_slug: str,
        reference: str,
        amount_cents: int
    ) -> dict[str, Any] | None:
        """
        Finds a transfer by checking if its memo contains the reference string.

        Args:
            org_slug: Organization slug
            reference: Reference string to search for in transfer memo
            amount_cents: Expected amount (for validation)

        Returns:
            Transfer object if found, None otherwise
        """
        transfers = await self.list_transfers(org_slug)
        for transfer in transfers:
            memo = transfer.get("memo") or transfer.get("name") or ""
            transfer_amount = transfer.get("amount_cents", 0)
            if reference in memo and transfer_amount == amount_cents:
                logger.info(f"Found matching transfer: {transfer.get('id')} for ref {reference}")
                return transfer
        return None


hcb_client = HCBClient()
