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
    """Client for HCB V4 API to create disbursements."""

    def __init__(self):
        self.settings = get_settings()
        self.base_url = self.settings.hcb_base_url.rstrip("/")
        self._access_token: str | None = None

    async def _get_access_token(self) -> str:
        """
        Get an access token using OAuth2 client credentials flow.
        Caches the token for reuse.
        """
        if self._access_token:
            return self._access_token

        token_url = f"{self.base_url}/oauth/token"

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    token_url,
                    data={
                        "grant_type": "client_credentials",
                        "client_id": self.settings.hcb_client_id,
                        "client_secret": self.settings.hcb_client_secret,
                    },
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    timeout=30.0,
                )

                if response.status_code != 200:
                    logger.error(f"HCB OAuth token request failed: {response.status_code} - {response.text}")
                    raise HCBAPIError(
                        f"Failed to obtain HCB access token: {response.status_code}",
                        status_code=response.status_code,
                    )

                token_data = response.json()
                self._access_token = token_data["access_token"]
                logger.info("Successfully obtained HCB access token")
                return self._access_token

            except httpx.TimeoutException:
                logger.error("HCB OAuth token request timed out")
                raise HCBAPIError("HCB OAuth token request timeout")
            except httpx.RequestError as e:
                logger.error(f"HCB OAuth token request error: {e}")
                raise HCBAPIError(f"Failed to connect to HCB OAuth: {str(e)}")

    async def _get_headers(self) -> dict:
        token = await self._get_access_token()
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }

    async def get_organization(self, org_slug_or_id: str) -> dict[str, Any]:
        """
        Gets organization details by slug or ID.
        Used to get the org ID from a slug.
        """
        url = f"{self.base_url}/organizations/{org_slug_or_id}"

        headers = await self._get_headers()
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    url,
                    headers=headers,
                    timeout=30.0
                )

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

        headers = await self._get_headers()
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    url,
                    headers=headers,
                    json=payload,
                    timeout=30.0
                )

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

        headers = await self._get_headers()
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    url,
                    headers=headers,
                    params={"per_page": limit},
                    timeout=30.0
                )

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
