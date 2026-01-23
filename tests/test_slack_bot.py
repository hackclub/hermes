from unittest.mock import MagicMock, patch

import pytest

from app.slack_bot import SlackBot


class TestBuildTrackingLink:
    """Tests for the _build_tracking_link method."""

    @patch("app.slack_bot.get_settings")
    @patch("app.slack_bot.WebClient")
    def test_returns_none_when_template_is_none(self, mock_web_client, mock_get_settings):
        """Test that None is returned when slack_tracking_url_template is None."""
        mock_settings = MagicMock()
        mock_settings.slack_tracking_url_template = None
        mock_get_settings.return_value = mock_settings

        bot = SlackBot()
        result = bot._build_tracking_link("12345")

        assert result is None

    @patch("app.slack_bot.get_settings")
    @patch("app.slack_bot.WebClient")
    def test_returns_formatted_url_with_simple_tracking_code(
        self, mock_web_client, mock_get_settings
    ):
        """Test that a properly formatted URL is returned for simple tracking codes."""
        mock_settings = MagicMock()
        mock_settings.slack_tracking_url_template = (
            "https://tracking.example.com/track?code={tracking_code}"
        )
        mock_get_settings.return_value = mock_settings

        bot = SlackBot()
        result = bot._build_tracking_link("ABC123")

        assert result == "https://tracking.example.com/track?code=ABC123"

    @patch("app.slack_bot.get_settings")
    @patch("app.slack_bot.WebClient")
    def test_url_encodes_special_characters(self, mock_web_client, mock_get_settings):
        """Test that special characters in tracking codes are properly URL encoded."""
        mock_settings = MagicMock()
        mock_settings.slack_tracking_url_template = (
            "https://tracking.example.com/track?code={tracking_code}"
        )
        mock_get_settings.return_value = mock_settings

        bot = SlackBot()
        
        # Test with space
        result = bot._build_tracking_link("ABC 123")
        assert result == "https://tracking.example.com/track?code=ABC%20123"
        
        # Test with ampersand
        result = bot._build_tracking_link("ABC&123")
        assert result == "https://tracking.example.com/track?code=ABC%26123"
        
        # Test with plus sign
        result = bot._build_tracking_link("ABC+123")
        assert result == "https://tracking.example.com/track?code=ABC%2B123"
        
        # Test with equals sign
        result = bot._build_tracking_link("ABC=123")
        assert result == "https://tracking.example.com/track?code=ABC%3D123"

    @patch("app.slack_bot.get_settings")
    @patch("app.slack_bot.WebClient")
    def test_url_encodes_unicode_characters(self, mock_web_client, mock_get_settings):
        """Test that Unicode characters in tracking codes are properly URL encoded."""
        mock_settings = MagicMock()
        mock_settings.slack_tracking_url_template = (
            "https://tracking.example.com/track?code={tracking_code}"
        )
        mock_get_settings.return_value = mock_settings

        bot = SlackBot()
        result = bot._build_tracking_link("ABC™123")

        assert result == "https://tracking.example.com/track?code=ABC%E2%84%A2123"

    @patch("app.slack_bot.get_settings")
    @patch("app.slack_bot.WebClient")
    def test_handles_forward_slash_in_tracking_code(self, mock_web_client, mock_get_settings):
        """Test that forward slashes in tracking codes are handled (not encoded by default)."""
        mock_settings = MagicMock()
        mock_settings.slack_tracking_url_template = (
            "https://tracking.example.com/track?code={tracking_code}"
        )
        mock_get_settings.return_value = mock_settings

        bot = SlackBot()
        result = bot._build_tracking_link("ABC/123")

        # Forward slash is considered safe by quote() and not encoded by default
        assert result == "https://tracking.example.com/track?code=ABC/123"

    @patch("app.slack_bot.get_settings")
    @patch("app.slack_bot.WebClient")
    def test_handles_invalid_template_format(self, mock_web_client, mock_get_settings):
        """Test that KeyError is handled gracefully for invalid templates."""
        mock_settings = MagicMock()
        # Template without {tracking_code} placeholder
        mock_settings.slack_tracking_url_template = (
            "https://tracking.example.com/track?code={invalid_placeholder}"
        )
        mock_get_settings.return_value = mock_settings

        bot = SlackBot()
        result = bot._build_tracking_link("ABC123")

        assert result is None

    @patch("app.slack_bot.get_settings")
    @patch("app.slack_bot.WebClient")
    def test_handles_multiple_special_characters(self, mock_web_client, mock_get_settings):
        """Test that tracking codes with multiple special characters are encoded correctly."""
        mock_settings = MagicMock()
        mock_settings.slack_tracking_url_template = (
            "https://tracking.example.com/track?code={tracking_code}"
        )
        mock_get_settings.return_value = mock_settings

        bot = SlackBot()
        result = bot._build_tracking_link("ABC-123_XYZ!@#")

        # Dash, underscore are safe, exclamation and at sign need encoding
        assert "ABC-123_XYZ" in result
        assert "%21" in result  # !
        assert "%40" in result  # @
        assert "%23" in result  # #

    @patch("app.slack_bot.get_settings")
    @patch("app.slack_bot.WebClient")
    def test_handles_percent_signs_in_tracking_code(self, mock_web_client, mock_get_settings):
        """Test that percent signs in tracking codes are properly double-encoded."""
        mock_settings = MagicMock()
        mock_settings.slack_tracking_url_template = (
            "https://tracking.example.com/track?code={tracking_code}"
        )
        mock_get_settings.return_value = mock_settings

        bot = SlackBot()
        result = bot._build_tracking_link("ABC%123")

        assert result == "https://tracking.example.com/track?code=ABC%25123"
