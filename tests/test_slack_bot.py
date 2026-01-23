import pytest
from app.slack_bot import SlackBot


class TestSlackBot:
    """Tests for SlackBot class."""

    def test_escape_slack_markdown_with_special_characters(self):
        """Test that special Slack markdown characters are properly escaped."""
        slack_bot = SlackBot()
        
        # Test escaping pipe character
        result = slack_bot._escape_slack_markdown("ABC|123")
        assert result == "ABC&#124;123"
        assert "|" not in result
        
        # Test escaping less than
        result = slack_bot._escape_slack_markdown("ABC<123")
        assert result == "ABC&lt;123"
        assert "<" not in result
        
        # Test escaping greater than
        result = slack_bot._escape_slack_markdown("ABC>123")
        assert result == "ABC&gt;123"
        assert ">" not in result
        
        # Test escaping ampersand
        result = slack_bot._escape_slack_markdown("ABC&123")
        assert result == "ABC&amp;123"
        
        # Test escaping multiple special characters
        result = slack_bot._escape_slack_markdown("ABC|<>123")
        assert result == "ABC&#124;&lt;&gt;123"
        assert "|" not in result
        assert "<" not in result
        assert ">" not in result
        
        # Test escaping with ampersand and other characters (order matters)
        result = slack_bot._escape_slack_markdown("A&B|C<D>E")
        assert result == "A&amp;B&#124;C&lt;D&gt;E"
        
    def test_escape_slack_markdown_with_no_special_characters(self):
        """Test that text without special characters is unchanged."""
        slack_bot = SlackBot()
        
        result = slack_bot._escape_slack_markdown("ABC123XYZ")
        assert result == "ABC123XYZ"
        
        result = slack_bot._escape_slack_markdown("tracking-code-123")
        assert result == "tracking-code-123"
        
        result = slack_bot._escape_slack_markdown("1Z999AA10123456784")
        assert result == "1Z999AA10123456784"
        
    def test_escape_slack_markdown_with_empty_string(self):
        """Test that empty string is handled correctly."""
        slack_bot = SlackBot()
        
        result = slack_bot._escape_slack_markdown("")
        assert result == ""
        
    def test_escape_slack_markdown_realistic_tracking_codes(self):
        """Test with realistic tracking code examples that might contain special characters."""
        slack_bot = SlackBot()
        
        # UPS tracking with brackets (though uncommon in practice)
        result = slack_bot._escape_slack_markdown("1Z<TEST>123")
        assert "<" not in result
        assert ">" not in result
        assert "&lt;" in result
        assert "&gt;" in result
        
        # Theoretical tracking code with pipes or special formatting
        result = slack_bot._escape_slack_markdown("TRACK|ABC|123")
        assert "|" not in result
        assert "&#124;" in result
