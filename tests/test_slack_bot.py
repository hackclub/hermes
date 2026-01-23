from app.slack_bot import SlackBot


class TestSlackBot:
    """Tests for SlackBot utility methods."""

    def test_escape_slack_mrkdwn_basic(self):
        """Test basic escaping of Slack mrkdwn special characters."""
        bot = SlackBot()

        # Test ampersand
        assert bot._escape_slack_mrkdwn("A&B") == "A&amp;B"

        # Test less than
        assert bot._escape_slack_mrkdwn("A<B") == "A&lt;B"

        # Test greater than
        assert bot._escape_slack_mrkdwn("A>B") == "A&gt;B"

    def test_escape_slack_mrkdwn_multiple_chars(self):
        """Test escaping multiple special characters."""
        bot = SlackBot()

        # Test multiple characters
        assert bot._escape_slack_mrkdwn("<script>alert('XSS')</script>") == "&lt;script&gt;alert('XSS')&lt;/script&gt;"
        assert bot._escape_slack_mrkdwn("A&B<C>D") == "A&amp;B&lt;C&gt;D"

    def test_escape_slack_mrkdwn_already_escaped(self):
        """Test that already escaped characters get double-escaped."""
        bot = SlackBot()

        # This is intentional - we escape what's given to prevent injection
        assert bot._escape_slack_mrkdwn("&amp;") == "&amp;amp;"

    def test_escape_slack_mrkdwn_normal_text(self):
        """Test that normal text without special characters is unchanged."""
        bot = SlackBot()

        assert bot._escape_slack_mrkdwn("TRACK123") == "TRACK123"
        assert bot._escape_slack_mrkdwn("1Z999AA10123456784") == "1Z999AA10123456784"
        assert bot._escape_slack_mrkdwn("Tracking_Code-123") == "Tracking_Code-123"

    def test_build_tracking_link_with_special_chars(self, monkeypatch):
        """Test that tracking codes with special characters are properly URL-encoded."""
        bot = SlackBot()

        # Set up a mock template
        monkeypatch.setattr(
            bot.settings,
            "slack_tracking_url_template",
            "https://track.example.com/track?code={tracking_code}"
        )

        # Test with special characters that need URL encoding
        link = bot._build_tracking_link("TRACK&123")
        assert link == "https://track.example.com/track?code=TRACK%26123"

        link = bot._build_tracking_link("TRACK<>123")
        assert link == "https://track.example.com/track?code=TRACK%3C%3E123"

    def test_build_tracking_link_no_template(self, monkeypatch):
        """Test that None is returned when no template is configured."""
        bot = SlackBot()
        monkeypatch.setattr(bot.settings, "slack_tracking_url_template", None)

        assert bot._build_tracking_link("TRACK123") is None

    def test_build_tracking_link_invalid_scheme(self, monkeypatch):
        """Test that links with invalid schemes are rejected."""
        bot = SlackBot()

        # Test with javascript: scheme (security risk)
        monkeypatch.setattr(
            bot.settings,
            "slack_tracking_url_template",
            "javascript:alert('{tracking_code}')"
        )

        link = bot._build_tracking_link("test")
        assert link is None

    def test_build_tracking_link_valid_schemes(self, monkeypatch):
        """Test that http and https schemes are accepted."""
        bot = SlackBot()

        # Test http
        monkeypatch.setattr(
            bot.settings,
            "slack_tracking_url_template",
            "http://track.example.com/{tracking_code}"
        )
        link = bot._build_tracking_link("TEST123")
        assert link == "http://track.example.com/TEST123"

        # Test https
        monkeypatch.setattr(
            bot.settings,
            "slack_tracking_url_template",
            "https://track.example.com/{tracking_code}"
        )
        link = bot._build_tracking_link("TEST123")
        assert link == "https://track.example.com/TEST123"
