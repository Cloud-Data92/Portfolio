"""
Alert system for sending notifications via Telegram and Discord.
Enhanced with chart images, inline keyboards, and alert history.
"""

import io
import json
import httpx
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, field

from .events import bus, ALERT_SENT
from .history import history


def _generate_price_chart(price_history: list) -> Optional[bytes]:
    """Generate a small PNG chart of recent price spreads. Returns PNG bytes or None."""
    if len(price_history) < 3:
        return None
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates

        times = [p.get("timestamp", 0) for p in price_history]
        ups = [p.get("up_ask", 0) for p in price_history]
        downs = [p.get("down_ask", 0) for p in price_history]
        totals = [p.get("total_ask", 0) for p in price_history]

        fig, ax = plt.subplots(figsize=(6, 2.5), dpi=100)
        fig.patch.set_facecolor("#12121f")
        ax.set_facecolor("#12121f")

        ax.plot(times, ups, color="#00d4aa", linewidth=1.5, label="UP")
        ax.plot(times, downs, color="#ff4757", linewidth=1.5, label="DOWN")
        ax.plot(times, totals, color="#4d7cfe", linewidth=1.5, linestyle="--", label="Total")

        ax.tick_params(colors="#8888a0", labelsize=7)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["bottom"].set_color("#2a2a40")
        ax.spines["left"].set_color("#2a2a40")
        ax.legend(fontsize=7, loc="upper left", facecolor="#12121f",
                  edgecolor="#2a2a40", labelcolor="#e8e8f0")

        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight",
                    facecolor="#12121f", edgecolor="none")
        plt.close(fig)
        buf.seek(0)
        return buf.read()
    except Exception:
        return None


@dataclass
class AlertManager:
    """Manages sending alerts to Telegram and Discord."""

    telegram_token: str
    telegram_chat_id: str
    discord_webhook: str
    dashboard_url: str = "http://127.0.0.1:8080"

    def __post_init__(self):
        self._client = httpx.Client(timeout=10.0)
        self.telegram_enabled = bool(self.telegram_token and self.telegram_chat_id)
        self.discord_enabled = bool(self.discord_webhook)

    # ---- Low-level senders ----

    def send_telegram(self, message: str, parse_mode: str = "HTML",
                      reply_markup: Optional[dict] = None) -> bool:
        """Send a message via Telegram bot."""
        if not self.telegram_enabled:
            return False

        try:
            url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
            payload = {
                "chat_id": self.telegram_chat_id,
                "text": message,
                "parse_mode": parse_mode,
            }
            if reply_markup:
                payload["reply_markup"] = json.dumps(reply_markup)

            response = self._client.post(url, json=payload)
            success = response.status_code == 200
            self._record_alert("telegram", "message", message, success)
            return success
        except Exception as e:
            print(f"Telegram error: {e}")
            self._record_alert("telegram", "message", message, False)
            return False

    def send_discord(self, message: str, embeds: Optional[list] = None,
                     image_bytes: Optional[bytes] = None,
                     filename: str = "chart.png") -> bool:
        """Send a message via Discord webhook, optionally with an image."""
        if not self.discord_enabled:
            return False

        try:
            if image_bytes:
                # Multipart upload with image
                files = {"file": (filename, image_bytes, "image/png")}
                payload_json = {"content": message}
                if embeds:
                    # Attach image to first embed
                    embeds[0]["image"] = {"url": f"attachment://{filename}"}
                    payload_json["embeds"] = embeds

                response = self._client.post(
                    self.discord_webhook,
                    data={"payload_json": json.dumps(payload_json)},
                    files=files,
                )
            else:
                payload = {"content": message}
                if embeds:
                    payload["embeds"] = embeds
                response = self._client.post(self.discord_webhook, json=payload)

            success = response.status_code in [200, 204]
            self._record_alert("discord", "message", message, success)
            return success
        except Exception as e:
            print(f"Discord error: {e}")
            self._record_alert("discord", "message", message, False)
            return False

    def _record_alert(self, channel: str, alert_type: str, message: str, success: bool):
        """Record alert in history and emit event."""
        try:
            history.add_alert(channel, alert_type, message[:500], success)
        except Exception:
            pass
        bus.publish(ALERT_SENT, {
            "channel": channel,
            "type": alert_type,
            "success": success,
        })

    def _dashboard_keyboard(self) -> dict:
        """Telegram inline keyboard with dashboard link."""
        return {
            "inline_keyboard": [[
                {"text": "Open Dashboard", "url": self.dashboard_url},
            ]]
        }

    # ---- Domain alert methods ----

    def send_opportunity_alert(
        self,
        market: str,
        up_price: float,
        down_price: float,
        total_cost: float,
        profit_pct: float,
        time_remaining: int,
        dry_run: bool = True,
        price_history: Optional[list] = None,
    ):
        """Send an arbitrage opportunity alert to all configured channels."""

        mode = "🔸 DRY RUN" if dry_run else "🔴 LIVE"
        profit_cents = (1 - total_cost) * 100

        # Telegram message with inline keyboard
        telegram_msg = f"""🎯 <b>ARBITRAGE OPPORTUNITY</b> {mode}

<b>Market:</b> {market}
<b>Time Left:</b> {time_remaining // 60}m {time_remaining % 60}s

📈 UP:   ${up_price:.4f}
📉 DOWN: ${down_price:.4f}
━━━━━━━━━━━━━━━
💰 Total: ${total_cost:.4f}
✨ Profit: {profit_cents:.2f}¢ ({profit_pct:.2f}%)"""

        self.send_telegram(telegram_msg, reply_markup=self._dashboard_keyboard())

        # Discord embed with chart image
        discord_embed = {
            "title": f"🎯 Arbitrage Opportunity {mode}",
            "color": 0x00ff00 if not dry_run else 0xffaa00,
            "fields": [
                {"name": "Market", "value": market, "inline": False},
                {"name": "📈 UP", "value": f"${up_price:.4f}", "inline": True},
                {"name": "📉 DOWN", "value": f"${down_price:.4f}", "inline": True},
                {"name": "💰 Total", "value": f"${total_cost:.4f}", "inline": True},
                {"name": "✨ Profit", "value": f"{profit_cents:.2f}¢ ({profit_pct:.2f}%)", "inline": True},
                {"name": "⏱ Time Left", "value": f"{time_remaining // 60}m {time_remaining % 60}s", "inline": True},
            ],
            "footer": {"text": "BTC Arb Bot v2.0"},
            "timestamp": datetime.utcnow().isoformat(),
        }

        chart_bytes = None
        if price_history:
            chart_bytes = _generate_price_chart(price_history)

        self.send_discord("", embeds=[discord_embed], image_bytes=chart_bytes)

    def send_trade_alert(
        self,
        market: str,
        action: str,
        up_price: float,
        down_price: float,
        shares: int,
        total_invested: float,
        expected_profit: float,
        dry_run: bool = True
    ):
        """Send a trade execution alert."""

        mode = "🔸 SIMULATED" if dry_run else "✅ EXECUTED"

        telegram_msg = f"""{mode} <b>TRADE</b>

<b>Action:</b> {action}
<b>Market:</b> {market}
<b>Shares:</b> {shares} each side

📈 UP @ ${up_price:.4f}
📉 DOWN @ ${down_price:.4f}

💵 Invested: ${total_invested:.2f}
💰 Expected Profit: ${expected_profit:.4f}"""

        self.send_telegram(telegram_msg, reply_markup=self._dashboard_keyboard())

        # Enhanced Discord embed for trades
        trade_embed = {
            "title": f"{mode} Trade",
            "color": 0x00d4aa if not dry_run else 0xffc107,
            "fields": [
                {"name": "Market", "value": market, "inline": False},
                {"name": "Action", "value": action, "inline": True},
                {"name": "Shares", "value": f"{shares}/side", "inline": True},
                {"name": "📈 UP", "value": f"${up_price:.4f}", "inline": True},
                {"name": "📉 DOWN", "value": f"${down_price:.4f}", "inline": True},
                {"name": "💵 Invested", "value": f"${total_invested:.2f}", "inline": True},
                {"name": "💰 Profit", "value": f"${expected_profit:.4f}", "inline": True},
            ],
            "footer": {"text": "BTC Arb Bot v2.0"},
            "timestamp": datetime.utcnow().isoformat(),
        }
        self.send_discord("", embeds=[trade_embed])

    def send_error_alert(self, error_msg: str):
        """Send an error alert."""
        telegram_msg = f"⚠️ <b>BOT ERROR</b>\n\n{error_msg}"
        self.send_telegram(telegram_msg)

        error_embed = {
            "title": "⚠️ Bot Error",
            "description": error_msg[:2000],
            "color": 0xff4757,
            "footer": {"text": "BTC Arb Bot v2.0"},
            "timestamp": datetime.utcnow().isoformat(),
        }
        self.send_discord("", embeds=[error_embed])

    def send_startup_alert(self, config_summary: str):
        """Send a bot startup notification."""
        telegram_msg = f"🚀 <b>Bot Started</b>\n\n<pre>{config_summary}</pre>"
        self.send_telegram(telegram_msg, reply_markup=self._dashboard_keyboard())

        startup_embed = {
            "title": "🚀 Bot Started",
            "description": f"```{config_summary}```",
            "color": 0x4d7cfe,
            "footer": {"text": "BTC Arb Bot v2.0"},
            "timestamp": datetime.utcnow().isoformat(),
        }
        self.send_discord("", embeds=[startup_embed])

    def close(self):
        """Close HTTP client."""
        self._client.close()


def create_alert_manager(config) -> AlertManager:
    """Create AlertManager from config."""
    dashboard_url = f"http://{config.dashboard_host}:{config.dashboard_port}"
    return AlertManager(
        telegram_token=config.telegram_token,
        telegram_chat_id=config.telegram_chat_id,
        discord_webhook=config.discord_webhook,
        dashboard_url=dashboard_url,
    )
