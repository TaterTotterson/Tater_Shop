from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PORTAL_DIR = ROOT / "portals"


class PortalIdentityContractTests(unittest.TestCase):
    CURRENT_EVENT_IDENTITY_MARKERS = {
        "discord": '"user_id": str(message.author.id)',
        "homekit": '"user_id": user_id',
        "irc": '"user": mask.nick',
        "matrix": '"user_id": sender',
        "meshtastic": 'origin["user_id"] = origin.get("node_id")',
        "telegram": '"user_id": sender_user_id',
        "xbmc": '"user_id": user_id',
    }

    def test_every_interactive_hydra_portal_uses_current_event_identity(self) -> None:
        for portal_id, marker in self.CURRENT_EVENT_IDENTITY_MARKERS.items():
            with self.subTest(portal=portal_id):
                source = (PORTAL_DIR / f"{portal_id}_portal.py").read_text(encoding="utf-8")
                self.assertIn(marker, source)
                self.assertIn(
                    f'resolve_admin_status(platform="{portal_id}", origin=origin',
                    source,
                )
                self.assertIn("origin=origin", source)
                self.assertIn("run_hydra_turn(", source)

    def test_discord_current_speaker_metadata_comes_from_current_message_origin(self) -> None:
        source = (PORTAL_DIR / "discord_portal.py").read_text(encoding="utf-8")
        self.assertIn(
            'current_speaker=str(origin.get("person_name") or origin.get("user") or "")',
            source,
        )
        self.assertIn('current_user_id=str(origin.get("user_id") or "")', source)
        self.assertIn("Only the latest user message belongs to this ", source)
        self.assertIn("names on older history messages belong to those older speakers", source)

    def test_noninteractive_moltbook_portal_does_not_open_a_hydra_user_turn(self) -> None:
        source = (PORTAL_DIR / "moltbook_portal.py").read_text(encoding="utf-8")
        self.assertNotIn("run_hydra_turn(", source)


if __name__ == "__main__":
    unittest.main()
