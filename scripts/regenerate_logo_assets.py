#!/usr/bin/env python3
# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

r"""Regenerate this repo's brand assets from one master logo.

The master is committed as ``assets/logo-master.png``: the shield mark on
transparency, at the highest resolution we have.  Everything else is derived,
so a new master is a one-command reroll:

    uv run --with pillow python scripts/regenerate_logo_assets.py

Pass ``--master PATH`` to cut the assets from a different source, which also
replaces the committed master.

The master lives at the repo root rather than in ``docs/public``, which Astro
copies verbatim into the deployed site — the source artwork is a build input,
not something to serve to every visitor.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from PIL import Image

_REPO = Path(__file__).resolve().parent.parent
_MASTER = _REPO / "assets" / "logo-master.png"
_PUBLIC = _REPO / "docs" / "public"

# Starlight renders the mark in the site header at ~40 CSS px and the README
# links it at full width on GitHub. 600 is the width the sibling ports serve
# for the same job, so the fleet ships one size rather than five.
_HERO_WIDTH = 600


def _scaled_to_width(logo: Image.Image, width: int) -> Image.Image:
    """Resample *logo* to *width*, preserving aspect ratio."""
    height = round(logo.height * width / logo.width)
    return logo.resize((width, height), Image.LANCZOS)


def _cropped_mark(master: Image.Image) -> Image.Image:
    """Return *master* as RGBA, cropped to the mark's bounding box.

    Args:
        master: The logo, on transparency.

    Returns:
        An RGBA image with no dead margin, so every derived size is tight and
        predictable.

    Raises:
        SystemExit: If the master is fully opaque, which means it is the mark on
            a background rather than on transparency — scaling that would bake
            a white slab into every asset.

    """
    image = master.convert("RGBA")
    if image.getchannel("A").getextrema() == (255, 255):
        raise SystemExit(f"{_MASTER} has no transparency: it is the mark on a background, not a keyed master")
    bbox = image.getbbox()
    return image.crop(bbox) if bbox else image


def main() -> None:
    """Cut every derived asset from the master and report what was written."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--master",
        type=Path,
        default=None,
        help="Source logo on transparency. Replaces the committed master when given.",
    )
    args = parser.parse_args()

    if args.master is not None:
        _MASTER.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(args.master, _MASTER)
    if not _MASTER.exists():
        parser.error(f"no master at {_MASTER}; pass --master PATH")

    logo = _cropped_mark(Image.open(_MASTER))
    print(f"master {Image.open(_MASTER).size} -> mark {logo.size}")

    hero = _PUBLIC / "logo-hero.png"
    _scaled_to_width(logo, _HERO_WIDTH).save(hero)
    print(f"  {hero.relative_to(_REPO)!s:34} {Image.open(hero).size!s:12} {hero.stat().st_size // 1024:>4} KiB")


if __name__ == "__main__":
    main()
