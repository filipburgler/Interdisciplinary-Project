import time

from ..config import PANELS_DIR
from ..panels.io_utils import ask_user, safely_delete_dir
from ..panels.panel_core import build_panels


def main():
    start_global = time.time()

    prompt = "Delete all existing panels?"
    choice = ask_user(prompt, default="y")

    if choice == "y":
        safely_delete_dir(PANELS_DIR)
    else:
        print("Skipping...")

    PANELS_DIR.mkdir(parents=True, exist_ok=True)

    build_panels(
        event_names=None,
        affected=["regional", "global", "bilateral"],
        mentioned=["EU"],
        threshold=0.5,
    )

    print(f"Complete runtime: {time.time() - start_global}")
    print()


if __name__ == "__main__":
    main()