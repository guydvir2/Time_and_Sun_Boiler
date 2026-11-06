from .base import Driver


class DryRunDriver(Driver):
    """Does nothing except say what it would have done. Safe for first runs and for testing."""
    name = "dryrun"

    def __init__(self, s=None):
        pass

    def check(self):
        return [{"name": "Dry run", "level": "yellow", "text": "Test mode: nothing is really switched"}]

    def start(self, minutes: int) -> bool:
        print(f"🧪 DRY RUN: would run the boiler for {minutes} minutes (nothing was switched)")
        return True
