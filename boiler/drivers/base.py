"""A driver turns 'run the boiler for N minutes' into a real action. That is its whole job."""


class Driver:
    name = "base"
    supports_stop = False     # can this driver switch the boiler off early?

    def problems(self) -> list:
        """Configuration problems that make this driver unusable (empty list = OK)."""
        return []

    def start(self, minutes: int) -> bool:
        """Run the boiler for `minutes` starting now. Return True only if it was accepted."""
        raise NotImplementedError

    def stop(self) -> bool:
        """Switch the boiler off now. Drivers that cannot do this leave supports_stop False."""
        return False
