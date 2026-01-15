class PauseState:
    def __init__(self):
        self._paused = False
        self._reason = None
    
    def pause(self, reason: str):
        self._paused = True
        self._reason = reason
    
    def resume(self):
        self._paused = False
        self._reason = None
    
    def is_paused(self) -> bool:
        return self._paused
    
    def reason(self) -> str:
        return self._reason
