from PIL import ImageGrab
from app.ui_app import VisualApp
app = VisualApp()
app.after(800, app._open_market_state_board_popup)
app.after(1800, lambda: ImageGrab.grab().save('/mnt/data/ui_market_state_popup_smoke.png'))
app.after(2200, app.destroy)
app.mainloop()
