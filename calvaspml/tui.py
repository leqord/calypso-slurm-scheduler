from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Button, Static

class MyApp(App):
    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("Нажми кнопку, чтобы увеличить счётчик:", id="prompt")
        yield Button("Нажми меня", id="increment")
        yield Static("Счётчик: 0", id="counter")
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "increment":
            counter_widget = self.query_one("#counter", Static)
            try:
                current_text = counter_widget.renderable
                current_count = int(str(current_text).split(":")[1].strip())
            except Exception:
                current_count = 0
            current_count += 1
            counter_widget.update(f"Счётчик: {current_count}")

if __name__ == "__main__":
    MyApp().run()
