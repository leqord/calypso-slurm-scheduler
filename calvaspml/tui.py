from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Button, Static

class MyApp(App):
    def compose(self) -> ComposeResult:
        # Выводим заголовок вверху экрана
        yield Header()
        # Выводим текст с инструкцией
        yield Static("Нажми кнопку, чтобы увеличить счётчик:", id="prompt")
        # Кнопка, по нажатию на которую будет увеличиваться счётчик
        yield Button("Нажми меня", id="increment")
        # Место для отображения текущего значения счётчика
        yield Static("Счётчик: 0", id="counter")
        # Выводим подвал внизу экрана
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        # Обработчик события нажатия кнопки
        if event.button.id == "increment":
            # Ищем виджет со счётчиком по его id
            counter_widget = self.query_one("#counter", Static)
            # Извлекаем текущее значение счётчика
            try:
                current_text = counter_widget.renderable
                # Если renderable уже строка, пробуем взять число после "Счётчик:"
                current_count = int(str(current_text).split(":")[1].strip())
            except Exception:
                current_count = 0
            # Увеличиваем счётчик
            current_count += 1
            # Обновляем текст виджета
            counter_widget.update(f"Счётчик: {current_count}")

if __name__ == "__main__":
    MyApp().run()
