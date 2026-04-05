    Kufar Parser
Асинхронный парсер объявлений с площадки Kufar.
  Как запустить:
Скачай проект: git clone https://github.com/Maxicus0/ParserKufar и перейди в папку cd ParserKufar
Поставь библиотеки: pip install -r requirements.txt
Настрой конфиг: Переименуй файл .env.exemple в .env и впиши свои значения (запрос, количество страниц, задержки).
Запускай: python app.py
  Что внутри конфига (.env):
QUERY — что ищем (например, автозапчасти).
MAX_PAGES — сколько страниц парсить.
MIN_DELAY / MAX_DELAY — пауза между запросами (анти-бан).
CONCURRENCY — количество потоков.
