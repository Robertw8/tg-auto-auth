# Исследование размещения в MRKT Mini App

Исторический снимок от 2026-08-31 основан на публичных рабочих bundles MRKT и одном запросе авторизации/инвентаря только для чтения. Endpoint размещения не вызывался. Имена bundles с хешем содержимого могут измениться при любом развёртывании MRKT.

## Обнаруженный поток

### 1. Запуск Mini App

`MiniAppService` разрешает `@mrkt` через авторизованную пользовательскую session Telethon и вызывает Telegram `messages.requestMainWebView`. Основной Mini App открывает `https://cdn.tgmrkt.io/index.html`; `tgWebAppData` остаётся внутри `MiniAppLaunchResult`, не логируется и не возвращается управляющему боту.

Frontend `config.js` задаёт `window.API_URL=api.tgmrkt.io`, поэтому рабочий origin API:

```text
https://api.tgmrkt.io
```

### 2. Авторизация

Перед инициализацией frontend отправляет:

```http
POST /api/v1/auth
Content-Type: application/json
Origin: https://cdn.tgmrkt.io
Referer: https://cdn.tgmrkt.io/
```

Имена JSON-полей:

```json
{
  "data": "<Telegram initData>",
  "photo": "<optional Telegram photo URL>",
  "appId": null
}
```

`data` содержит raw signed Telegram Mini App initData. Ответ содержит MRKT `token`, а сервер также устанавливает cookie `access_token`. Наблюдавшаяся cookie имела атрибуты `Secure`, `HttpOnly`, `Domain`, `Path`, `Expires` и `SameSite`.

Frontend хранит token в памяти и отправляет его напрямую в `Authorization` без префикса `Bearer`. Axios использует `withCredentials: true`, поэтому API того же сайта получает cookie. Отдельная WebSocket-авторизация не относится к HTTP-мутации Storage/размещения.

### 3. Загрузка Storage

Вкладка непроданных подарков вызывает:

```http
POST /api/v1/gifts
Authorization: <MRKT auth response token>
Content-Type: application/json
Origin: https://cdn.tgmrkt.io
Referer: https://cdn.tgmrkt.io/
```

Базовые поля запроса: `isListed`, `count`, `cursor` и текущие фильтры. Начальный запрос использует `isListed: false`, `count: 20`, пустой cursor, `ordering: "None"`, `lowToHigh: false`, `query: null`, пустые массивы collection/model/backdrop/symbol и допускающие null поля остальных фильтров.

Успешная структура ответа:

```json
{
  "cursor": "<opaque pagination cursor>",
  "gifts": [],
  "total": 0
}
```

Размещение использует серверное значение `gift.id` в `ids: [gift.id]`. Идентификатор считается opaque и не преобразуется. Во время проверки только для чтения Storage был пуст, поэтому конкретный тип в протоколе тогда определить не удалось.

### 4. Цена

Локальное окно **List/Place** принимает цифры, один десятичный разделитель и максимум два знака после него. Запятая заменяется точкой, затем TON-библиотека выполняет `toNano(priceString)`.

```text
price = точная Decimal-сумма TON * 1_000_000_000
```

Например, `1.25 TON` превращается в целое число `1250000000`. Это цена продавца. Публичный `salePrice` включает подтверждённую комиссию покупателя 2% и используется frontend в `/api/v1/gifts/buy`.

Минимум и максимум приходят из `/api/v1/configs`. Ограничения площадки остаются обязательными.

### 5. Подтверждение размещения

Первая кнопка формы цены только сохраняет целое значение nanoTON в состоянии frontend и открывает `LIST_GIFT_CONFIRM`. Серверный запрос подготовки отсутствует.

Финальное подтверждение отправляет единственную мутацию:

```http
POST /api/v1/gifts/sale
Authorization: <MRKT auth response token>
Content-Type: application/json
Origin: https://cdn.tgmrkt.io
Referer: https://cdn.tgmrkt.io/

{"ids": ["<opaque gift.id>"], "price": 1250000000}
```

Frontend ожидает объект ответа с массивом `ids`: непустой массив означает успех, пустой — ожидание/cooldown. Второго завершающего запроса нет.

Связанные мутации:

- `POST /api/v1/gifts/sale/change-price` с `ids`, `newPrice`;
- `POST /api/v1/gifts/sale/cancel` с `ids`.

Они не вызывались во время исследования.

## Нужна ли автоматизация браузера

Размещение использует обычный JSON API. Свежий запуск основного Mini App, `/api/v1/auth`, token/cookie в памяти, opaque gift ID и один SALE технически достаточны. Автоматизация DOM/WebView после получения initData не требуется.

Вывод основан на статическом анализе frontend и чтении auth/inventory. Серверные комиссии, cooldown, блокировки передачи, collateral, CAPTCHA и другие ограничения нельзя обходить; после нового развёртывания контракт нужно проверять повторно.

## Безопасная диагностическая проверка

Проверка использует `MiniAppLaunchResult`, штатно авторизуется и читает только непроданный инвентарь. Список разрешённых endpoints содержит лишь `/api/v1/auth` и `/api/v1/gifts`, поэтому SALE вызвать невозможно.

```bash
source .venv/bin/activate
python scripts/trace_mrkt_flow.py \
  --account-id <database-account-id> \
  --owner-telegram-id <control-bot-user-id>
```

Проверка выводит только origins, HTTP-статусы, имена полей, имена/атрибуты cookies, число объектов и классификацию типа ID. Исходные initData, Authorization, cookies, item IDs, тела ответов, session-файлы и учётные данные Telegram не печатаются.
