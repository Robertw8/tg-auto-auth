# Интеграция с Tonnel Network Mini App

Документ объединяет результаты статического исследования официального Mini App от 2026-09-10/11 и последующих контролируемых live-проверок. Он описывает текущий рабочий маршрут `BUY_OFFER`, а также исторически исследованные альтернативы. Секретные значения, тела ответов с учётными данными и Telegram-сессии здесь не приводятся.

## Текущий результат

В приложении подтверждены три независимых семейства возможностей Tonnel:

- `BUY_OFFER` — текущий штатный маршрут. N1 создаёт точный оффер на один непроданный custodial-подарок N2, после чего N2 принимает именно этот opaque `offer_id`.
- `MARKET_SALE` — альтернативный публичный маршрут через размещение подарка и его покупку. Он сохранён в коде, но не является штатным batch-режимом.
- `DIRECT_RECIPIENT` — исторически исследованный Business-only маршрут через возврат управляемого подарка конкретному получателю. Он требует официального Telegram Business connection и не является текущим режимом.

Штатная конфигурация:

```dotenv
TONNEL_TRANSFER_MODE=BUY_OFFER
TONNEL_API_ORIGIN=https://gifts.coffin.meme
TONNEL_AUTO_OFFER_AMOUNT=2
TONNEL_OFFER_ACCEPT_DELAY_MS=5000
TONNEL_OWNERSHIP_VERIFY_TIMEOUT_MS=15000
MAX_TONNEL_CONCURRENT_JOBS=5
```

Текущий интерфейс Tonnel — one-click batch: приложение загружает все подходящие подарки N2, создаёт отдельное дочернее задание на каждый подарок и использует фиксированную сумму `TONNEL_AUTO_OFFER_AMOUNT`. Пользователь не выбирает подарок и не вводит сумму вручную.

## Запуск Mini App и аутентификация

- бот Telegram: `@Tonnel_Network_bot`
- короткое имя: `gift`
- метод MTProto: `messages.requestAppWebView`
- peer: авторизованный пользователь (`InputPeerSelf`)
- платформа: `android`
- интерфейс: `https://marketplace.tonnel.network/gift`

WebView URL содержит `tgWebAppData` во fragment. Frontend один раз обменивает это значение:

```http
POST https://gifts.coffin.meme/api/auth/telegram/miniapp/session
Content-Type: application/json
Origin: https://marketplace.tonnel.network

{
  "initData": "<Telegram WebApp initData>",
  "origin": "https://marketplace.tonnel.network"
}
```

Ответ содержит поля `status`, `token` и `user`. Последующие запросы передают Tonnel session token в JSON-поле `authData`; заголовок `Authorization` не используется. Обязательного cookie exchange не обнаружено. `initData`, `authData`, cookies и Telegram session material остаются только в памяти и не попадают в логи или документацию.

## Origins и sharding

Текущий frontend разделяет authentication, action API и account data shards:

- обмен Mini App init data: `https://gifts.coffin.meme`;
- основной action API: `https://gifts.coffin.meme`;
- основной account data API: `https://gifts2.tonnel.network` или `https://gifts3.tonnel.network`;
- региональный action API: `https://rs-api.tonnel.network`;
- региональный account data API: `https://rs-gifts.tonnel.network`.

Без регионального режима `gifts3` выбирается, когда Telegram user ID делится на три, иначе используется `gifts2`. При `localStorage["server-ru"] == "true"` frontend выбирает региональные hosts.

`TONNEL_API_ORIGIN` задаёт только разрешённый origin мутаций до запуска задания. Автоматического переключения между hosts нет. `pageGifts`, balance и endpoints чтения BUY_OFFER продолжают использовать shard данных аккаунта; CREATE/ACCEPT/CANCEL используют action origin. Это разделение важно: маршрутизация всех `/api/buyOffer/*` через `gifts.coffin.meme` приводила бы к `404` для `getOffers` и `getMyOffers`.

## Текущий BUY_OFFER flow

### 1. Получение точного подарка N2

Обычный custodial inventory загружается через account data shard:

```http
POST https://{n2-data-shard}/api/pageGifts
Content-Type: application/json

{
  "page": 1,
  "limit": 30,
  "sort": "{\"gift_num\":1,\"gift_id\":-1}",
  "filter": "{\"seller\":<telegram_user_id>,\"buyer\":{\"$exists\":false},\"refunded\":{\"$ne\":true},\"price\":{\"$exists\":false},\"auction_id\":{\"$exists\":false}}",
  "ref": "",
  "user_auth": "<Tonnel session>"
}
```

Ответ — JSON array. Pagination продолжается, пока страница содержит 30 записей. Для операции используются точные `gift_id`/`sale_id`; имя, номер и свойства служат только для безопасного отображения и финальной проверки.

Frontend нормализует исходный `gift_id` в строковый `sale_id` для detail/read routes. CREATE требует числовой `gift_id`, поэтому преобразование выполняется строго и отклоняется при потере значения или неожиданном типе.

### 2. Проверка баланса N1

```http
POST https://{n1-data-shard}/api/balance/info
Content-Type: application/json

{
  "authData": "<N1 Tonnel session>",
  "ref": ""
}
```

Сумма оффера задаётся в TON, не в nanoTON. Приложение использует `Decimal`, разрешает не более трёх знаков после запятой и не выполняет денежные вычисления через binary `float`.

Для одного оффера текущая модель требует principal плюс fee создания `0.01 TON`. При `TONNEL_AUTO_OFFER_AMOUNT=2` минимальный preflight balance на подарок равен `2.01 TON`. Batch проверяет покрытие всей суммы до запуска дочерних заданий.

### 3. Снимок существующих офферов

До CREATE приложение читает обе официальные проекции:

```http
POST https://{n1-data-shard}/api/buyOffer/getMyOffers
Content-Type: application/json

{
  "authData": "<N1 Tonnel session>",
  "pageSize": 50,
  "filter": {},
  "tillTime": "<current ISO-8601 timestamp>"
}
```

```http
POST https://{n2-data-shard}/api/buyOffer/getOffers
Content-Type: application/json

{
  "authData": "<N2 Tonnel session>",
  "gift_id": "<exact gift.sale_id>"
}
```

Успешные ответы содержат `offers`. Из записей безопасно сопоставляются `offer_id`, `gift_id`, `price`, `asset`, `buyer`, `seller`, `status`, `createdAt`, `gift_name` и `gift_num`.

### 4. Создание одного оффера

```http
POST https://{configured-action-origin}/api/buyOffer/create
Content-Type: application/json
Origin: https://marketplace.tonnel.network
Referer: https://marketplace.tonnel.network/

{
  "authData": "<N1 Tonnel session>",
  "gift_id": <Number(exact gift.sale_id)>,
  "amount": 2,
  "asset": "TON"
}
```

`amount` — JSON number в TON. Для значений с дробной частью serializer сохраняет точное Decimal-представление без промежуточного `float`. `gift_id` — целочисленная форма точного `sale_id`. Recipient, timestamp, `wtf`, listing ID и отдельный finalize request отсутствуют.

Успех на уровне приложения определяется по `status == "success"`; при ошибке используется безопасное скалярное поле `message`. CREATE отправляется не более одного раза. Тайм-аут или неоднозначный ответ не приводит к повторной мутации.

### 5. Корреляция нового opaque offer ID

После CREATE обе read-проекции опрашиваются в ограниченном окне. Приложение строит разницу множеств offer IDs до и после запроса и ищет единственный общий новый оффер в N1 `getMyOffers` и N2 `getOffers`.

Кандидат должен одновременно удовлетворять условиям:

- один и тот же opaque `offer_id` с точным типом и значением в обеих проекциях;
- точные тип и значение `gift_id` выбранного подарка;
- точная сумма Decimal;
- `asset == "TON"`;
- status `pending` или `active`;
- buyer равен N1 и seller равен N2, если эти поля присутствуют;
- кандидат отсутствовал в pre-create snapshots.

Нельзя выбирать первый, последний, самый новый, самый дешёвый или совпавший только по имени оффер. Ноль кандидатов или несколько точных кандидатов дают безопасную остановку без ACCEPT.

### 6. Пауза готовности и единственный ACCEPT

После точной N2-side корреляции выполняется настроенная пауза:

```text
TONNEL_OFFER_ACCEPT_DELAY_MS=5000
```

Пауза применяется независимо к каждому дочернему заданию batch. Во время неё нет polling и мутаций. После паузы отправляется ровно один запрос:

```http
POST https://{configured-action-origin}/api/buyOffer/acceptBuyOffer
Content-Type: application/json
Origin: https://marketplace.tonnel.network
Referer: https://marketplace.tonnel.network/

{
  "authData": "<N2 Tonnel session>",
  "offer_id": <exact opaque offer_id>
}
```

Opaque `offer_id` передаётся без `String`, `Number` или `parseInt` conversion. Frontend не добавляет timestamp, `wtf`, prepare/finalize или автоматический retry.

Исторический контролируемый эксперимент с `1000` мс завершался HTTP 200 с `status="error"` и точным внешним сообщением `Please try again later!`, хотя тот же оффер позже принимался вручную. Последующий контролируемый запуск с `5000` мс прошёл успешно, поэтому `5000` — текущая подтверждённая задержка готовности. Возврат к `1000` не допускается без нового отдельного исследования.

### 7. Read-only проверка владения

После `status == "success"` на уровне приложения ACCEPT больше не повторяется. Приложение выполняет только проверки владения чтением примерно раз в секунду в течение:

```text
TONNEL_OWNERSHIP_VERIFY_TIMEOUT_MS=15000
```

Результат `SUCCESS` допустим только когда N1 владеет точным collectible, а N2 им больше не владеет. Диагностика сохраняет `timeout_ms`, `probe_count`, `first_probe_ms`, `confirmed_ms`, `source_owns_after`, `destination_owns_after` и `accept_success_to_ownership_confirmed_ms`.

Если ACCEPT вернул `status="success"`, но модель чтения не сошлась за 15 секунд, результат — `AMBIGUOUS / OWNERSHIP_VERIFICATION_TIMEOUT`, а не обычный `FAILED`. Пользователь получает сообщение: «Принятие подтверждено Tonnel, владение ещё не успело обновиться». Никакая мутация при этой проверке не повторяется.

Если ACCEPT вернул ошибку на уровне приложения, сохраняется прежняя ветка ошибки и безопасное сообщение Tonnel. Строка `success` никогда не используется как причина ошибки.

## Batch-модель и инварианты

Штатный one-click flow:

```text
PREPARE BATCH
  -> атомарно зарезервировать один batch для control user + N1 + N2
  -> авторизовать N1 и N2 по одному разу
  -> загрузить все подходящие подарки N2
  -> проверить общий баланс N1
  -> создать по одному child job на exact gift
FOR EACH GIFT
  -> CREATE_OFFER не более одного раза
  -> точная N1/N2 корреляция нового offer_id
  -> задержка 5000 мс после N2 confirmation
  -> ACCEPT_OFFER не более одного раза
  -> read-only ownership reconciliation до 15000 мс
FINALIZE BATCH
  -> одна progress message
  -> edit той же message
  -> одна final notification
```

`MAX_TONNEL_CONCURRENT_JOBS` задаёт параллельность независимых подарков в диапазоне `1..5`. N1/N2 Mini App auth и HTTP sessions открываются один раз на batch и безопасно используются children через batch auth context; конкурентные открытия одного Telethon session исключены.

Постоянный идентификатор batch, резервация точного подарка и защита транспортного уровня обеспечивают:

- один активный batch на control user + market + N1 + N2;
- принадлежность каждого дочернего задания ровно одному batch;
- одно активное дочернее задание на gift ID с точными типом и значением;
- `tonnel_offer_create <= 1` и `tonnel_offer_accept <= 1` для child;
- повторное нажатие не создаёт второй batch, observers или final notification;
- восстановление после перезапуска не повторяет внешнюю мутацию с неоднозначным состоянием.

`TONNEL_TRANSFER_DRY_RUN=true` выполняет аутентификацию, загрузку inventory, balance/fee preflight и официальные offer reads, но отправляет ноль CREATE/ACCEPT mutations.

## Цена и комиссия BUY_OFFER

Для оффера 2 TON текущая модель:

```text
principal N1                 2.000 TON
create fee N1               0.010 TON
minimum N1 balance          2.010 TON
proceeds N2                 1.990 TON
seller-side fee             0.010 TON (0.5%)
```

Frontend вычисляет seller proceeds как `offer * 0.995`, отображая значение с округлением до трёх знаков. Cashback в этой формуле не используется. Статическое исследование само по себе не определяет точный момент escrow/списания principal или возврата при отмене; эти детали нельзя выводить из UI.

## Историческая альтернатива `MARKET_SALE`

Этот маршрут не является текущим режимом batch по умолчанию. Он оставлен для диагностики и совместимости и создаёт публичную гонку покупки.

Размещение N2:

```http
POST https://gifts.coffin.meme/api/listForSale
Content-Type: application/json

{
  "authData": "<N2 Tonnel session>",
  "gift_id": "<exact sale_id>",
  "price": "5",
  "asset": "TON",
  "timestamp": <unix seconds>,
  "wtf": "<CryptoJS-compatible AES timestamp proof>"
}
```

Покупка N1:

```http
POST https://gifts.coffin.meme/api/buyGift/{sale_id}
Content-Type: application/json

{
  "authData": "<N1 Tonnel session>",
  "asset": "TON",
  "price": 5,
  "timestamp": <unix seconds>,
  "wtf": "<CryptoJS-compatible AES timestamp proof>"
}
```

`wtf` — `CryptoJS.AES.encrypt(String(timestamp), passphrase)` в OpenSSL-salted AES-256-CBC framing. Proof формируется непосредственно перед мутацией и не логируется.

BUY payload содержит seller/listing price, а buyer total рассчитывается отдельно. Для TON frontend использует `fee_factor = 1.005`. Без cashback:

```text
seller price  4.5 TON
buyer cost    4.5225 TON
fee           0.0225 TON (0.5%)
```

Один `/api/listForSale` и один `/api/buyGift/{sale_id}` — жёсткий максимум. Неоднозначный результат проверяется только чтениями. Поскольку listing публичный и не содержит `buyer_id`, `receiver`, `reserved_for`, `private` или `target_user`, другой покупатель может успеть раньше N1.

## Исторический Business-only маршрут `DIRECT_RECIPIENT`

Этот маршрут не выбран для рабочего пользовательского интерфейса. Он работает с отдельным управляемым инвентарём и требует официального подключения Telegram Business.

Управляемые подарки:

```http
POST https://{data-shard}/api/fetchMangedGifts
{
  "page": 1,
  "authData": "<Tonnel session>",
  "sort": "{\"gift_num\":1}",
  "filter": "{}"
}
```

Получатель сначала разрешается через:

```http
POST https://gifts2.tonnel.network/api/userInfo
{
  "authData": "<Tonnel session>",
  "user": "<username without @, or Telegram numeric ID as text>"
}
```

Затем frontend отправляет одну мутацию:

```http
POST https://gifts.coffin.meme/api/returnGiftToUser
Content-Type: application/json

{
  "authData": "<Tonnel session>",
  "gift_id": <exact opaque owned_gift_id>,
  "receiver": <numeric Telegram user ID>,
  "anonymous": false
}
```

Fee читается через `POST /api/returnGiftStats`; frontend показывал `0.3 TON`, а после `data.totalReturns >= 20` — `0.4 TON`. Эти значения исторические и требуют повторного подтверждения перед любым использованием.

## Карта других обнаруженных методов

Ниже перечислены методы, найденные во frontend; это не означает, что текущий бот их вызывает.

| Возможность | Вызов официального frontend | Основные поля |
|---|---|---|
| Публичное размещение | `POST /api/listForSale` | `authData`, `gift_id`, `price`, `asset`, `timestamp`, encrypted `wtf` |
| Отмена размещения | `POST /api/cancelSale` | `authData`, `gift_id` |
| Покупка точного listing | `POST /api/buyGift/{sale_id}` | `authData`, `asset`, `price`, `timestamp`, encrypted `wtf` |
| Создание buy offer | `POST /api/buyOffer/create` | `authData`, `gift_id`, `amount`, `asset` |
| Офферы подарка | `POST /api/buyOffer/getOffers` | `authData`, `gift_id` |
| Мои офферы | `POST /api/buyOffer/getMyOffers` | `authData`, `pageSize`, `filter`, `tillTime` |
| Принятие buy offer | `POST /api/buyOffer/acceptBuyOffer` | `authData`, `offer_id` |
| Отклонение buy offer | `POST /api/buyOffer/rejectBuyOffer` | `authData`, `offer_id` |
| Отмена buy offer | `POST /api/buyOffer/cancel` | `authData`, `offer_id` |
| Встречный оффер | `POST /api/buyOffer/counterOffer` | `authData`, `offer_id`, `amount` |
| Обмен подарок на подарок | `POST /api/trade/create` | `authData`, `gift_ids`, `amount`, `asset` |
| Детали обмена | `POST /api/trade/info` | `authData`, `trade_id` |
| Оффер обмена | `POST /api/offer/create` | `authData`, `trade_id`, `gift_ids`, `asset`, `amount` |
| Принятие trade offer | `POST /api/offer/accept` | `authData`, `trade_id`, `offer_id` |
| Возврат своему аккаунту | `POST /api/returnGift` | `authData`, `gift_id` |
| Приём через Relayer | `POST /api/quickTransfer` | `authData`, `gift_id` |
| Пакетный приём через Relayer | `POST /api/multiInstantTransfer` | `authData`, `gift_ids` |

## Безопасность и оставшиеся ограничения

- `TONNEL_TRANSFER_DRY_RUN=true` должен использоваться для первого операторского прогона после изменения конфигурации или frontend contract.
- CREATE и ACCEPT никогда не повторяются автоматически.
- Точные идентификаторы, сумма Decimal, buyer/seller, активный статус и разница множеств до/после обязательны для корреляции.
- CAPTCHA, cooldown, balance, ownership, marketplace и anti-bot restrictions не обходятся.
- Исходные initData, `authData`, cookies, ключи авторизации Telegram, `.session` и полные чувствительные ответы не сохраняются и не логируются.
- Внешний frontend/API может измениться; content-hashed bundle и маршрутизацию необходимо повторно проверять перед существенным изменением интеграции.
- Точный escrow/refund lifecycle BUY_OFFER и граничные правила fee остаются внешними контрактами Tonnel и требуют контролируемых проверок, если они становятся бизнес-критичными.
- `MARKET_SALE` остаётся публичной гонкой, а `DIRECT_RECIPIENT` — Business-only исторической альтернативой; ни один из них не должен незаметно подменять текущий `BUY_OFFER`.
