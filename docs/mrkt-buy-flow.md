# Покупка подарка через MRKT

Исторический снимок статического исследования от 2026-09-06. Публичный рабочий chunk `cdn.tgmrkt.io/js/storage-CsK8j0tH.js` был изучен без размещения и покупки подарка. Имя файла с хешем содержимого может измениться после обновления frontend.

## Подтверждённая последовательность

```text
свежий @mrkt Main Mini App WebView
→ POST /api/v1/auth с Telegram initData
→ рынок: POST /api/v1/gifts/saling
→ точный поиск: POST /api/v1/gifts/saling/by-ids
→ локальное подтверждение
→ POST /api/v1/gifts/buy
```

Пагинация рынка использует `POST /api/v1/gifts/saling` с `count`, `cursor` и фильтрами текущего экрана. Для известного размещения frontend вызывает:

```http
POST https://api.tgmrkt.io/api/v1/gifts/saling/by-ids
Authorization: <MRKT token without Bearer prefix>
Content-Type: application/json

{"ids":["<opaque gift id>"]}
```

Контроллер покупки строит отображение текущего `gift.id` в целое значение `gift.salePrice`, затем отправляет одну мутацию. `salePrice` — публичная цена покупателя с подтверждённой комиссией MRKT 2%, а не входная цена продавца из `/gifts/sale`:

```http
POST https://api.tgmrkt.io/api/v1/gifts/buy
Authorization: <MRKT token without Bearer prefix>
Content-Type: application/json

{"ids":["<gift id as object-key string>"],"prices":{"<gift id as object-key string>":3590400000}}
```

Преобразование ID в строку требуется контрактом frontend, потому что ключи JavaScript-объекта являются строками. Цена передаётся целым числом nanoTON. Например, цена продавца `3520000000` nanoTON публикуется как `salePrice=3590400000`, то есть точно `3520000000 * 1.02`. В `prices` запроса `/gifts/buy` используется `3590400000`.

Отдельных серверных запросов подготовки или завершения нет: подтверждение в интерфейсе локальное, а `/gifts/buy` — единственная мутация покупки.

В парной передаче предшествующий успешный `/gifts/sale` должен относиться к тому же opaque gift ID. Штатный поток не содержит приватной покупки, привязанной к продавцу или получателю, поэтому между публичным SALE и BUY остаётся риск внешней покупки. Ни SALE, ни BUY автоматически не повторяются.

## Источники

- [Bundle хранилища MRKT](https://cdn.tgmrkt.io/js/storage-CsK8j0tH.js)
- [Bundle интерфейса подарков MRKT](https://cdn.tgmrkt.io/js/gifts-CMlHWyyH.js)

После нового развёртывания frontend контракт нужно проверить повторно перед использованием.
