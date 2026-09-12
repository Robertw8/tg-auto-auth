# Исследование прямой/приватной передачи MRKT

Официальные frontend bundles MRKT проверены 2026-09-08. Документ описывает
только наблюдаемые контракты; неподтверждённые endpoints не добавлялись.

## Вывод

В изученном frontend MRKT нет официальной передачи подарка конкретному
получателю или приватной продажи. Поэтому по-прежнему необходима последовательность
публичного SALE и публичного BUY. Экспериментальный режим SPECULATIVE может
сократить время экспозиции, но не делает размещение атомарным или зарезервированным
для N1.

## Связанные официальные контракты

| Назначение | Метод и endpoint | Запрос | Идентификатор получателя/продавца | Публично? | Заменяет SALE → BUY? |
|---|---|---|---|---|---|
| Создать публичное размещение | `POST /api/v1/gifts/sale` | `{"ids": [gift_id], "price": integer_nanotons}` | Нет поля buyer/recipient | Да | Текущий первый шаг |
| Купить публичное размещение | `POST /api/v1/gifts/buy` | `{"ids": [string_gift_id], "prices": {string_gift_id: integer_nanotons}}` | Нет ограничения продавца и резервации | Да | Текущий второй шаг |
| Вернуть/вывести подарок | `POST /api/v1/gifts/return` | `{"ids": [gift_id]}` | Нет получателя; действие выполняет авторизованный аккаунт | Нет продажи | Нет, не передаёт N1 |
| Создать gift offer | `POST /api/v1/offers/create` | `{"price": integer_nanotons, "giftSaleId": gift_sale_id}` | Связан с существующим размещением, а не получателем | Подарок уже должен быть публично размещён | Нет |
| Принять gift offer | `POST /api/v1/offers/accept?offerId=...&price=...` | Нет тела JSON | Точный offer ID после публичного размещения/оффера | Зависит от публичного размещения | Нет |

Интерфейс gift offer прямо предлагает покупателю открыть уже выставленный подарок, а
тело запроса использует `giftSaleId`. Это не приватный, резервируемый или привязанный
к покупателю механизм. В изученных контроллерах sale, multi-sale, price-change,
cancel, offer и return нет target Telegram user ID, recipient ID, reserved buyer
ID или эквивалентного поля.

## Источники

- [Официальный gift controller MRKT](https://cdn.tgmrkt.io/js/my-gifts.controller-Cx1JQcCB.js)
- [Официальный offer controller MRKT](https://cdn.tgmrkt.io/js/offer.controller-BBdJZWMV.js)
- [Официальный gift UI bundle MRKT](https://cdn.tgmrkt.io/js/gifts-CMlHWyyH.js)

Имена bundles содержат хеш содержимого и могут измениться после развёртывания frontend.
После такого обновления контракты нужно проверить повторно.
