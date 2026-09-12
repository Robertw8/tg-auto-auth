# Исследование Portals Mini App

Исторический снимок чтения от 2026-08-31. Документ основан на статическом анализе frontend `portal-market.com` и авторизованных запросах чтения аккаунта, подключённого к приложению. Офферы не создавались, не принимались, не отклонялись и не изменялись.

Позднейшая реализация находится в `app/telegram_client/portals_service.py` и `app/jobs/portals_jobs.py`. Этот файл сохраняет исходные результаты исследования; диагностический инструмент по-прежнему не отправляет ACCEPT.

## Обнаруженная последовательность

```text
авторизованная Telegram session
→ RequestMainWebViewRequest для @portals
→ https://portal-market.com/ WebView со свежим tgWebAppData
→ Authorization: tma <raw Telegram initData>
→ GET /api/users/auth
→ GET /api/nfts/owned?offset=0&limit=20
→ GET /api/offers/received?offset=0&limit=20
→ выбор received NFT/offer
→ client route /offers
→ GET /api/offers/nft/{nft_id}?limit=10
→ локальное окно accept/reject
→ POST /api/offers/{offer_id}/accept с {"amount": "<offer amount>"}
→ read-only refresh NFT offers, received offers и inventory
```

Финальный POST в этом историческом документе получен из static inspection. Discovery-инструмент его не выполняет.

## Origins и авторизация

- интерфейс Mini App: `https://portal-market.com`;
- API площадки: `https://portal-market.com/api`;
- отдельный games API: `https://backend.portal-market.com`;
- static assets игр: `https://game-engine.portal-market.com/static`.

Raw Mini App initData отправляется на каждый marketplace request:

```http
Authorization: tma <raw initData>
Content-Type: application/json
x-request-id: <client-generated UUID>
```

`GET /users/auth` проверяет авторизацию и возвращает profile fields. Ответ содержит `token`, но marketplace client продолжает использовать raw initData в заголовке `tma`; exchange/replacement token не обнаружен. Обязательная cookie-session в read-only probe не наблюдалась.

## Инвентарь и полученные офферы

| Назначение | Запрос | Верхнеуровневые поля ответа |
|---|---|---|
| NFT пользователя | `GET /nfts/owned?offset=0&limit=20` | `nfts`, `total_count` |
| Полученные офферы | `GET /offers/received?offset=0&limit=20` | `top_offers`, `total_amount`, `total_count` |
| Офферы точного NFT | `GET /offers/nft/{nft_id}?limit=10` | `offers` |

Frontend ожидает вложенные объекты `offer` и `nft`. Для path действия используется `offer.id`, для точечного поиска — `nft.id`, а для ACCEPT — исходное `offer.amount`. В исследованном аккаунте списки были пусты, поэтому реальные типы JSON и полный набор вложенных полей тогда остались неизвестными.

## Открытие и принятие

Открытие оффера — клиентский переход в `/offers` и запрос чтения офферов NFT. `AcceptOfferModal` является явным пользовательским шагом подтверждения. Серверная подготовка отсутствует.

После подтверждения выполняется один запрос:

```http
POST /api/offers/{offer_id}/accept
Authorization: tma <raw initData>
Content-Type: application/json

{"amount":"<exact offer.amount value>"}
```

Amount отправляется в исходном виде, обычно как строка Decimal в TON. После успеха frontend обновляет офферы NFT, полученные офферы и инвентарь. Эти чтения не являются второй или завершающей мутацией.

## Ошибки и ограничения

Общий frontend распознаёт HTTP 400, 401, 404, 429 и 5xx; отдельные экраны обрабатывают 403. Среди идентификаторов ошибок встречались `USER_BANNED`, `INSUFFICIENT_BALANCE`, `NFT_NOT_LISTED`, но их применимость к ACCEPT не подтверждалась живой мутацией на момент исследования.

Collateral, разблокировка передачи, cooldown, баланс, блокировки аккаунта, rate limits и серверные ограничения остаются обязательными. Любой CAPTCHA, пароль, подпись кошелька или новый шаг подтверждения должен остановить автоматизацию, а не обходиться.

## Возможность прямой HTTP-интеграции

Для обнаруженного потока достаточно обычных HTTP-запросов JSON после официального запуска MTProto WebView и получения свежих initData. Автоматизация браузера/WebView для действия не требуется.

Этот вывод относится к зафиксированной версии frontend. После обновления схемы или bundles нужно повторить безопасную проверку. Исходные initData, token, cookies, request IDs, тела ответов и учётные данные Telegram диагностикой не выводятся.
