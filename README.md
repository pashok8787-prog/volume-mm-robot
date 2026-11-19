# Volume Market-Making Robot

Python 3.12+ робот для бессрочных фьючерсов **Aster DEX** и **Hyperliquid**. Он ищет аномально высокий двусторонний оборот при ограниченном движении цены и выставляет лимитные заявки на покупку ниже и продажу выше. Размер ограничен небольшой долей наблюдаемого оборота.

По умолчанию включён `paper`: подключаются только публичные данные, реальные ордера не отправляются. Есть testnet/live-адаптеры, replay и автономный demo-режим.

> Всплеск объёма, чередование покупок и продаж или повторяющиеся размеры сделок не доказывают wash trading и не идентифицируют маркет-мейкера. Робот использует рыночную закономерность, а не утверждение о мотивах участников. Такая активность может оказаться направленным потоком или новостным импульсом; прибыль не гарантирована.

## Логика стратегии

1. Сделки объединяются в секундные корзины. Последние 10 корзин сравниваются с предыдущими 60.
2. Сигнал требует одновременно:
   - рост оборота минимум в 4 раза и абсолютный минимум 5 000 USDT/USDC;
   - не менее 20 % оборота на каждой стороне;
   - ограниченные диапазон и направленное изменение цены;
   - достаточное число сделок и приемлемый спред.
3. Bid размещается не выше лучшего bid и минимум на `entry_offset_bps` ниже midpoint; ask — симметрично. Входы только post-only: Aster `GTX`, Hyperliquid `ALO`.
4. Базовый номинал заявки:

   `min(max_order_quote, 2 × min(buy_quote, sell_quote) × participation_rate)`

   Дополнительно действуют лимит доли видимого уровня стакана (когда цена совпадает с уровнем), остаток inventory limit, `stepSize` и минимальный номинал биржи. Если после округления заявка слишком мала, она не отправляется.
5. После входа противоположная entry-заявка отменяется. Выход только reduce-only на заданном расстоянии от средней цены.
6. Направленный импульс, устаревший стакан и отсутствие сигнала снимают входы. Stop-loss, дневной лимит убытка и таймаут позиции вызывают reduce-only **лимитный IOC** с ограниченным проскальзыванием, но не рыночный ордер.

Параметры являются консервативной отправной точкой, а не оптимизированными значениями. Их нужно проверять на записанных данных отдельно для каждого инструмента.

## Paper: быстрый запуск

```bash
uv sync --all-groups
cp config.yaml.example config.yaml

# Синтетический полный цикл без сети
uv run volume-mm-robot demo

# Aster, 180 секунд публичных данных
uv run volume-mm-robot run --exchange aster --symbol ASTERUSDT --duration 180

# Hyperliquid perpetual
uv run volume-mm-robot run --exchange hyperliquid --symbol HYPE --duration 180
```

API-ключи для paper не нужны. Первые 70 секунд детектор обычно накапливает базу, поэтому короткий запуск может не дать сигналов. Нулевой PnL за короткую сессию не означает ошибку, как и положительный PnL demo не доказывает доходность стратегии.

На этом ПК можно использовать готовый `uv` из соседнего проекта:

```bat
volume-mm.bat demo
volume-mm.bat run --exchange aster --symbol ASTERUSDT --duration 180
volume-mm.bat run --exchange hyperliquid --symbol HYPE --duration 180
```

Во время работы создаются:

- `data/*-paper.raw.jsonl` — нормализованные публичные сделки и стакан;
- `data/*-paper.events.jsonl` — сигналы, заявки, fills, ошибки и итоговые метрики.

`Ctrl+C` отменяет заявки. В paper оставшаяся виртуальная позиция закрывается по доступному стакану. В testnet/live робот отменяет свои ордера, но **не закрывает автоматически реальную позицию при завершении процесса**.

## Replay

```bash
uv run volume-mm-robot replay \
  --input data/файл.raw.jsonl \
  --tick-size 0.0001 \
  --step-size 0.001 \
  --min-notional 5
```

`tick-size`, `step-size` и минимальный номинал нужно взять из `exchangeInfo`/метаданных конкретного инструмента. Replay работает без сети. Paper-модель учитывает частичные fills, maker/taker-комиссии и очередь впереди заявки на уровне. Сделка по той же цене исполняет заявку только после расхода предполагаемой очереди; сделка через цену исполняет по лимиту.

Это всё ещё приближение: публичный поток не содержит точной позиции робота в очереди, отмены чужих ордеров впереди и фактическую сетевую задержку.

## Testnet

Установите зависимости подписания и официальный SDK Hyperliquid:

```bash
uv sync --all-groups --extra live
```

Для Aster V3 Futures используются API wallet и EIP-712. Для Hyperliquid — API wallet и адрес основного аккаунта. Секреты задаются только переменными окружения, не YAML.

PowerShell, Aster:

```powershell
$env:ASTER_USER_ADDRESS="адрес основного кошелька"
$env:ASTER_SIGNER_ADDRESS="адрес API wallet"
$env:ASTER_PRIVATE_KEY="приватный ключ API wallet"
uv run volume-mm-robot --config config.yaml run --mode testnet --duration 300
```

PowerShell, Hyperliquid:

```powershell
$env:HYPERLIQUID_ACCOUNT_ADDRESS="адрес основного аккаунта"
$env:HYPERLIQUID_PRIVATE_KEY="приватный ключ API wallet"
uv run volume-mm-robot --config config.yaml run --exchange hyperliquid --symbol HYPE --mode testnet --duration 300
```

Перед запуском адаптер проверяет позицию и запрещает старт при уже открытых ордерах по инструменту. Используйте one-way mode; hedge mode не поддерживается. Адаптер периодически сверяет статусы и фактические объёмы fills. Комиссии в учёте рассчитываются по ставкам из конфигурации, поэтому их нужно сверить с тарифами аккаунта.

**Testnet-запросы с ордерами в этой сборке ещё не выполнялись: для этого нужны отдельные testnet-ключи пользователя. Перед любыми средствами необходимо проверить размещение, частичный fill, отмену, reconnect и reduce-only выход на testnet.**

## Live

Сначала задайте в YAML:

```yaml
mode: live
allow_live: true
```

Затем потребуется второе подтверждение в команде:

```bash
uv run volume-mm-robot --config config.yaml run --mode live --confirm-live --duration 300
```

Не используйте основной приватный ключ: создайте отдельный API wallet с минимальными правами и небольшим балансом. Робот не меняет плечо, не переводит средства и не использует рыночные ордера, но лимитный IOC может исполниться как taker. Funding, ликвидация и проскальзывание реального исполнения остаются вне paper-модели.

## Основные ограничения риска

- `max_order_quote` — максимум одной заявки;
- `max_inventory_quote` — максимум абсолютной позиции;
- `participation_rate` — доля от двустороннего оборота;
- `max_book_participation` — доля видимого объёма уровня;
- `max_daily_loss_quote` — остановка по реализованному и текущему PnL;
- `stop_loss_bps`, `max_hold_sec`, `cooldown_sec`;
- отмена при старом стакане и ограничение частоты перестановки заявок.

Для длительной эксплуатации дополнительно нужны независимый внешний kill-switch, persistent recovery после падения процесса, user-data WebSocket и сверка баланса/комиссий. Их отсутствие особенно важно для live.

## Проверки

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv build
uv run volume-mm-robot demo
```

## Изученные источники

- [Aster: официальный репозиторий API](https://github.com/asterdex/api-docs) и [Aster Futures V3](https://github.com/asterdex/api-docs/blob/master/V3(Recommended)/EN/aster-finance-futures-api-v3.md): `aggTrade`, `depth20`, `exchangeInfo`, `GTX`, V3 EIP-712.
- [Hyperliquid WebSocket](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket) и [Subscriptions](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions): `trades`, `l2Book`, reconnect.
- [Hyperliquid Exchange endpoint](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint): `ALO`, `IOC`, reduce-only и cancel.
- [Официальный Hyperliquid Python SDK](https://github.com/hyperliquid-dex/hyperliquid-python-sdk).
- [QuestDB: Market Making Algorithms](https://questdb.com/glossary/market-making-algorithms/): двусторонние котировки, inventory risk, adverse selection и quote throttling.

Исходный код опубликован в приватном репозитории [volume-mm-robot](https://github.com/pashok8787-prog/volume-mm-robot).
