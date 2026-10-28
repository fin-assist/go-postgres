# go-postgres

Общие транзакции pgx v5 и обработка ожидаемых нарушений ограничений PostgreSQL для сервисов «Понятные финансы» ([FIN-195](https://finassist.youtrack.cloud/issue/FIN-195)). Основа — `go-library-template`. FIN-253 добавляет отдельный lifecycle primary/replica pools через `OpenPools`; прежний `New(pool)` оставляет переданный пул во владении вызывающего. Библиотека не создаёт таблиц и не знает доменных ошибок или `finerr`.

## Внедрение

1. Подключить опубликованную версию: `go get github.com/fin-assist/go-postgres@v0.2.0`. Теги следующих версий ставит владелец после мержа release PR; наличие секции в CHANGELOG ещё не означает публикацию версии.
2. Создать **один** `postgres.New(pool)` на основной `*pgxpool.Pool`. Пул открывает и закрывает сервис.
3. Передавать менеджер репозиториям; внутри `Run` брать `manager.Tx(ctx)`. Он реализует `interface { Tx(context.Context) (pgx.Tx, bool) }` для idempotency/outbox. Библиотеки не завершают полученную транзакцию.
4. Usecase объявляет свой интерфейс `Run(context.Context, func(context.Context) error) error`. Для операции с ожидаемыми нарушениями адаптер реализует его через `RunMapped`, задавая обработчик **всей операции**.
5. Usecase переводит полученную доменную ошибку в `finerr` по контракту метода. pgx, SQLSTATE и имена ограничений остаются в адаптере.

### Пулы, создаваемые библиотекой (FIN-253)

Доступно с `v0.2.0`.
Сервис может передать существующий caller-owned pool в `New`, как раньше, либо
явно поручить библиотеке создать и закрыть собственные пулы:

```go
pools, err := postgres.OpenPools(ctx, postgres.PoolOptions{
    PrimaryDSN: cfg.PrimaryDSN,
    ReplicaDSN: cfg.ReplicaDSN,
    MaxConns:   cfg.MaxConns,
    AppName:    "user",
})
if err != nil {
    return err
}
defer pools.Close() // after listeners/jobs/transactions have stopped
manager := postgres.New(pools.Primary())
```

Непустой `PrimaryDSN` обязателен: пустая строка не выбирает неявную БД окружения.
Пустой или точно совпадающий с primary `ReplicaDSN` использует один пул. Иной DSN
создаёт отдельный replica pool; как в шаблоне, startup проверяет только primary,
а соединения с replica создаются лениво. Неограниченное ожидание replica не
добавляется. Положительный `MaxConns` и непустой `AppName` применяются к обоим
пулам; иначе сохраняются настройки DSN/pgxpool.

`Pools.Primary()`/`Replica()` возвращают `*pgxpool.Pool` для существующих адаптеров.
Сервис выбирает Reader/Writer, read-primary hint и собственную транзакцию через
`manager.Tx(ctx)`. Чужая транзакция по-прежнему требует отказа, а не fallback в пул.
Зависимости от gRPC/request context в библиотеке нет. `Pools` не создаёт второй
Manager и не выбирает доменный mapper.

Ошибка startup закрывает все уже созданные пулы. `Pools.Close()` безопасен при
повторных/одновременных вызовах и закрывает каждый принадлежащий ему пул один раз.
Как `pgxpool.Close`, он ждёт возврата выданных соединений: сначала остановить работу
и завершить транзакции. Пулы нельзя отдельно закрывать через полученные указатели
или использовать после `Close`. `New(pool)` по-прежнему не закрывает переданный пул.

`OpenPools`/`Pools.Ping` возвращают безопасные ошибки, проверяемые через `errors.Is`:
`ErrInvalidDSN`, `ErrPoolUnavailable`, `context.Canceled`, `context.DeadlineExceeded`.
Причины `context.*` берутся из контекста вызывающего (`ctx.Err()`), а не из цепочки
драйвера: таймаут подключения pgx (`connect_timeout`, молчащий primary) при живом
контексте — это `ErrPoolUnavailable` (FIN-277).
Raw driver diagnostics и DSN намеренно не включаются ни в текст, ни в цепочку ошибки;
доступны стадия отказа и безопасная причина. Прежние transaction/mapping ошибки
не меняются. У nil/zero `Pools` метод `Ping` возвращает `ErrPoolUnavailable`, `Close`
ничего не делает. Методы доступа к пулам предназначены для результата успешного
`OpenPools`.

Адаптер конкретной операции (сервисные `domain` и `DB` здесь условные):

```go
func (r AssignCategoryTx) Run(ctx context.Context, fn func(context.Context) error) error {
    return r.manager.RunMapped(ctx, pgx.TxOptions{}, func(err error) error {
        if postgres.IsViolation(err, "23503", "assignment_category_fk") {
            return domain.ErrCategoryNotFound
        }
        return nil // неизвестная ошибка сохраняется для INTERNAL и внутренней диагностики
    }, fn)
}
```

В `wire()` именно `AssignCategoryTx` передаётся usecase назначения категории как его `Run`-интерфейс. Общий `DB.Run` остаётся без доменного обработчика. Внутри callback репозитории возвращают ошибки `Exec`, `Scan`, `Rows.Err` и batch без перевода: менеджер обрабатывает возвращённую ошибку запроса и ошибку настоящего `COMMIT` одним обработчиком. Для запроса вне транзакции есть `MapError(err, mapper)`; ошибки не теряются при `mapper == nil` или результате `nil`.

## Транзакции и вложенность

- `Run` / `RunWith` открывают транзакцию; `nil` из callback ведёт к COMMIT, ошибка, паника или `runtime.Goexit` (включая `t.Fatal` / `t.SkipNow` в тестах) — к ROLLBACK. Паника после cleanup пробрасывается; `Goexit` продолжает завершение goroutine. Отмена запроса не отменяет cleanup: для отката используется отдельный контекст с пределом 5 секунд.
- Вложенный `Run` с тем же менеджером присоединяется без savepoint и раннего COMMIT. Его ошибки обязательно возвращать наружу; вложенный вызов сам не откатывает транзакцию. `RunWith` при присоединении игнорирует свои опции: действуют опции внешнего вызова.
- Обработчик выбирает только внешний `RunMapped`. Вложенный вызов с ненулевым обработчиком возвращает `ErrNestedMapper` **до callback**. Когда операция включает несколько репозиториев, адаптер внешней операции явно перечисляет ожидаемые нарушения. Локальные обработчики не накапливаются и не конкурируют за поздний COMMIT.
- Контекст другого менеджера отвергается в `Run` с `ErrForeignTransaction`. `manager.Tx` для него возвращает `false`; это **не разрешение** выполнять запрос в своём пуле вне транзакции. Адаптер при выборе Writer/Reader должен отличать чужой контекст через пакетный `postgres.Tx(ctx)` и отклонять запрос (реализация — в `go-service-template`).
- Транзакционный контекст нельзя использовать конкурентно или после возврата callback. Независимые вызовы менеджера из разных goroutine допустимы при конкурентно безопасном пуле. Внутри транзакции нет внешних API-вызовов и медленного ожидания.

## Ошибки и ограничения

`IsViolation(err, sqlstate, constraint)` проверяет точную пару SQLSTATE + имя ограничения, включая обычные `%w`-обёртки. `MapError` допускает только FK (`23503`) и UNIQUE/PRIMARY KEY (`23505`). Пустое имя, другой код, неизвестное ограничение или дерево нескольких ошибок не превращаются автоматически в клиентский отказ. Обработчик не вызывается для BEGIN и ROLLBACK. При ошибке отката перевод не выполняется: ошибка cleanup присоединяется к исходной, обе причины доступны через `errors.Is/As`. Так сбой cleanup не маскируется ожидаемой доменной ошибкой.

Обработчик находится в адаптере и зависит от операции: одинаковое нарушение в другом методе может быть багом сервиса. Возвращать доменный sentinel или безопасную доменную ошибку; не оборачивать исходный `PgError` и не копировать `Detail` (значения ключей). Неожиданные ошибки сохраняются для внутренней диагностики; transport отдаёт голый INTERNAL без PostgreSQL-текста. `VALIDATION_ERROR` не используется для ссылки на отсутствующий объект; код выбирается по контракту.

Отложенный FK проверяется на COMMIT. `RunMapped` ловит эту ошибку после завершения callback; `SET CONSTRAINTS ALL IMMEDIATE` не применяется. Можно сначала создать дочернюю запись, затем родительскую в другом репозитории: если к COMMIT ссылка исправлена, транзакция успешна. Если нарушение неоднозначно в рамках операции, обработчик возвращает `nil`, сохраняя внутреннюю ошибку.

Для новых миграций рекомендуется обсуждать явные `CONSTRAINT <name>` вместо зависимости от автоматических имён. Это предложение требует решения владельца; библиотека не переименовывает существующие ограничения.

## Проверки и выпуск

```bash
TEST_DATABASE_URL=postgres://postgres@localhost:5432/postgres?sslmode=disable make test
make lint
make api
make cover
```

Без `TEST_DATABASE_URL` используется PostgreSQL 18 в testcontainers. Отсутствие PostgreSQL в CI — ошибка, локально — skip, который не считается проверкой. Интеграционные тесты проверяют реальные FK/UNIQUE, отложенный COMMIT, исправление ссылки до COMMIT, вложенный откат, panic и независимые конкурентные транзакции. Usage-пример с переводом в `finerr.NOT_FOUND` тестируется в шаблоне сервиса.

CI и скрипты взяты из шаблона: tidy, vet, race, lint, CHANGELOG, анализ API и независимые statements-пороги80 для unit/integration/API (FIN-243). [CHANGELOG.md](CHANGELOG.md), [docs/releasing.md](docs/releasing.md), [AGENTS.md](AGENTS.md). Своих метрик, фоновых заданий и миграций нет. Последний опубликованный выпуск — [v0.2.0](https://github.com/fin-assist/go-postgres/releases/tag/v0.2.0) (FIN-254). История сбоя tag-workflow v0.1.2 и порядок выпусков описаны в [инструкции](docs/releasing.md#история-go-postgres-v012-fin-242). Теги агент не ставит.

Bootstrap и активация контроллера Renovate выполнены в [FIN-237](https://finassist.youtrack.cloud/issue/FIN-237); текущее включение задаёт repository variable `RENOVATE_POLICY_ENABLED`. Настройка — [docs/renovate-policy.md](docs/renovate-policy.md). Workflow Claude уже в `main`; ревью последующих PR запрашивается по [AGENTS.md](AGENTS.md#workflow).

### Покрытие FIN-243

`make collect-unit`, `make collect-integration`, `make collect-api` собирают независимые профили штатным Go coverage. `make check-unit`, `make check-integration`, `make check-api` проверяют каждый профиль закреплённым go-test-coverage/v2 v2.20.1 с total80. Принимается округление79,95% до80,0%; `make cover-combined` строит дополнительное объединение штатными средствами инструмента.

Все три набора выполняют `./...` и инструментируют одинаковые пакеты из `go list -race ./...`, исключая только точный пакет `<module>/internal/pgtest` по решению владельца 25.10.2026. Остальные обычные `.go` helpers и generated production code учитываются; тесты pgtest продолжают исполняться. Имена TestUnit/TestIntegration/TestAPI и FuzzUnit/FuzzIntegration/FuzzAPI проверяются race inventory до сборщика; исполняемые Example входят в API. Unit не использует БД/сеть, integration использует реальные зависимости, API проверяет публичный Go-контракт от лица потребителя. API не означает новый HTTP/gRPC сервис.

Пустые/пропущенные/упавшие наборы и отсутствующие/пустые/zero-statement профили не проходят guard; недоступный PostgreSQL при CI=true — ошибка. Обязательный job `go` падает при любой ошибке одного из трёх уровней, даже если combined выше80. Отчёты и профили сохраняются в CI artifacts; fork PR не пишет комментарии. Собственного расчёта процентов и merge нет.

[docs/testing.md](docs/testing.md) описывает уровни и границы; актуальные проценты и невыполненные критерии указаны в PR.

Проверка `api` устойчива к публикации тега во время CI: release-коммит сравнивается
с предыдущим выпуском, удалённый тег проверяется по commit до и после `gorelease`.
Подробности и границы исключения — [docs/releasing.md](docs/releasing.md#проверка-версии-в-ci-fin-264).

## Общий Go CI и линтеры (FIN-372)

`.finassist-ci.json` закрепляет общий источник полным SHA и объявляет профиль/исключения. CI вызывает reusable workflows того же SHA; `make lint`, `make fmt`, `make lint-config` и `make lint-migrations BASE_REF=origin/main` используют его через `scripts/common_ci.py`. Конфигурации создаются в игнорируемом `.finassist-ci/generated/`; локальные копии правил не редактируются. Первый локальный вызов требует доступ к GitHub. Обновление workflow и manifest pin приходит одним PR Renovate; откат возвращает прежние SHA вместе. [Источник, профили, обновление и откат](https://github.com/fin-assist/go-service-template/blob/main/docs/shared-go-ci.md). Проверки тестов/coverage, PostgreSQL 18, race и совместимость сохраняются.

Если новая ревизия общего источника меняет `common/bootstrap.py`, в том же PR обновить `scripts/common_ci.py` точными байтами из закреплённого SHA вместе со всеми pins. Byte-equality contract намеренно отклонит частичный переход; конфиги линтеров обратно не копируются. Откат возвращает прежние pins и соответствующую копию bootstrap вместе. Человеческий коммит для такой синхронизации переводит Renovate PR в ручное сопровождение: автомерж FIN-211 к нему больше не применяется.
