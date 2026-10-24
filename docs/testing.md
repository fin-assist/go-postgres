# Проверки библиотеки и покрытие FIN-243

## Уровни

- Unit: изолированные функции и ветви без БД и сети, на фейках объявленных интерфейсов.
- Integration: настоящие PostgreSQL18, миграции/транзакции/блокировки, transport и фоновые процессы там, где они есть.
- API: публичный Go API со стороны потребителя; для stateful контрактов реальный PostgreSQL и транзакция вызывающего. Прямой вызов private реализации не становится API из-за имени теста. Примеры с Output исполняются в API, без Output компилируются, как раньше.

TestUnit/TestIntegration/TestAPI и FuzzUnit/FuzzIntegration/FuzzAPI задают селекторы. Race inventory блокирует неклассифицированные Test/Fuzz до каждого сборщика; отдельный регрессионный тест запускает настоящий Go-модуль с Example, fuzz seed и race-only тестом. Собственного реестра тестов нет.

## Команды и метрика

`make collect-{unit,integration,api}` собирает отдельные JSONL и профили Go statements с race/count1/timeout. `make check-{unit,integration,api}` сначала проверяет execution evidence и профиль, затем запускает go-test-coverage/v2 v2.20.1 с total80. Принимается штатное округление79,95→80,0. `make cover` требует все три порога. `make cover-combined` объединяет исходные профили штатным comma-входом; результат информационный.

Для всех трёх профилей инструментируется одинаковый весь `./...`, включая generated и обычные `.go` helpers. Исключений ради порога нет; решение об `internal/pgtest` отдельно ожидается от владельца. Сравнивать старые уникальные строки FIN-210 с новой statements-метрикой нельзя. Проценты/merge рассчитывает только готовый инструмент.

Guard требует реально выполненный passing Test/Example/Fuzz event, отсутствие skipped/failed тестов и исполняемый statement block. Package skip при отсутствии тестов допустим; Test skip — нет. Пустой, отсутствующий, header-only и zero-statement профиль не даёт зелёный статус. PostgreSQL без Docker/TEST_DATABASE_URL при CI=true — ошибка. Каждый агентский запуск также ограничивается process timeout.

CI сохраняет unit/integration/API/combined reports, native breakdowns, JSONL и профили как artifacts. Его прежний required check `go` проверяет все шесть collector/check outcomes; промежуточный continue-on-error не скрывает ошибку. Права только read, комментариев coverage нет, fork PR сохраняет ограничения. Job `api` по-прежнему означает gorelease compatibility и не заменяет API-тесты.

`make test`, `make lint`, `make api` выполняются до публикации. Release/CHANGELOG/migrations/Renovate проверки сохранены. Новые версии и теги не создаются.

## Контракт go-postgres

Публичные consumer-сценарии проверяют совместную запись двух репозиториев в одной транзакции, вложенные опции и принадлежность транзакции, общий commit/rollback на ошибке/panic/cancel/Goexit, отказ другому manager и вложенному mapper без эффектов, перевод настоящего deferred FK только после COMMIT без raw SQL detail, закрытый pool до вызова callback. Все используют PostgreSQL18 и экспортируемые New/Tx/Run/RunWith/RunMapped/IsViolation. Unit отдельно проверяет declared Beginner/pgx.Tx контракты, ошибки BEGIN/COMMIT/ROLLBACK и mapper без сети.
