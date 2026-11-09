# EA-010 — план этапов реализации

## 1. Паспорт и статус передачи

Редакция **0.1**, 9 ноября 2026 года. Проект `vikunja-local-ea010-install-integration`, native task `t_606468bd`, карточка `t_918a16a1`, шаг `implementation-planning`, роль `ai-developer`. Владелец приёмки — `iiefim10` в Development. **Проект плана в подтверждённой части с условиями допуска; не принят, не DoR и не разрешение рабочих изменений.**

Единственный результат текущего поручения — этот документ. Код, тесты, патчи, рабочие службы, конфигурации, принятые документы и process-state не изменяются. Commit/push/PR сейчас не выполняются. Предлагаемые ниже файлы и команды реализации **ещё не существуют и не запускались**: это точный план их создания, не отчёт об успешном исполнении.

Рабочая область: `/Users/efimovivan/Documents/DevProjects/worktrees/vikunja-local-ea010-install-integration/vikunja-local/install-vikunja-tracker`. Наблюдаемые HEAD `ce4e5405f135c0a0189a8226bd5ca0ea973ca002`, ветка `hermes/install-vikunja-tracker`, remote `origin` → `git@github.com:iefimov1008/vikunja-local.git`. Они совпадают с назначенной областью и SSH remote frozen brief. Tracking branch в локальной конфигурации не задана; сетевой проверки remote SHA этим документным этапом не было. В дереве до работы — README и ранее подготовленные `docs/`; не выполнять `git add .` и не переписывать их.

Штатный read-only `process_plan.py next` с `AI_PLATFORM_PROCESS_DIR=/Users/efimovivan/Projects/cto-agent-environment/ops/real/process-profile-staged` вернул `implementation-planning / ai-developer`, `blocked_by: []`, `external_inputs: []`, recorded/current registry `65dc5c6854ff`, drift `false`. Переменная указывается явно в shell-вызовах planner; базовый реестр не подставляется. `requirements-review-gate` записан как `passed`; его прежняя приёмка не закрывает новые содержательные условия G-01–G-07.

### Проверенные входные редакции

Полная постановка прочитана, включая дополнительные указания, публикацию и готовность. SHA256 всех восьми файлов повторно вычислены и совпали с назначением. Принятые документы read-only; источник их приёмки — [process-state](../process/t_606468bd.md), не исторические надписи «черновик» в тексте.

| Вход | SHA256 |
| --- | --- |
| [Frozen brief](</Users/efimovivan/Documents/DevProjects/intake/vikunja-local-ea010-install-integration-task.md>) | `b5127175403c077884d3d1de00e445fd7cabbab13718024c00a4d1b4027b4165` |
| [БТ](t_606468bd-business-requirements.md) | `31648614925c5542f0b588d715586219fbe168b505452c802f0f663fcf344f9b` |
| [ADR](../adr/t_606468bd-adr.md) | `94d5da185d5ec617cb7b23214afd196f8aafe2a91d6eadb587cce6bc73ca6a85` |
| [НФТ](../requirements/t_606468bd-nfr.md) | `5bb23e0e741bf29eacee9cb5827e08218368d31441077cfa028682e6e9052dfc` |
| [Макеты](../design/t_606468bd-mockups.md) | `eb413575640b276c8188d43b87fcb0f5c68c1a9421f5d7147cd7b823d43105a8` |
| [UX](../design/t_606468bd-ux-spec.md) | `07b9da2d94890000f09391d90ff31387e75b636aaffe492f12bcbd6a1ac90966` |
| [Оценённые пакеты](../tasks/t_606468bd-estimated-tasks.md) | `7cdd371ac63b3e3edaefa65dde6035d085cd2776adda5e04336315d4e2858d8e` |
| [ТЗ 0.5](../requirements/t_606468bd-system-requirements.md) | `ac47f82736cf2373052b7d31ca2a74a4e7f7b01767cb38c920f2c99ae06d2266` |

Чтение инструкций: AI Platform AGENTS/ai-developer, staged `steps/implementation-planning.md`, process-guard, intake AGENTS/WORKFLOW, WS6 README и README сервиса. Новых локальных AGENTS.md в назначенном дереве не найдено. Общий dev-flow ведёт родительский coordinator; этот worker выполняет только назначенный документный шаг.

## 2. Уточнения владельца и граница доказательств

В карточке сохранены и использованы ответы:

- `5633d201-968f-4775-aa24-5060d3c4f790` на `ea010-implementation-plan-conditions-01`: сначала получить входы, не запускать зависимую реализацию.
- **Актуальное уточнение** `ea010-g03-standard-api-dad4617e11ab3370` на `ea010-implementation-plan-evidence-02`: подготовить план в подтверждённой части; оставшиеся проверки и точную внешнюю область оставить условиями перед зависимыми этапами. Это изменяет прежнюю паузу планирования, но не принимает документы, DoR или рабочее применение. Использование ответа зафиксировано `[devflow-answer-used:v1]`.

### G-03: стандартный API и однопользовательское допущение

Владелец как архитектор прямо разрешил пренебречь гонкой одновременной ручной и интеграционной записи для личного однопользовательского пилота. **CAS, расширение/пересборка Vikunja и соответствующая внешняя область исключаются.** Стандартная Vikunja v2.8.0 сохраняется. Это не ослабляет недублирование create, сериализацию интеграционных повторов, проверку прав, журнал, outbox и backlink.

Для update/complete — `PATCH /api/v2/tasks/{id}`, `Content-Type: application/merge-patch+json`, только явно изменённые разрешённые поля, узкие scopes и последующий GET того же ID. Пропущенные поля, due и маркер сохраняются; явный `null` очищает due только после подтверждения. `0001-01-01T00:00:00Z` нормализуется как отсутствие срока, не показывается дедлайном. Date-only передаётся как `23:59:59 Europe/Moscow`, пользователю показывается дата; относительная дата фиксируется при подтверждении вместе с абсолютным значением/часовым поясом и не пересчитывается при повторе. Дата встречи не становится due. Изменение срока снимает прежнее подтверждение.

ETag/If-Match **не CAS**; предварительный GET и локальный lock не дают защиты от UI-гонки. При 304 выполняется GET без условного кеширования и проверка состояния. У обычной завершённой задачи повтор не отправляет новую мутацию. У повторяющейся задачи один complete может передвинуть due и вернуть `done=false`; это не повод повторять complete до `done=true`. При неизвестном исходе update/complete нет blind retry: сверка сохранённого исходного наблюдения и фактического объекта; недоказанный результат остаётся unknown. Личная одновременная правка может потеряться — это явно принятое ограничение, не тестируемая гарантия отсутствия lost update. Остальные негативные проверки доступа остаются обязательными.

**Расхождение с принятыми входами:** ТЗ UC-02 E3, интерфейс update, G-03, AC-EA010-18 и I4 оценённого бэклога ещё требуют защиты от ручной гонки; API update там описан через v1 POST. Эти файлы разработчик не меняет. Перед зависимым кодом аналитик должен отразить новое решение в принятом уточнении ТЗ, архитектор — согласовать затронутые ссылки/условия A1/ADR; владелец повторно рассматривает затронутую редакцию через `requirements-review-gate`. Старые SHA не объявляются SHA новых требований.

### Переданные свидетельства (read-only)

Базовый каталог свидетельств, далее **E**:
`/Users/efimovivan/Documents/obsidian/obsidian/CTO AI Environment/Workstreams/06 Agent Platform/Review/Real/t_606468bd`.
Все перечисленные ниже файлы прочитаны, их SHA256 вычислены. Это адреса существующих локальных материалов, не публичные ссылки и не новые принимаемые документы этого worker.

| Материал в E | SHA256 | Что подтверждает / предел |
| --- | --- | --- |
| `EA-010 A1 Architectural Review.md`, 0.1 | `b565f8d0d802955e74ba6a744419564597553cfa17443fa0bc6b781cdcc2fbb6` | Принятое A1 по ответу `ea010-a1-architect-01-b565f8d0d802955e74ba`; не повторная приёмка эксплуатации |
| `EA-010 G01-G07 Evidence Register.md`, 0.1 | `b5d0a09bfff6912e1b46881515b69bdb4bdacd5ebb30aed016c8592f63bb2129` | Частичные bindings/права и наблюдённые SHA внешних кандидатов; не whitelist применения |
| `EA-010 G01-G03 Synthetic Evidence 02.json` | `fcb8d159ff632087e28bde61a3a623ab0bb195c91af11a6c67734f938a2d0452` | Create-маркер, exact paging, три SIGKILL собственного процесса; журнал/backlink синтетические |
| `EA-010 G01 Late Write Evidence 01.json` | `6c0201e29b7c271fc56dff4b593bbc897002e632679a80b45c5fcdd75b743ca3` | Клиентский timeout, промежуточный ноль, поздний 201 и один ID; задержка в прокси до upstream, не внутри серверной транзакции |
| `EA-010 G03 P9 P10 G06 G07 Architecture Decisions.md`, 0.1 | `0ba7851497515c7153abec732cb98b5caf09b6a0b8a87e6b419fdaab25eaa6b9` | Исторические варианты/условия. Вариант A/CAS заменён последним ответом; календарные окна P9/P10 сняты, пробы выполнены по требованию |
| `EA-010 G03 Single Owner API Evidence 02.json` | `dad4617e11ab33706462e8f2bbb1ba4c047e69730f8528d46b51a158474ffc45` | Последовательные v2 PATCH, omission/null, MSK, 304/GET, один recurring complete, 401 и отзыв двух токенов; не CAS и не response-loss retry |
| `EA-010 P9 MacBook Evidence 02.json` | `f0c3be1587764c8bcb7ea55c197e3f1644a7a7c2f73557b6144f2363661a1489` | Два отдельных SSH/Codex чтения ID 1, локальный reconnect, браузер подтверждён владельцем; не CRUD/reconnect ключа записи будущего MCP. DELETE токена в пользовательском прогоне 401; после исправления отдельный preflight дал 200; изолированное хранилище удалено |
| `EA-010 P10 In-Process Readiness 01.json` | `8394ddf543d4fdccf1f407eee073299517b0b8973582959a671c7f78bcab5a7f` | Guard 401/403/409/422, одна задача/ссылка и сохранение старого Markdown в preflight; временные auth/outbox spike |
| `EA-010 P10 Owner Evidence 01.json` | `57809feef2c08ac328270f643638a50e3564d7cb5a969c1fd8254a99bdecfa33` | Личное синтетическое принятие, одна acceptance/outbox linked/задача ID 1/backlink. Штатного оконного отчёта нет: восстановлен координатором. В личном прогоне нет pre-run digest старого Markdown; cleanup завершил координатор после оставшегося процесса |

P9/P10 признаются полученными результатами стенда, не требуют повторного общего разрешения старых проб. **P10 относится к согласованному `in_process`, не к hosted IPC.** В рабочем контуре principal/Origin/режим ещё проверяются; синтетический login/outbox нельзя переносить как production-auth. Исправления cleanup в старых runner не считаются проверкой будущего runner. Исторический CAS spike/SQL-миграции/Go toolchain не являются зависимостью этой реализации.

## 3. Условия допуска и точный следующий гейт

Текущий результат можно рассмотреть и принять как **условный план**, но это не готовность к запуску всех stages. Нельзя интерпретировать машинный список stages как обход условий. До передачи любого stage Cursor coordinator проверяет актуальные принятые редакции, DoR и отдельные условия его строки. При новом поведении/новом пути внешнего воздействия — остановка зависимой части и решение владельца, не молчаливое дополнение.

| Условие | Что требуется и кто передаёт | Что остаётся запрещено |
| --- | --- | --- |
| C1 — уточнение G-01–03 | Ведущий flow передаёт проверенные свидетельства аналитику; аналитик закрепляет точный формат/кодирование маркера, разрешённую область/пейджинг сверки, устойчивые ключи/issuer/retention, границы durable-save/lock, ошибки и unknown, v2 PATCH/recurring/даты и однопользовательское исключение. Профильное review и приёмка точной редакции владельцем | Зависимый API/recovery/MCP-код не стартует по одному синтетическому JSON. Exactly-once и безопасность пустого поиска не обещаются |
| C2 — рабочий G-04 | Зафиксировать совместимую delta decision/recover, stable proposal/event IDs, атомарное принятие + восстанавливаемое намерение/outbox, права на запись backlink и политику старых accepted/Markdown. Для in_process — настоящий серверный owner principal, exact Origin и revision; hosted не включать без отдельного решения | s06/s07 не реализуются по придуманному wire/auth-контракту; старые принятые предложения не выгружаются автоматически |
| C3 — оставшийся G-05 | Утвердить полную MCP tools/input/output/errors/version/reconnect identity спецификацию без ручного ввода технических ключей владельцем; шаблоны Mini/MacBook UI/deep links, exact origins/scopes. P9 доказывает read-транспорт, не все эти свойства | Нет заявления о полном CRUD/reconnect/обеих рабочих ссылках по P9; native HTTP MCP не второй writer |
| C4 — G-06 | Уполномоченная эксплуатация дополняет read-only inventory: процесс/DB/files/config bindings, единственность, права, reuse supervisor; безопасная согласованная копия и обратный переход до любых изменений. Личный пилот после login принят; сон/ожидание FileVault — недоступность, без изменения настроек системы. Согласовать конкретное окно рестарта и воздействия | Нет второго сервиса над рабочей БД, неявной миграции, pre-login/24×7 гарантий, рабочего рестарта без окна |
| C5 — G-07 | Владелец внешней среды утверждает отдельный точный manifest: файлы/редакции/preflight SHA, разрешённые участки, patch SHA, операции для новых файлов, локальная копия/rollback, исполнитель/контур/окно/проверяющий. Записать разрешение apply **и** rollback, не выводить его из commit/push | Наблюдённые пути §4 не whitelist. Нет рабочих внешних изменений ни в одном coding-stage |
| C6 — эксплуатация/backup/MET | Принят пилот без числового SLO и **без регулярного резервирования**. Нельзя этим отменить BR-17/NFR-15/FR-24: аналитик/архитектор отражают границы требований; ручная безопасная копия перед изменениями и изолированный restore остаются. Принять профиль MET-01–07 и хранение ключей; числовых продуктовых гарантий не назначать | Нет scheduler backup, обещанных RPO/RTO/retention. Нельзя зачесть restore/restart по skip или стереть неизвестные ключи по TTL |
| C7 — профиль репозитория | В `repositories.json` для `vikunja-local` явно остаётся blocker mandatory local stand/test profile; Go/PostgreSQL/Kafka-профиль другого сервиса запрещён. Ведущий flow/владелец среды регистрируют и проверяют именно Vikunja-specific профиль с командами §6, cleanup и правильным staged registry до DoR; bootstrap реализации s01 не заменяет регистрацию | Нет DoR/Orca-start при отсутствующей обязательной конфигурации; отсутствие harness до s01 — планируемый результат, отсутствие допуска профиля — блокер |
| C8 — публикация/DoR | Владелец принимает точную редакцию этого плана с branch/remote; затем лично проходит native `dor-gate` (`accept-dor`) по актуальным требованиям/оценкам/плану | Ни ответа на вопрос, ни document review недостаточно для development. PR/GitHub capability проверяется отдельно до финальной публикации |

**Следующий native гейт после приёмки implementation-plan — `dor-gate`, требует `system-requirements`, `estimated-tasks`, `implementation-plan`, on-reject `system-analysis`.** Сейчас DoR не пройден. До положительного решения нужна профильная доработка/приёмка ТЗ и затронутых входов по C1–C3/C6, повторное рассмотрение затронутых требований через `requirements-review-gate`, C7 и актуальная приёмка плана. Ведущий flow выполняет штатный пересмотр и сверку native planner; этот worker не меняет последовательность, гейты, approvals и bypass. Для стадий, зависящих от ещё неготовой внешней области, C4/C5 должны быть закрыты до их разрешения; DoR не может молча их снять. Если для DoR нужен полный внешний manifest, гейт остаётся непройденным до его передачи.

Подтверждённое однопользовательское упрощение не исключает обязательные CRUD/сквозные сценарии, сохранность и восстановление. Изменившееся содержание оценивает профильный исполнитель; 82 SP старой условной декомпозиции не объявляются новой фиксированной оценкой, SP не пересчитываются в context_budget.

## 4. Внешние точки: кандидаты, не действующее разрешение

Все изменения сначала воспроизводятся **только как артефакты в vikunja-local** и тестируются на синтетической копии. Прямых исправлений чужих checkout coding-исполнителем нет. ENV ниже = `/Users/efimovivan/Projects/cto-agent-environment`. Наблюдённые хеши из E Evidence Register — исторические входы для preflight; заново внешний исходник в этой документной роли не обследовался.

| Кандидат внешней точки | Изменение/владелец артефакта | Наблюдённый исходный SHA256 / ограничение |
| --- | --- | --- |
| `ENV/plugins/personal-tasks/dashboard/plugin_api.py` | s06: узкая серверная проверка/вызов bridge, заполнение tracker/backlink через существующие slots | `938dc648daee8f5484a76de6bac0ed86f79b28463c89a1d17c6af02e833b0f51` |
| `ENV/scripts/ea_core.py` | s06: совместимый durable outbox/recover/backlink; не миграция всех старых accepted | `23deacba5cca06d314c5ec5717bbb3f6dd7f074974670324ab7f64baaecf10ef` |
| `ENV/plugins/personal-tasks/dashboard/dist/index.js` | s07: только подтверждение/состояния/переход; воспроизводимый build/transform и контроль результата | `bb0144e8bcb343880a334b342f74cdcbdf08a5d8996e96d5442b503a77b822e8`; источник сборки и допустимый способ правки должны войти в C5; слепой edit minified bundle запрещён |
| `ENV/scripts/ea_vikunja_install.py` | s08: reference/reuse, только отдельно утверждённая delta при необходимости | `4cb88493f912df09b78fa3ab6ce8c267e10e2d7485aa2bbd9fb1c7a726e823b4`; не разрешено автоматически заменять установщик |
| LaunchAgent `local.cto.vikunja`, его plist и manifest установки | s08: сверка/идемпотентное reuse; полный абсолютный plist/config/data path ещё должен быть передан C4/C5 | Пути plist и безопасной копии здесь не придуманы; без exact manifest apply заблокирован |
| MCP registration профиля default на Mini и клиентская конфигурация Codex/MacBook | s05/s08: зарегистрировать stdio/существующий SSH-маршрут без нового writer | Exact config file/исходный SHA и рабочие origins не переданы; C3/C5. Другие профили, SSH rules, host keys и маршрутизация не меняются |

Host runtime `H=/Users/efimovivan/.hermes/hermes-agent` **не включён в минимальную область**. В частности `plugin_host_http_auth.py` (наблюдался `d9544c1079c02cb2b465f8cdd10c98cb82f01a3e2f04f6ec0aa06a214aa6b4a3`) и `plugin_host_child.py` (`ac8ffe2dbdf5bf93ab4aafe16c497387903dc05cd4211c786c58dc6198258551`) не патчатся ради переноса principal. Выбранный P10 in_process не доказывает hosted. Любая необходимость править H — новое архитектурное решение и явное расширение C5. Исходники/миграции БД Vikunja для CAS исключены полностью.

Рабочие DB, вложения, `.analysis/state.json`, конфигурация с секретами и расшифровки не являются patch targets. Их runtime-запись допустима только штатным авторизованным поведением в отдельно допущенном контуре; нельзя заменять файл рабочего состояния синтетическим. `GET /snapshot` по R3 вызывает recover/save, потому не используется как read-only readiness.

### Протокол применения и обратного перехода (отдельная эксплуатационная работа)

1. До действия — точное человеческое разрешение C4/C5, подтверждённый SHA stage, независимые QA/review, локально защищённая согласованная копия, перечень собственных ресурсов, actual hashes и отсутствие drift/конкурирующего экземпляра. Несовпадение — stop, не fuzzy patch/force overwrite.
2. Сначала dry-run всех патчей в копии approved synthetic baseline, проверка нового/старого expected SHA, тесты decision/recover/Markdown и отрицательных Origin/principal. Для незаданного источника UI build — stop до C5. Apply/rollback-инструменты по умолчанию dry-run, не получают токены в argv.
3. Рабочее применение выполняет отдельно допущенная эксплуатационная роль под coordinator, не Cursor coding-worker и не этот документ. Существующий SSH-маршрут не перенастраивать. Сохранять единственный supervisor/экземпляр, не удалять старую подготовку. Если нужен несовместимый переход — отдельное решение, не этот план.
4. После применения читать **точные** целевые файлы/expected SHA, runtime readiness/permissions и помеченные синтетические ID/backlinks. Проверить обычный CRUD и отсутствие development-run; не принимать реальное предложение. Только этот read-back подтверждает эффект apply, не rc=0 инструмента.
5. При провале — остановить новый dispatch, сохранить pending/receipts/mappings/факт принятия. Откатить только свои code/config изменения к защищённой копии после проверки current SHA; не откатывать рабочую БД назад и не удалять созданные личные задачи. Если есть несовместимость durable schema — writes выключены до согласованного восстановления, не стирать журнал. Проверить старые сценарии/ссылки и единственность сервиса.
6. Рестарт Mini — отдельное согласованное окно после применения; проверка **после login**. Ожидание FileVault/сон фиксируются как ограничения, настройки power/FileVault не меняются. Успешный restart тестовой Vikunja не равен restart Mini.

## 5. Последовательность и ownership этапов

Все coding-stages выполняются **после** актуальных требований/приёмки плана/DoR; порядок линейный по таблице. Тесты каждого stage входят в тот же diff, а формальный `write-autotests` и итоговые QA native flow не исключаются. Каждый stage — один независимо проверяемый diff/commit; следующий начинается только после независимых QA/review и подтверждённого remote SHA предыдущего. Промпт, точные paths/checks/acceptance/commit/context_budget находятся в единственном JSON §8 и являются частью поручения.

| Stage | Результат / пакет декомпозиции | Входы и дополнительные условия | Исключения |
| --- | --- | --- | --- |
| s01-stand | Собственный изолированный runner/фикстуры, readiness и аварийный cleanup; I1 | Все вводные §1–3; C7 допуск профиля, известный бинарный SHA | Рабочая служба, внешние edits, автоматический download/обновление runtime |
| s02-api | Внутренний API-клиент/даты/read-back; I4 | s01, C1; G03 Evidence 02 | Нет внешнего MCP/bridge writer до s04, CAS, v1 POST update |
| s03-journal | Долговечные операции/сериализация; I5 | s02, C1: принятые issuer/формат/retention/ошибки | Нет сетевых мутаций, TTL-удаления unknown и мастер-копии статуса |
| s04-recovery | Единый dispatcher/create/exact reconcile; I6 и соединение I4/I5 | s03, C1; реальные crash/late-write сценарии реализации | Нет повторного create по пустому поиску, слепого complete или Hermes UI |
| s05-mcp | Полный stdio MCP чата; I7 | s04, C3; P9 reference, approved contract и launch recipe | Нет native HTTP второго writer, правок клиентских конфигов или SSH |
| s06-hermes-bridge | Backend bridge/outbox/backlink + воспроизводимый patch; I8 | s05, C2, approved synthetic source baseline из C5; P10 in_process reference | Нет подмены principal bool, hosted host patch, auto-export старых accepted, external apply |
| s07-hermes-ux | Узкая навигация/подтверждение/состояния + patch; I9 | s06, UX/mockups, C2/C3/C5 build recipe | Нет нового трекера/редизайна встреч, ручного edit чужого dist |
| s08-lifecycle | Reuse/preflight/access/service/apply/rollback **артефакты**; I2/I3 | s07, C4/C5, согласованное reuse и post-login | Не рабочая установка/рестарт, не новый supervisor, не миграция |
| s09-restore | Ручная согласованная копия/изолированный restore и сверка; I10 | s08, C6 и согласованный состав копии | Нет регулярного backup, обещанных RPO/RTO, restore поверх рабочего контура |
| s10-e2e | Полный synthetic E2E/регресс/эксплуатационный verify harness; V1/V2 инструменты | s09, актуальный профиль и approved synthetic Hermes baseline | Не независимая самоприёмка; owner/device проверки не симулируются |
| s11-handoff | Проверенные инструкции и накопительный пакет свидетельств; V3 | s10, отдельное применение §4, независимые итоговые QA/review и разрешённые device/owner/restart прогоны | Нет выдуманных отчётов/ссылок, PR, UAT/DoD или записи approvals |

**Переиспользование:** принятый A1 и исполненный R2 не повторяются «с нуля». Реализация проверяет свои новые артефакты на их SHA. Служба/transport этапов s08 не являются уже выполненными работами; отсутствие external manifest останавливает зависимую стадию, а не заменяется выдуманным path.

Логика, API-клиент, SQLite-журнал, CLI и portable runner планируются на Python 3 со стандартной библиотекой (`unittest`, `sqlite3`, `zoneinfo`, HTTP/JSON). Это внутренняя структура нового пустого репозитория, не выбор нового сетевого контракта. При необходимости сторонней MCP/UI-зависимости исполнитель обосновывает/фиксирует её версию в task diff и согласует изменение поведения; сетевые установки в процессе проверок не выполняются молча. UI patch следует существующей утверждённой build recipe, новый frontend stack не вводится.

## 6. Контракт команд проверки и стенда

`checks` JSON — **argv**, не shell-команды и не уже выполненные тесты. Рабочий каталог каждого вызова — назначенная worktree. s01 создаёт `scripts/ea010_verify.py`; следующие stages добавляют настоящие suites `api`, `journal`, `recovery`, `mcp`, `hermes-bridge`, `hermes-ux`, `lifecycle`, `restore`, `all`. `--mode isolated` — default-safe контур, новый отдельный каталог и собственный процесс на каждый прогон. Неизвестная suite, отсутствующая зависимость, неисполненный обязательный сценарий или cleanup failure дают ненулевой rc; никакого success по пустому набору/skip. Отдельные `unittest discover -s tests -p test_*.py` в JSON запускают указанный файл; runner обязан проверять наличие/ненулевой счётчик тестов и не подменять stand этими unit-тестами.

Перед запуском оператор передаёт через локальную закрытую конфигурацию/окружение **только пути**, а не секреты: проверенный binary, approved synthetic Hermes baseline, допущенный run-root, режим/device manifest. Runner читает токены из локального 0600 файла/защищённого канала на Mini; не печатает их и не передаёт в argv. Пустая конфигурация — stop. Автоскачивания/установки по непроверенной версии нет. При bootstrap s01 заранее отсутствующий suite не добавляется в список успешных проверок.

Планируемый binary v2.8.0 SHA256 `02b0b3966ab201c3bea40edfe32a1c470214671cba99383cb262d36012acc000` из переданных исследований; оригинальность upstream здесь не устанавливается. До запуска сверить exact file/hash/version. **Не собирать Vikunja ради CAS.** Внутренний client может использовать проверенные v1 create/list (`PUT /api/v1/projects/{id}/tasks`, `GET /api/v1/tasks` с `s/page/per_page/sort_by`) и v2 read/PATCH; смешение поколений не даёт права использовать разрушительный v1 POST update. Полную комбинацию/scopes проверяет suite api на реальном binary; final методы закрепляет C1.

Изолированный профиль: одна тестовая Vikunja, loopback 19345 (API/UI), 19346 (тестовый fault proxy), 19347 (synthetic Hermes при необходимости); занятый порт — stop, не kill владельца порта. Runtime — отдельно допущенная область `/Users/efimovivan/.ai-platform/work/ea010-r2/<run_id>/` либо её явно утверждённая замена в профиле C7, не рабочие данные/каталог Git. Каталоги 0700, чувствительные файлы 0600; synthetic prefix `EA010-SYNTHETIC-<run_id>`. Один прогон до 60 минут, 2 GiB диска и 2 GiB RSS собственных процессов — **пределы прежнего исследовательского профиля, предложены для утверждения C7, не утверждённый продуктовый SLO**. Runner отказывается, если эти пути/лимиты не разрешены актуальным профилем. Нет доступа — ненулевой результат, не обход sandbox другим каналом.

Runner пишет durable resource manifest до запуска: run_id, real PID/start identity, выделенные порты/каталоги/тестовые IDs и токены по внутренним handles. Не дампит реальные задачи/встречи. На SIGTERM/SIGHUP/исключении/нормальном выходе finally закрывает соединения, отзывает тестовые токены (с действующей тестовой сессией и проверкой результата), завершает только свои процессы, удаляет только свой synthetic root, читает отсутствие слушателей/ресурсов. SIGKILL/авария хоста: следующий запуск исполняет bounded cleanup из manifest с проверкой PID identity, без широкого pkill/rm. Сбой cleanup сохраняется failed даже если тесты зелёные; отдельный repair-run не переписывает историю исходного провала.

Артефакт прогона вне Git: время, проверенный source SHA/diff digest, binary/config/patch hashes без секретов, argv, rc, counts, synthetic ID/совпадения, безопасная chronology, границы пробы, cleanup/read-back. В Git допускается только обезличенный итоговый отчёт, без закрытой топологии. Публичные ссылки — шаблоны; **проверенные рабочие адреса** передаются владельцу в локальном закрытом приложении/receipt. Отчёт не должен называть незапущенный тест passed.

### Итоговые проверки после всех реализационных этапов

Независимый QA запускает `["python3", "scripts/ea010_verify.py", "--suite", "all", "--mode", "isolated"]`, затем в отдельно допущенном рабочем контуре — тот же runner с `--mode approved-pilot`. Последний режим создаётся s10; без exact C4/C5/C6/C7 и разрешённого окна/действия владельца он отказывает до мутаций. До готовности этих условий результат **not-run**, не pass. Проверки обязательны:

| Группа | Доказательство на проверяемом SHA |
| --- | --- |
| UI ↔ CHAT | Готовый UI CRUD → MCP видит тот же ID/актуальный статус; MCP create/find/PATCH/complete → UI тот же ID; реальные Mini/MacBook переходы. Не запускать development/run/dispatch |
| MEETING | Владелец лично принимает только помеченное синтетическое предложение в допущенном контуре; происхождение/подтверждённый или пустой срок/обе ссылки. Непринятие, чужой principal, неверный Origin, stale revision и client accepted=true не создают задачу |
| RELIABILITY | Повторы/два конкурента/новые одинаковые названия; late create после timeout и промежуточного нуля; SIGKILL до HTTP/после commit до receipt/после receipt до backlink; unknown 0/несколько/порча/удаление не разрешают create. Crash реализации не заменяется staged interruption fixture |
| UPDATE/DATES/ACCESS | v2 PATCH omission/null/marker, date-only и повтор через MSK midnight, 304+GET, recurring один complete и unknown без повторного сдвига due; read-only/bad/revoked scopes, отказ без мутации. UI concurrency — документированное допущение, не обещание CAS |
| PRESERVATION/UX | Старые синтетические Markdown/accepted/state не мигрируют/не диспатчатся при просмотре; hash до/после сохранён на этом прогоне; regression decision/recover, подтверждение срока, busy/ошибки, keyboard/focus/Escape и 390 px |
| RESTART/RESTORE | Отдельно разрешённый restart Mini с post-login автозапуском, прежние synthetic ID/вложение/hash/журнал/backlink; изолированный restore согласованного набора/старой копии и сверка до writes, без overwrite рабочих данных |
| CLEANUP/DELIVERY | Cleanup после **каждого** прогона, включая fault/error/signal; отсутствие чужих ресурсов, секретов/реального содержания, единственность рабочего сервиса; независимые результаты и ограничения на точный SHA |

MET-01/02/06/07: предложен измерительный профиль личного пилота — последовательный CRUD, отдельно локальный и SSH путь, плюс два конкурента только для повтора одной операции; выборку/длительность фиксирует C7, порогов p95/uptime/CPU здесь нет. MET-03 от login до готовности; MET-04/05 только измеренная ручная копия/restore без регулярности и гарантий RPO/RTO. Unknown keys не удаляются автоматически; отсутствие SLA не разрешает дубль. Точные технические timeout/backoff/budget/retention закрепляются C1/C6 до кода, не выдаются за согласованные числа этим планом.

## 7. Публикационный контракт и независимость

Приёмка владельцем **точной редакции этого плана** явно разрешает coordinator helper после успешных независимых QA/review каждого допущенного этапа сделать его commit и **обычный push только** `origin` (`git@github.com:iefimov1008/vikunja-local.git`), ветка `hermes/install-vikunja-tracker`. Это будущая область stage-push, не текущая приёмка. Ограничение local-only текущего document worker сохраняется. Автор stage сам не commit/push и не принимает свой код.

Coordinator в Orca — Codex, реализация — Cursor, QA и review — отдельные Codex-исполнители, не автор diff. Анализ текста предпочтительно Codex/Sol. Каждый получает только текущий stage, принятые входы/хеши, exact предыдущий remote SHA и ограничения. Перед публикацией helper проверяет ветку/remote URL/actual diff, отсутствие чужих/секретных файлов, успешные checks/cleanup, независимые решения на эту редакцию, отсутствие remote divergence. Неизвестный результат push — сначала remote read-back, не новый слепой push; следующий stage получает подтверждённый remote SHA. Изменение редакции после review требует повторной проверки. Дополнительный файл в своём репозитории объясняется в отчёте и проверяется всем diff; изменение требований/внешней области требует нового решения.

Первый commit может требовать ранее принятых документов/состояния, сейчас они untracked. Их состав отдельно сверяет helper с flow и точными приёмками; этот план не разрешает `git add .`, подмену process-state или приписывание неизменённых принятых файлов автору stage. Каждый stage публикуется отдельным обозримым commit с указанным message и receipt; документы текущего этапа не коммитятся его worker.

Force push, default/protected branch, merge, автоматический deploy и GitHub PR этим документом **не разрешаются**. В профиле репозитория отмечена неподключённая GitHub final-publication capability; её штатная настройка/проверка — ведущий flow до `open-merge-request`, не использование GitLab другого проекта. После всех stages, итогового локального QA/cleanup и review накопительного отчёта владелец отдельно разрешает PR через native `open-merge-request`. Внутренние Orca проверки не заменяют `write-autotests`, `code-review-gate`, `functional-testing`, `defects-gate`, UAT/DoD. UAT и DoD — лично `iiefim10` в Hermes. Общая EA-010 закрывается только подтверждённым штатным завершением, не этим планом/коммитом.

## 8. Машиночитаемые этапы и полные промпты

Ровно один JSON-блок `implementation-plan/v1`. Перечисленные stages — проект заданий после допуска, не уже выданные dispatch. Пути относительны worktree и не включают внешние targets. `context_budget` — ориентир токенов контекста, не гарантия расхода или оценка SP. При невозможности удержать один проверяемый diff coordinator возвращает план на явное уточнение, не сливает stages без review.

```json
{
  "schema": "implementation-plan/v1",
  "publication": {
    "mode": "stage-push",
    "remote": "origin",
    "remote_url": "git@github.com:iefimov1008/vikunja-local.git",
    "branch": "hermes/install-vikunja-tracker"
  },
  "stages": [
    {
      "id": "s01-stand",
      "title": "Изолированный стенд и аварийный cleanup",
      "prompt": "process: /Users/efimovivan/Documents/DevProjects/worktrees/vikunja-local-ea010-install-integration/vikunja-local/install-vikunja-tracker/docs/process/t_606468bd.md#development\nEA-010, native t_606468bd. Исполни только этот stage как Cursor implementation под Codex coordinator в /Users/efimovivan/Documents/DevProjects/worktrees/vikunja-local-ea010-install-integration/vikunja-local/install-vikunja-tracker, ветка hermes/install-vikunja-tracker. Сначала прочитай применимые AGENTS/ai-developer, docs/artifacts/t_606468bd-implementation-plan.md целиком (паспорт входов, C1–C8, внешняя область, профиль и publication), docs/requirements/t_606468bd-system-requirements.md, ADR/НФТ и указанные в плане БТ/UX/оценённые пакеты; проверь принятые актуальные SHA и допуск stage/DoR у coordinator. Сохрани AI_PLATFORM_PROCESS_DIR=/Users/efimovivan/Projects/cto-agent-environment/ops/real/process-profile-staged. При незакрытом условии не начинай зависимый код, передай конкретный вопрос coordinator; не принимай и не редактируй входные документы/process-state. Только свой task diff, синтетические тесты, без секретов/реальных задач/встреч, внешних edits, commit/push/PR/merge или самоприёмки. Новых агентов/работ по собственной инициативе не запускай. Вход: допущенный C7 профиль и binary v2.8.0 с SHA из §6. Создай standalone Python runner и tests/support для реального изолированного экземпляра, не stub. Реализуй manifest ресурсов до запуска, безопасный run-root 0700, файлы 0600, проверку hash/version/портов/лимитов, readiness, synthetic fixtures и журнал argv/rc. Не меняй рабочую службу и не скачивай бинарник автоматически. Проверь normal exit, exception, занятый порт, неверный hash и SIGTERM/SIGHUP; после SIGKILL очистку выполняет следующий контролируемый запуск из manifest с проверкой PID identity. Отзыв synthetic token должен иметь действующую сессию и read-back результата. Создай suite stand и тест её реального цикла. Подготовь интерфейс регистрации будущих suites; неизвестная/пустая suite отказывает, не считается успешной. .gitignore защищает runtime/secrets, но не заменяет их размещение вне Git. Исключены client CRUD, внешняя конфигурация и рабочие данные. Выполни указанные checks этого stage, сохрани argv/rc/ненулевое покрытие, source SHA/diff digest, реальные synthetic IDs и cleanup каждого прогона. В отчёте назови изменённые пути, решения, ограничения и зависимости; не выдавай mock/skip за стенд. Отдай diff coordinator для отдельных независимых QA/review; публикация только helper после них.",
      "paths": [
        "scripts/ea010_verify.py",
        "tests/support",
        "tests/test_stand.py",
        ".gitignore"
      ],
      "checks": [
        [
          "python3",
          "-m",
          "unittest",
          "discover",
          "-s",
          "tests",
          "-p",
          "test_stand.py"
        ],
        [
          "python3",
          "scripts/ea010_verify.py",
          "--suite",
          "stand",
          "--mode",
          "isolated"
        ],
        [
          "git",
          "diff",
          "--check"
        ]
      ],
      "acceptance": "Настоящий v2.8.0 запускается только в изоляции, readiness прочитан; fault/signal/port/hash сценарии проверены; все собственные ресурсы очищены с read-back. Ошибка cleanup/нет binary дают failure, не skip.",
      "commit_message": "test: add isolated Vikunja stand and verified cleanup",
      "context_budget": 120000
    },
    {
      "id": "s02-api",
      "title": "Стандартный API-клиент и даты без CAS",
      "prompt": "process: /Users/efimovivan/Documents/DevProjects/worktrees/vikunja-local-ea010-install-integration/vikunja-local/install-vikunja-tracker/docs/process/t_606468bd.md#development\nEA-010, native t_606468bd. Исполни только этот stage как Cursor implementation под Codex coordinator в /Users/efimovivan/Documents/DevProjects/worktrees/vikunja-local-ea010-install-integration/vikunja-local/install-vikunja-tracker, ветка hermes/install-vikunja-tracker. Сначала прочитай применимые AGENTS/ai-developer, docs/artifacts/t_606468bd-implementation-plan.md целиком (паспорт входов, C1–C8, внешняя область, профиль и publication), docs/requirements/t_606468bd-system-requirements.md, ADR/НФТ и указанные в плане БТ/UX/оценённые пакеты; проверь принятые актуальные SHA и допуск stage/DoR у coordinator. Сохрани AI_PLATFORM_PROCESS_DIR=/Users/efimovivan/Projects/cto-agent-environment/ops/real/process-profile-staged. При незакрытом условии не начинай зависимый код, передай конкретный вопрос coordinator; не принимай и не редактируй входные документы/process-state. Только свой task diff, синтетические тесты, без секретов/реальных задач/встреч, внешних edits, commit/push/PR/merge или самоприёмки. Новых агентов/работ по собственной инициативе не запускай. Вход: s01, закрытый C1 и принятый однопользовательский G-03. Создай внутренний Python клиент stock API: create/list/read по принятому wire, update/complete только v2 PATCH application/merge-patch+json. Только явно изменённые поля, token/scopes из закрытого Mini storage, без токенов в argv/логах. В одной create передай утверждённый маркер/происхождение, не отдельным label-request. Success после GET того же ID. Отдели отказ/пустой поиск/сеть; проверь pagination и выбор ID. Реализуй MSK reference при подтверждении, сохранение абсолютного due, date-only 23:59:59 MSK, omission/null и нормализацию zero date. 304 требует GET. Завершение recurring возвращает фактический перенос срока, не бесконечный retry до done=true; unknown mutation сообщает unknown без повтора. CAS и v1 POST update запрещены. Пока s03/s04 не готовы, клиент доступен только внутренним синтетическим проверкам, не как внешний writer. Добавь real API suite с read-only/bad/revoked правами, omission/marker, null, границей суток и recurring. Выполни указанные checks этого stage, сохрани argv/rc/ненулевое покрытие, source SHA/diff digest, реальные synthetic IDs и cleanup каждого прогона. В отчёте назови изменённые пути, решения, ограничения и зависимости; не выдавай mock/skip за стенд. Отдай diff coordinator для отдельных независимых QA/review; публикация только helper после них.",
      "paths": [
        "vikunja_local/__init__.py",
        "vikunja_local/api.py",
        "vikunja_local/dates.py",
        "tests/test_api.py",
        "tests/support/api_suite.py"
      ],
      "checks": [
        [
          "python3",
          "-m",
          "unittest",
          "discover",
          "-s",
          "tests",
          "-p",
          "test_api.py"
        ],
        [
          "python3",
          "scripts/ea010_verify.py",
          "--suite",
          "api",
          "--mode",
          "isolated"
        ],
        [
          "git",
          "diff",
          "--check"
        ]
      ],
      "acceptance": "Реальный API round-trip подтверждает поля/срок/ID; 401 не меняет объект; 304 сверяется GET; recurring отправлен один раз. Нет обещания защиты от UI-гонки и внешнего обхода журнала.",
      "commit_message": "feat: add stock Vikunja API client and confirmed MSK dates",
      "context_budget": 150000
    },
    {
      "id": "s03-journal",
      "title": "Долговечное намерение и сериализация повторов",
      "prompt": "process: /Users/efimovivan/Documents/DevProjects/worktrees/vikunja-local-ea010-install-integration/vikunja-local/install-vikunja-tracker/docs/process/t_606468bd.md#development\nEA-010, native t_606468bd. Исполни только этот stage как Cursor implementation под Codex coordinator в /Users/efimovivan/Documents/DevProjects/worktrees/vikunja-local-ea010-install-integration/vikunja-local/install-vikunja-tracker, ветка hermes/install-vikunja-tracker. Сначала прочитай применимые AGENTS/ai-developer, docs/artifacts/t_606468bd-implementation-plan.md целиком (паспорт входов, C1–C8, внешняя область, профиль и publication), docs/requirements/t_606468bd-system-requirements.md, ADR/НФТ и указанные в плане БТ/UX/оценённые пакеты; проверь принятые актуальные SHA и допуск stage/DoR у coordinator. Сохрани AI_PLATFORM_PROCESS_DIR=/Users/efimovivan/Projects/cto-agent-environment/ops/real/process-profile-staged. При незакрытом условии не начинай зависимый код, передай конкретный вопрос coordinator; не принимай и не редактируй входные документы/process-state. Только свой task diff, синтетические тесты, без секретов/реальных задач/встреч, внешних edits, commit/push/PR/merge или самоприёмки. Новых агентов/работ по собственной инициативе не запускай. Вход: s02 и закрытый C1 с issuer/ключами/retention/границами durable-save. Реализуй SQLite-журнал интеграции на Mini вне Git: операции, подтверждённое намерение, receipt/mapping и состояние доставки, не мастер done. Формат публичных IDs берётся только из принятого контракта. Уникальность операции/проверка несовместимого payload, durable транзакция до HTTP, межпроцессная сериализация и fail-closed на corrupt/unavailable/full storage. Сохраняй возможность сверки после разрыва: наличие intent само по себе не доказывает, что HTTP не отправлялся. Не удаляй unknown/ключи автоматически по TTL. Используй реальные subprocess crash/concurrent проверки и недоступное хранилище; сеть в этом stage не вызывается. Recovery схемы журнала не должен терять receipts/history; несовместимая миграция требует решения, не очистки. Добавь suite journal и проверь сохранность после SIGKILL, конфликт ключа/payload, два конкурента и отказ durable-save. Выполни указанные checks этого stage, сохрани argv/rc/ненулевое покрытие, source SHA/diff digest, реальные synthetic IDs и cleanup каждого прогона. В отчёте назови изменённые пути, решения, ограничения и зависимости; не выдавай mock/skip за стенд. Отдай diff coordinator для отдельных независимых QA/review; публикация только helper после них.",
      "paths": [
        "vikunja_local/journal.py",
        "vikunja_local/storage.py",
        "tests/test_journal.py",
        "tests/support/journal_suite.py"
      ],
      "checks": [
        [
          "python3",
          "-m",
          "unittest",
          "discover",
          "-s",
          "tests",
          "-p",
          "test_journal.py"
        ],
        [
          "python3",
          "scripts/ea010_verify.py",
          "--suite",
          "journal",
          "--mode",
          "isolated"
        ],
        [
          "git",
          "diff",
          "--check"
        ]
      ],
      "acceptance": "После restart доступны прежние ключи/намерения/receipts; два процесса не обходят сериализацию; отказ журнала не допускает dispatch. Все тесты имеют ненулевой набор, нет сети и потери unknown по retention.",
      "commit_message": "feat: persist integration intents and serialize repeated operations",
      "context_budget": 130000
    },
    {
      "id": "s04-recovery",
      "title": "Единый dispatch и сверка неизвестного исхода",
      "prompt": "process: /Users/efimovivan/Documents/DevProjects/worktrees/vikunja-local-ea010-install-integration/vikunja-local/install-vikunja-tracker/docs/process/t_606468bd.md#development\nEA-010, native t_606468bd. Исполни только этот stage как Cursor implementation под Codex coordinator в /Users/efimovivan/Documents/DevProjects/worktrees/vikunja-local-ea010-install-integration/vikunja-local/install-vikunja-tracker, ветка hermes/install-vikunja-tracker. Сначала прочитай применимые AGENTS/ai-developer, docs/artifacts/t_606468bd-implementation-plan.md целиком (паспорт входов, C1–C8, внешняя область, профиль и publication), docs/requirements/t_606468bd-system-requirements.md, ADR/НФТ и указанные в плане БТ/UX/оценённые пакеты; проверь принятые актуальные SHA и допуск stage/DoR у coordinator. Сохрани AI_PLATFORM_PROCESS_DIR=/Users/efimovivan/Projects/cto-agent-environment/ops/real/process-profile-staged. При незакрытом условии не начинай зависимый код, передай конкретный вопрос coordinator; не принимай и не редактируй входные документы/process-state. Только свой task diff, синтетические тесты, без секретов/реальных задач/встреч, внешних edits, commit/push/PR/merge или самоприёмки. Новых агентов/работ по собственной инициативе не запускай. Вход: s03 и принятый C1, свидетельства G01/G02. Соедини внутренние api/journal в единственный интеграционный writer: durable intent/claim до первого разрешённого HTTP, create с утверждённым маркером в том же запросе, receipt/read-back. Для повтора с возможной записью — только exact reconcile в разрешённом проекте/полной paging области; substring-приманки исключаются точным разбором маркера. Одно точное совпадение читать и связать, несколько — конфликт, ноль после unknown/порчи/удаления — сохранить неизвестность и запретить create. Не использовать UID/labels как атомарный JSON correlation. Unknown update/complete сверять без повторной отправки, особенно recurring. Fault proxy должен показать timeout, промежуточный ноль и поздний исходный ID с create_count=1. Убей реальный свой процесс до HTTP, после commit до receipt и после receipt до link callback, восстанови двумя процессами без нового create. Hermes backlink пока controlled callback, не заявляй production bridge готовым. Добавь suite recovery и fail-closed права/журнал/пустая или неполная сверка. Выполни указанные checks этого stage, сохрани argv/rc/ненулевое покрытие, source SHA/diff digest, реальные synthetic IDs и cleanup каждого прогона. В отчёте назови изменённые пути, решения, ограничения и зависимости; не выдавай mock/skip за стенд. Отдай diff coordinator для отдельных независимых QA/review; публикация только helper после них.",
      "paths": [
        "vikunja_local/dispatcher.py",
        "vikunja_local/reconcile.py",
        "tests/test_recovery.py",
        "tests/support/recovery_suite.py"
      ],
      "checks": [
        [
          "python3",
          "-m",
          "unittest",
          "discover",
          "-s",
          "tests",
          "-p",
          "test_recovery.py"
        ],
        [
          "python3",
          "scripts/ea010_verify.py",
          "--suite",
          "recovery",
          "--mode",
          "isolated"
        ],
        [
          "git",
          "diff",
          "--check"
        ]
      ],
      "acceptance": "Одна логическая операция даёт один synthetic ID при повторах/конкуренции/crash/late-write; distinct намерения с одинаковым title разные. Ноль/несколько/маркер/удаление не приводят к create. Unknown recurring не сдвигает due повторно.",
      "commit_message": "feat: reconcile uncertain writes without duplicate task creation",
      "context_budget": 180000
    },
    {
      "id": "s05-mcp",
      "title": "MCP stdio чата через единственный writer",
      "prompt": "process: /Users/efimovivan/Documents/DevProjects/worktrees/vikunja-local-ea010-install-integration/vikunja-local/install-vikunja-tracker/docs/process/t_606468bd.md#development\nEA-010, native t_606468bd. Исполни только этот stage как Cursor implementation под Codex coordinator в /Users/efimovivan/Documents/DevProjects/worktrees/vikunja-local-ea010-install-integration/vikunja-local/install-vikunja-tracker, ветка hermes/install-vikunja-tracker. Сначала прочитай применимые AGENTS/ai-developer, docs/artifacts/t_606468bd-implementation-plan.md целиком (паспорт входов, C1–C8, внешняя область, профиль и publication), docs/requirements/t_606468bd-system-requirements.md, ADR/НФТ и указанные в плане БТ/UX/оценённые пакеты; проверь принятые актуальные SHA и допуск stage/DoR у coordinator. Сохрани AI_PLATFORM_PROCESS_DIR=/Users/efimovivan/Projects/cto-agent-environment/ops/real/process-profile-staged. При незакрытом условии не начинай зависимый код, передай конкретный вопрос coordinator; не принимай и не редактируй входные документы/process-state. Только свой task diff, синтетические тесты, без секретов/реальных задач/встреч, внешних edits, commit/push/PR/merge или самоприёмки. Новых агентов/работ по собственной инициативе не запускай. Вход: s04, закрытый C3 tools/schema/errors/version и reconnect identity; P9 read-транспорт как reference. Реализуй stdio/JSON-RPC adapter по принятой спецификации: initialize/tools/list и create/find/update/complete/reconcile через dispatcher, не прямой обход API. Tool names/operation issuer и error wire не придумывай при отсутствии утверждённого контракта. Стабильность ключа при transport retry/reconnect не строится только на JSON-RPC ID и не требует ручного технического ID от владельца. Ответ содержит read-back ID и проверенный URL из допущенной конфигурации либо явное unknown/denied/unavailable/ambiguity. Проверяй scope/поля, не запускай dev-flow/coding-agent. Config example без секретов и выдуманных рабочих URL, только recipe для Mini/существующего SSH stdio. Реальный framed exchange/subprocess restart и повтор одной команды должны вернуть тот же ID; негативные права не меняют объект. Не редактируй default/MacBook config и SSH; живой Codex/device прогон — отдельное допущенное QA, не фабрикация в suite. Выполни указанные checks этого stage, сохрани argv/rc/ненулевое покрытие, source SHA/diff digest, реальные synthetic IDs и cleanup каждого прогона. В отчёте назови изменённые пути, решения, ограничения и зависимости; не выдавай mock/skip за стенд. Отдай diff coordinator для отдельных независимых QA/review; публикация только helper после них.",
      "paths": [
        "vikunja_local/mcp.py",
        "scripts/ea010_mcp.py",
        "config/mcp.example.json",
        "tests/test_mcp.py",
        "tests/support/mcp_suite.py"
      ],
      "checks": [
        [
          "python3",
          "-m",
          "unittest",
          "discover",
          "-s",
          "tests",
          "-p",
          "test_mcp.py"
        ],
        [
          "python3",
          "scripts/ea010_verify.py",
          "--suite",
          "mcp",
          "--mode",
          "isolated"
        ],
        [
          "git",
          "diff",
          "--check"
        ]
      ],
      "acceptance": "Полные JSON-RPC CRUD и reconnect/repeat проверены на изолированной Vikunja через dispatcher; реальные ID/read-back; нет development-вызовов и второго writer. P9 не объявляется доказательством CRUD новой реализации.",
      "commit_message": "feat: expose personal tasks through durable stdio MCP adapter",
      "context_budget": 160000
    },
    {
      "id": "s06-hermes-bridge",
      "title": "Долговечный мост принятия и backend patch",
      "prompt": "process: /Users/efimovivan/Documents/DevProjects/worktrees/vikunja-local-ea010-install-integration/vikunja-local/install-vikunja-tracker/docs/process/t_606468bd.md#development\nEA-010, native t_606468bd. Исполни только этот stage как Cursor implementation под Codex coordinator в /Users/efimovivan/Documents/DevProjects/worktrees/vikunja-local-ea010-install-integration/vikunja-local/install-vikunja-tracker, ветка hermes/install-vikunja-tracker. Сначала прочитай применимые AGENTS/ai-developer, docs/artifacts/t_606468bd-implementation-plan.md целиком (паспорт входов, C1–C8, внешняя область, профиль и publication), docs/requirements/t_606468bd-system-requirements.md, ADR/НФТ и указанные в плане БТ/UX/оценённые пакеты; проверь принятые актуальные SHA и допуск stage/DoR у coordinator. Сохрани AI_PLATFORM_PROCESS_DIR=/Users/efimovivan/Projects/cto-agent-environment/ops/real/process-profile-staged. При незакрытом условии не начинай зависимый код, передай конкретный вопрос coordinator; не принимай и не редактируй входные документы/process-state. Только свой task diff, синтетические тесты, без секретов/реальных задач/встреч, внешних edits, commit/push/PR/merge или самоприёмки. Новых агентов/работ по собственной инициативе не запускай. Вход: s05, закрытый C2 и утверждённый synthetic baseline исходников C5 с точными hashes. Создай узкий bridge и воспроизводимый backend patch только в этом репозитории. Режим in_process: настоящий server owner principal, exact Origin, revision и явное due confirmation проверяются до принятия/dispatch; bool admission/accepted=true не доказательство. По принятому контракту durable acceptance и outbox не теряют доставку в crash gap; stable event/proposal/operation issuer берётся из C2. Повтор принятия использует прежний event/receipt, lost backlink восстанавливается без create/повторного принятия. Сохрани Markdown/решения/старые accepted без автоматической доставки при чтении/recover. Не правь H runtime и внешние checkout. В approved synthetic copy проверь 401/403/stale/неподтверждённый due/поддельное accepted, реальные crash между acceptance/outbox/dispatch/backlink и rollback без потери pending. Без передачи source baseline не извлекай рабочие данные ради теста. Manifest содержит from/to hashes и точные участки; патч production не применяется. Личное принятие владельца на будущей сборке остаётся отдельным QA, не заменяется spike P10. Выполни указанные checks этого stage, сохрани argv/rc/ненулевое покрытие, source SHA/diff digest, реальные synthetic IDs и cleanup каждого прогона. В отчёте назови изменённые пути, решения, ограничения и зависимости; не выдавай mock/skip за стенд. Отдай diff coordinator для отдельных независимых QA/review; публикация только helper после них.",
      "paths": [
        "vikunja_local/hermes_bridge.py",
        "integration/hermes/backend.patch",
        "integration/hermes/backend-manifest.json",
        "tests/test_hermes_bridge.py",
        "tests/support/hermes_bridge_suite.py"
      ],
      "checks": [
        [
          "python3",
          "-m",
          "unittest",
          "discover",
          "-s",
          "tests",
          "-p",
          "test_hermes_bridge.py"
        ],
        [
          "python3",
          "scripts/ea010_verify.py",
          "--suite",
          "hermes-bridge",
          "--mode",
          "isolated"
        ],
        [
          "git",
          "diff",
          "--check"
        ]
      ],
      "acceptance": "Изолированные настоящие router/Core с новым patch и stock Vikunja дают одну acceptance/outbox/task/backlink, негативы не создают задачу; legacy synthetic hashes до/после совпали, old accepted не dispatched. Crash/recovery сохраняют один ID. Нет внешних edits/подмены auth.",
      "commit_message": "feat: add authorized durable Hermes proposal delivery bridge",
      "context_budget": 190000
    },
    {
      "id": "s07-hermes-ux",
      "title": "Узкие UX-состояния и переход к Vikunja",
      "prompt": "process: /Users/efimovivan/Documents/DevProjects/worktrees/vikunja-local-ea010-install-integration/vikunja-local/install-vikunja-tracker/docs/process/t_606468bd.md#development\nEA-010, native t_606468bd. Исполни только этот stage как Cursor implementation под Codex coordinator в /Users/efimovivan/Documents/DevProjects/worktrees/vikunja-local-ea010-install-integration/vikunja-local/install-vikunja-tracker, ветка hermes/install-vikunja-tracker. Сначала прочитай применимые AGENTS/ai-developer, docs/artifacts/t_606468bd-implementation-plan.md целиком (паспорт входов, C1–C8, внешняя область, профиль и publication), docs/requirements/t_606468bd-system-requirements.md, ADR/НФТ и указанные в плане БТ/UX/оценённые пакеты; проверь принятые актуальные SHA и допуск stage/DoR у coordinator. Сохрани AI_PLATFORM_PROCESS_DIR=/Users/efimovivan/Projects/cto-agent-environment/ops/real/process-profile-staged. При незакрытом условии не начинай зависимый код, передай конкретный вопрос coordinator; не принимай и не редактируй входные документы/process-state. Только свой task diff, синтетические тесты, без секретов/реальных задач/встреч, внешних edits, commit/push/PR/merge или самоприёмки. Новых агентов/работ по собственной инициативе не запускай. Вход: s06, принятые UX/mockups и C2/C3/C5 с source/build recipe и baseline. Воспроизводимый patch существующей поверхности: Личные задачи открывает готовый UI отдельной вкладкой; DecisionDialog показывает title/source/context и пустой due/неподтверждённую подсказку, требует подтверждения срока/отсутствия и текущей revision. Изменение due снимает подтверждение; cancel/view не передают; busy/reopen не создают новую операцию. Различай принятие/доставку/выполнение, unknown/reconcile/lost backlink/denied/unavailable/deleted; последнее известное done помечается временем, не актуальным статусом. Готовность server-backed, не ready=true ради UI. Сохрани старые Markdown/вкладки встреч и длинные названия. Проверь клавиатуру, фокус/Escape/возврат, подписи не только цветом, 390 px и обе ссылки в допущенной synthetic browser surface. Не создавай новый frontend stack/трекер и не правь minified dist вручную; если recipe нет — C5 stop. Browser automation dependency/версия фиксируется reproducibly при необходимости, недоступный браузер не заменяется static-string тестом. Добавь suite и точный build/patch manifest; внешний apply запрещён. Выполни указанные checks этого stage, сохрани argv/rc/ненулевое покрытие, source SHA/diff digest, реальные synthetic IDs и cleanup каждого прогона. В отчёте назови изменённые пути, решения, ограничения и зависимости; не выдавай mock/skip за стенд. Отдай diff coordinator для отдельных независимых QA/review; публикация только helper после них.",
      "paths": [
        "integration/hermes/ui.patch",
        "integration/hermes/ui-manifest.json",
        "tests/test_hermes_ux.py",
        "tests/support/hermes_ux_suite.py"
      ],
      "checks": [
        [
          "python3",
          "-m",
          "unittest",
          "discover",
          "-s",
          "tests",
          "-p",
          "test_hermes_ux.py"
        ],
        [
          "python3",
          "scripts/ea010_verify.py",
          "--suite",
          "hermes-ux",
          "--mode",
          "isolated"
        ],
        [
          "git",
          "diff",
          "--check"
        ]
      ],
      "acceptance": "Настоящий синтетический browser-прогон нового UI подтверждает состояния, отсутствие повторного dispatch, keyboard/390 px и переходы одного ID; before/after legacy regression сохранён. Build/patch воспроизводим с проверенными hashes.",
      "commit_message": "feat: add minimal Hermes personal task navigation and delivery states",
      "context_budget": 160000
    },
    {
      "id": "s08-lifecycle",
      "title": "Артефакты безопасного reuse, доступа и rollback",
      "prompt": "process: /Users/efimovivan/Documents/DevProjects/worktrees/vikunja-local-ea010-install-integration/vikunja-local/install-vikunja-tracker/docs/process/t_606468bd.md#development\nEA-010, native t_606468bd. Исполни только этот stage как Cursor implementation под Codex coordinator в /Users/efimovivan/Documents/DevProjects/worktrees/vikunja-local-ea010-install-integration/vikunja-local/install-vikunja-tracker, ветка hermes/install-vikunja-tracker. Сначала прочитай применимые AGENTS/ai-developer, docs/artifacts/t_606468bd-implementation-plan.md целиком (паспорт входов, C1–C8, внешняя область, профиль и publication), docs/requirements/t_606468bd-system-requirements.md, ADR/НФТ и указанные в плане БТ/UX/оценённые пакеты; проверь принятые актуальные SHA и допуск stage/DoR у coordinator. Сохрани AI_PLATFORM_PROCESS_DIR=/Users/efimovivan/Projects/cto-agent-environment/ops/real/process-profile-staged. При незакрытом условии не начинай зависимый код, передай конкретный вопрос coordinator; не принимай и не редактируй входные документы/process-state. Только свой task diff, синтетические тесты, без секретов/реальных задач/встреч, внешних edits, commit/push/PR/merge или самоприёмки. Новых агентов/работ по собственной инициативе не запускай. Вход: s07, C4/C5 approved bindings/reuse/точный внешний manifest, post-login пилот. Создай воспроизводимые tools/шаблоны preflight/dry-run/apply/rollback, не выполняй рабочий apply. Сохрани существующий единственный supervisor local.cto.vikunja; никаких конкурирующих процессов над рабочими DB, смены runtime/FileVault/SSH/power settings и миграции. Проверяй absolute whitelist path, исходный hash/отсутствие для нового файла, patch hash, expected result, защищённую согласованную копию, явный контур/допуск. При drift stop без fuzzy overwrite. Все новые данные/секреты на Mini вне Git; MCP config recipe не содержит credential. Стандартно dry-run; real apply требует отдельного оперативного разрешения, не только флага плана. Rollback останавливает новый dispatch и возвращает только собственный code/config, сохраняет pending/receipt/backlink/личные задачи. Сконструируй synthetic filesystem/supervisor fixture и ограниченный изолированный lifecycle настоящий Vikunja; повтор применения не создаёт второй управляющий контур. Проверь no-permission/drift/частичный сбой/rollback/post-login описание. Не стартуй/не останавливай рабочую службу или Mini. Exact неизвестные plist/config paths не выдумывай. Выполни указанные checks этого stage, сохрани argv/rc/ненулевое покрытие, source SHA/diff digest, реальные synthetic IDs и cleanup каждого прогона. В отчёте назови изменённые пути, решения, ограничения и зависимости; не выдавай mock/skip за стенд. Отдай diff coordinator для отдельных независимых QA/review; публикация только helper после них.",
      "paths": [
        "scripts/ea010_service.py",
        "scripts/ea010_apply.py",
        "config/service.example.json",
        "integration/environment/manifest.schema.json",
        "tests/test_lifecycle.py",
        "tests/support/lifecycle_suite.py"
      ],
      "checks": [
        [
          "python3",
          "-m",
          "unittest",
          "discover",
          "-s",
          "tests",
          "-p",
          "test_lifecycle.py"
        ],
        [
          "python3",
          "scripts/ea010_verify.py",
          "--suite",
          "lifecycle",
          "--mode",
          "isolated"
        ],
        [
          "git",
          "diff",
          "--check"
        ]
      ],
      "acceptance": "На изолированной копии dry-run/repeat/apply/rollback проверены; drift/несовместимость блокируют запись; только свои файлы возвращены, журнал и legacy links сохранены; рабочий сервис не затронут.",
      "commit_message": "feat: add guarded reuse and environment change tooling",
      "context_budget": 170000
    },
    {
      "id": "s09-restore",
      "title": "Ручная копия и изолированное восстановление",
      "prompt": "process: /Users/efimovivan/Documents/DevProjects/worktrees/vikunja-local-ea010-install-integration/vikunja-local/install-vikunja-tracker/docs/process/t_606468bd.md#development\nEA-010, native t_606468bd. Исполни только этот stage как Cursor implementation под Codex coordinator в /Users/efimovivan/Documents/DevProjects/worktrees/vikunja-local-ea010-install-integration/vikunja-local/install-vikunja-tracker, ветка hermes/install-vikunja-tracker. Сначала прочитай применимые AGENTS/ai-developer, docs/artifacts/t_606468bd-implementation-plan.md целиком (паспорт входов, C1–C8, внешняя область, профиль и publication), docs/requirements/t_606468bd-system-requirements.md, ADR/НФТ и указанные в плане БТ/UX/оценённые пакеты; проверь принятые актуальные SHA и допуск stage/DoR у coordinator. Сохрани AI_PLATFORM_PROCESS_DIR=/Users/efimovivan/Projects/cto-agent-environment/ops/real/process-profile-staged. При незакрытом условии не начинай зависимый код, передай конкретный вопрос coordinator; не принимай и не редактируй входные документы/process-state. Только свой task diff, синтетические тесты, без секретов/реальных задач/встреч, внешних edits, commit/push/PR/merge или самоприёмки. Новых агентов/работ по собственной инициативе не запускай. Вход: s08, C6 принятые границы backup/restore и состав согласованной копии. Реализуй ручную согласованную копию задач/вложений/безопасной конфигурации/журнала/связей и restore в отдельный пустой synthetic target. Не добавляй scheduler/регулярный backup и не обещай RPO/RTO/retention. Секреты остаются в защищённой локальной копии, не в Git/logs. Writes после restore выключены до сверки фактической Vikunja/receipt/mapping, старая копия/unknown не дают новый create. Проверь настоящий изолированный restore с synthetic вложением/hash, прежним ID/backlink и replay; потеря/повреждение части копии и drift запрещают writes. Рабочий source/target разрешаются только отдельной эксплуатационной работой C4/C5, этот stage работает на synthetic данных. Не перезаписывай рабочую БД ради демонстрации. В отчёте различай копию на Mini и защиту от физической потери устройства. Выполни указанные checks этого stage, сохрани argv/rc/ненулевое покрытие, source SHA/diff digest, реальные synthetic IDs и cleanup каждого прогона. В отчёте назови изменённые пути, решения, ограничения и зависимости; не выдавай mock/skip за стенд. Отдай diff coordinator для отдельных независимых QA/review; публикация только helper после них.",
      "paths": [
        "scripts/ea010_backup.py",
        "vikunja_local/restore.py",
        "tests/test_restore.py",
        "tests/support/restore_suite.py"
      ],
      "checks": [
        [
          "python3",
          "-m",
          "unittest",
          "discover",
          "-s",
          "tests",
          "-p",
          "test_restore.py"
        ],
        [
          "python3",
          "scripts/ea010_verify.py",
          "--suite",
          "restore",
          "--mode",
          "isolated"
        ],
        [
          "git",
          "diff",
          "--check"
        ]
      ],
      "acceptance": "Реальный isolated restore возвращает прежние synthetic ID, attachment checksum и mapping; после старой/несогласованной копии writes закрыты до reconcile, дубликатов нет. Нет регулярного backup и рабочих restore.",
      "commit_message": "feat: add consistent manual backup and isolated recovery checks",
      "context_budget": 150000
    },
    {
      "id": "s10-e2e",
      "title": "Полный профиль сквозных и эксплуатационных проверок",
      "prompt": "process: /Users/efimovivan/Documents/DevProjects/worktrees/vikunja-local-ea010-install-integration/vikunja-local/install-vikunja-tracker/docs/process/t_606468bd.md#development\nEA-010, native t_606468bd. Исполни только этот stage как Cursor implementation под Codex coordinator в /Users/efimovivan/Documents/DevProjects/worktrees/vikunja-local-ea010-install-integration/vikunja-local/install-vikunja-tracker, ветка hermes/install-vikunja-tracker. Сначала прочитай применимые AGENTS/ai-developer, docs/artifacts/t_606468bd-implementation-plan.md целиком (паспорт входов, C1–C8, внешняя область, профиль и publication), docs/requirements/t_606468bd-system-requirements.md, ADR/НФТ и указанные в плане БТ/UX/оценённые пакеты; проверь принятые актуальные SHA и допуск stage/DoR у coordinator. Сохрани AI_PLATFORM_PROCESS_DIR=/Users/efimovivan/Projects/cto-agent-environment/ops/real/process-profile-staged. При незакрытом условии не начинай зависимый код, передай конкретный вопрос coordinator; не принимай и не редактируй входные документы/process-state. Только свой task diff, синтетические тесты, без секретов/реальных задач/встреч, внешних edits, commit/push/PR/merge или самоприёмки. Новых агентов/работ по собственной инициативе не запускай. Вход: s09 и актуальный approved профиль C7/C1–C6. Собери suite all из реально созданных suites и E2E §6: UI↔MCP, все CRUD, owner acceptance/outbox/обе ссылки, auth/Origin/scopes/revoke, retry/reconnect/crash/late-write, даты/304/recurring, legacy/390px, restart/restore и no development-run. Не зачти старые P9/P10 за tests нового SHA. Создай fail-closed --mode approved-pilot: только по локальному exact operational permit/resources manifest; без него не пишет ни в среду, ни в сервис. Действия владельца и доступ MacBook/рестарт Mini — ручные checkpoints с настоящими receipts; без них результат not-run/fail, не simulated success. Coding-worker выполняет isolated suite; pilot run отдельно разрешает coordinator. Profile описывает exact argv/dependencies/выборку/лимиты/cleanup, а регистрацию внешнего execution profile выполняет его владелец, не этот worker. Проверь ошибочные и signal прогоны/ресурсный read-back; не маскируй baseline failure. Самостоятельное функциональное QA/самоприёмка stage запрещены: это harness и self-check, независимые QA/review затем отдельные. Выполни указанные checks этого stage, сохрани argv/rc/ненулевое покрытие, source SHA/diff digest, реальные synthetic IDs и cleanup каждого прогона. В отчёте назови изменённые пути, решения, ограничения и зависимости; не выдавай mock/skip за стенд. Отдай diff coordinator для отдельных независимых QA/review; публикация только helper после них.",
      "paths": [
        "tests/test_e2e.py",
        "tests/support/e2e_suite.py",
        "tests/support/pilot_suite.py",
        "docs/testing/ea010-verification-profile.md"
      ],
      "checks": [
        [
          "python3",
          "-m",
          "unittest",
          "discover",
          "-s",
          "tests",
          "-p",
          "test_e2e.py"
        ],
        [
          "python3",
          "scripts/ea010_verify.py",
          "--suite",
          "all",
          "--mode",
          "isolated"
        ],
        [
          "git",
          "diff",
          "--check"
        ]
      ],
      "acceptance": "Все обязательные isolated suites выполнены на реальных сервисах нового diff с ненулевым покрытием и cleanup; approved-pilot без разрешения fail-closed. Документированы неисполненные owner/device/restart checkpoints; не заявлена полная рабочая готовность.",
      "commit_message": "test: add full synthetic integration and guarded pilot profile",
      "context_budget": 180000
    },
    {
      "id": "s11-handoff",
      "title": "Проверенные инструкции и накопительный отчёт",
      "prompt": "process: /Users/efimovivan/Documents/DevProjects/worktrees/vikunja-local-ea010-install-integration/vikunja-local/install-vikunja-tracker/docs/process/t_606468bd.md#development\nEA-010, native t_606468bd. Исполни только этот stage как Cursor implementation под Codex coordinator в /Users/efimovivan/Documents/DevProjects/worktrees/vikunja-local-ea010-install-integration/vikunja-local/install-vikunja-tracker, ветка hermes/install-vikunja-tracker. Сначала прочитай применимые AGENTS/ai-developer, docs/artifacts/t_606468bd-implementation-plan.md целиком (паспорт входов, C1–C8, внешняя область, профиль и publication), docs/requirements/t_606468bd-system-requirements.md, ADR/НФТ и указанные в плане БТ/UX/оценённые пакеты; проверь принятые актуальные SHA и допуск stage/DoR у coordinator. Сохрани AI_PLATFORM_PROCESS_DIR=/Users/efimovivan/Projects/cto-agent-environment/ops/real/process-profile-staged. При незакрытом условии не начинай зависимый код, передай конкретный вопрос coordinator; не принимай и не редактируй входные документы/process-state. Только свой task diff, синтетические тесты, без секретов/реальных задач/встреч, внешних edits, commit/push/PR/merge или самоприёмки. Новых агентов/работ по собственной инициативе не запускай. Вход: s10, отдельное допущенное рабочее применение §4 и независимые итоговые QA/review с настоящими Mini/MacBook/owner/restart/restore/cleanup receipts на проверенном source SHA. Если чего-то нет, запроси конкретный недостающий вход через coordinator и не объявляй stage завершённым. Измени только документацию: три канала, управление единственной службой после login, диагностика/reconcile неизвестности/восстановление только backlink, ручные backup/restore и ограничения пилота. Передай проверенные адреса обоих устройств владельцу только локально в закрытом приложении; в Git обезличенные шаблоны и ссылки на безопасные receipts, не topology/секреты/реальные данные. Накопительный отчёт включает stage source/commit/remote SHA, argv/rc/counts, независимые review/QA, hash evidence и cleanup каждого прогона, remaining conditions/failures; различай SHA кода до docs-only commit и финальный commit. Не фабрикуй ID/успех и не закрывай process/gates/UAT/DoD/PR. Выполни regression all isolated и сверку инструкций по реальным receipts, не повторяй рабочий рестарт без нового окна. Финальное независимое review проверяет весь пакет; release-docs остаётся отдельным native шагом. Выполни указанные checks этого stage, сохрани argv/rc/ненулевое покрытие, source SHA/diff digest, реальные synthetic IDs и cleanup каждого прогона. В отчёте назови изменённые пути, решения, ограничения и зависимости; не выдавай mock/skip за стенд. Отдай diff coordinator для отдельных независимых QA/review; публикация только helper после них.",
      "paths": [
        "README.md",
        "docs/operations/ea010-runbook.md",
        "docs/reports/t_606468bd-implementation-report.md"
      ],
      "checks": [
        [
          "python3",
          "-m",
          "unittest",
          "discover",
          "-s",
          "tests",
          "-p",
          "test_e2e.py"
        ],
        [
          "python3",
          "scripts/ea010_verify.py",
          "--suite",
          "all",
          "--mode",
          "isolated"
        ],
        [
          "git",
          "diff",
          "--check"
        ]
      ],
      "acceptance": "Инструкции опираются на реальные независимые проверки/cleanup и доступны владельцу; все 3 канала, post-login restart/restore и обе ссылки подтверждены на указанном кодовом SHA. Есть накопительная SHA/проверка/ограничения, нет самоприёмки и публикации PR.",
      "commit_message": "docs: deliver verified personal tracker runbook and evidence index",
      "context_budget": 120000
    }
  ]
}
```


## 9. Трассировка и самопроверка документа

| Основание | Stage / независимое доказательство |
| --- | --- |
| BR-01/12/13; FR-01/17–19; NFR-01/13/14 | s01/s08/s09/s10, отдельный approved-pilot restart и reuse/сохранность |
| BR-02; FR-02; UX/mockups | s07/s10, готовый UI и вход из Hermes, keyboard/390 px |
| BR-03/06/07; FR-03/04/08/09/21/22 | s02/s04/s05/s10, CRUD/ID/read-back/status/no dev-run |
| BR-04/05/14; FR-05–07/20/23; NFR-08–10/17 | s06/s07/s10, настоящее принятие владельцем/ссылки/legacy и негативы |
| BR-08/09; FR-10–13; NFR-06–08/16 | s03/s04/s06/s09/s10, crash/late-write/repeat/reconcile одного ID |
| BR-10; FR-14/15; NFR-11 | s02/s05/s07/s10, MSK/omission/null/подтверждение/recurring |
| BR-11/16; FR-16; NFR-02–04/12 | Все stages; scopes/Origin/права/локальные секреты/синтетика |
| BR-15/17/18; FR-24; NFR-15/18 | s08–s11, отдельное применение, isolated restore, независимые результаты/cleanup/инструкции и publication contract |

AC-EA010-01–19 используются как матрица будущих проверок §6. У AC-18 новая граница однопользовательского исключения требует принятого уточнения C1: негативы доступа обязательны, защита от одновременной ручной правки не обещается. Трассировка не объявляет AC пройденными.

Документная проверка перед handoff: один JSON-блок, схема/обязательные ключи/уникальные stage IDs, относительные paths без выхода наружу, argv без shell/секретов, context_budget в пределах ориентира, exact publication совпадает с веткой/remote, для каждого stage есть результат/входы/scope/исключения/промпт/checks/acceptance/commit. Повторно проверяются SHA read-only входов и `git diff --check`; найденные замечания исправляются только в этом плане. Проверка JSON/трассировки автором не является независимым review или проверкой продукта.

**Фактически выполненная документная проверка:** JSON разобран стандартным парсером; схема и обязательные поля проверены, 11 уникальных stages, относительные paths, argv и бюджеты до 200000 токенов валидны; branch/remote совпали с Git. SHA256 всех 8 входов неизменны; локальные Markdown-ссылки существуют. `git diff --check` и `git diff --no-index --check /dev/null docs/artifacts/t_606468bd-implementation-plan.md` завершились без замечаний. Продуктовые suites из плана не запускались: код/стенд этим этапом не создавались.

**Сдача:** только implementation-plan на обычный `kanban_request_review` исходной карточки без metadata вопроса и без reviewer override. Ответы владельца сохранены как основания, не approvals. Приёмка плана, пересмотр требований, закрытие условий, DoR и рабочие действия остаются у ведущего flow и владельца. Ни текущий worker, ни этот документ не принимают собственный результат и не завершают EA-010.
