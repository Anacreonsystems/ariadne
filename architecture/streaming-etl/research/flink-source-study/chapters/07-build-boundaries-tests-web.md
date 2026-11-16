# Сборка, границы модулей, архитектурные проверки и web-поверхность

Статус: **progress**. Это исследование исходников, не принятое решение о новом ядре Ariadne. Ревизия Flink: `068f428b29f4d9ee587fa1006d2f1a69b43c0000`. Обязательство области `root` — 7 069 файлов и 678 568 строк текстовых файлов. Эта глава покрывает только явно перечисленные ниже прочитанные файлы. Остальные остаются открытыми в [реестре](../reading-status.jsonl). Сборки, тесты Flink и установка зависимостей не выполнялись.

## Потребность и критерии приёмки исследования

**As a developer, I want to** понимать, где Flink проводит границы API/runtime/planner/connectors, как эти границы проверяются и какая часть системы отвечает за сборку и web-интерфейс.

**Acceptance criteria: as a developer, I can** пройти от каждого описанного ограничения до конкретной проверки на закреплённой ревизии, отличить правило от его исключений и увидеть непрочитанную область. **As a system, I got** воспроизводимый реестр диапазонов без двойного счёта. Полная приёмка всей области требует чтения оставшихся файлов; наличие этой главы её не закрывает.

Связи: [карта исследования](../README.md), [API/сериализация/Python](04-api-types-serialization-python.md), [управление и scheduler](01-control-plane-scheduling.md), [исполнение и сеть](02-task-execution-network-backpressure.md), [Table/SQL](05-table-sql-planner-runtime.md), [коннекторы](06-connectors-formats-storage-metrics.md).

## 1. Reactor — состав продукта, а не граф исполнения задачи

Полностью прочитан корневой [`pom.xml`, 2 457 строк](https://github.com/apache/flink/blob/068f428b29f4d9ee587fa1006d2f1a69b43c0000/pom.xml). Это Maven parent/aggregator версии `2.5-SNAPSHOT`. Перечень [модулей, строки 71–110](https://github.com/apache/flink/blob/068f428b29f4d9ee587fa1006d2f1a69b43c0000/pom.xml#L71) включает core/core-api, runtime, streaming-java, новые datastream/datastream-api, table, Python, connectors, state-backends, RPC, web, clients, deployment, tests и distribution. Наличие нескольких API-модулей не доказывает наличие нескольких конкурирующих планировщиков: причинный переход от API к исполняемому графу изучается в главе 04.

Root зависимости наследуются модулями; dependencyManagement задаёт версии, но сам по себе не означает подключение каждой библиотеки ко всем артефактам. Внешние shaded-артефакты подключаются отдельно. [Предупреждение в строках 339–346](https://github.com/apache/flink/blob/068f428b29f4d9ee587fa1006d2f1a69b43c0000/pom.xml#L339) запрещает помещать ряд исходных библиотек в общее управление версиями из-за влияния на транзитивные зависимости Hadoop. Это ограничение упаковки/совместимости classpath, не семантика доставки записей.

## 2. Языковая совместимость зависит от профиля

В [properties, строки 134–145](https://github.com/apache/flink/blob/068f428b29f4d9ee587fa1006d2f1a69b43c0000/pom.xml#L134) исходный синтаксис Java — 11, target по умолчанию — 17. Нельзя свести это к утверждению «весь проект Java 11» или «весь проект Java 17». Профиль `java11-target` меняет target на 11; `java17-target` добавляет `flink-tests-java17` и явно оставляет source 11; `java21-target` задаёт source/target 21. Release меняет target на 11. Это прочитанные объявления Maven; фактические комбинации CI/JDK ещё требуют чтения workflow и дочерних POM.

В [настройке compiler, строки 2140–2158](https://github.com/apache/flink/blob/068f428b29f4d9ee587fa1006d2f1a69b43c0000/pom.xml#L2140) `release` очищен, потому что используются `--add-exports`; следовательно комментарий «только Java 11 APIs» не следует воспринимать как доказательство полноценной проверки через `--release 11`. [Enforcer, строки 1874–1895](https://github.com/apache/flink/blob/068f428b29f4d9ee587fa1006d2f1a69b43c0000/pom.xml#L1874) задаёт Maven 3.9.16, Java version property и ограничение версии Hadoop.

## 3. Границы реализации защищены сборкой

[Enforcer, строки 1952–1990](https://github.com/apache/flink/blob/068f428b29f4d9ee587fa1006d2f1a69b43c0000/pom.xml#L1952) запрещает прямую зависимость от `flink-rpc-akka`, направляя к loader и `RpcSystem#load`/testing service. Аналогично production-зависимости от Scala-suffixed table-planner направляются к Table API или planner-loader, с явным исключением для test scope. Это подтверждает намерение изолировать реализации за точками загрузки. Сам внутренний classloading ещё не прочитан этой областью и здесь не описывается как проверенный.

[Shade, строки 2008–2096](https://github.com/apache/flink/blob/068f428b29f4d9ee587fa1006d2f1a69b43c0000/pom.xml#L2008) удаляет bundled logging properties/signatures/ненужные Maven POM, объединяет `META-INF/services`, агрегирует NOTICE, создаёт dependency-reduced POM. Поэтому упаковка влияет на обнаружение сервисов и конфликты зависимостей. Нельзя механически переносить только Java-классы и ожидать идентичную загрузку.

`dependency-convergence` по умолчанию имеет phase `none`; включается отдельным профилем. Профиль `fast` пропускает ряд проверок. Поэтому название успешной Maven-команды без профилей/фаз недостаточно для утверждения, что все архитектурные и совместимые границы проверены.

## 4. API-контракт проверяется отдельно от допустимости зависимости

Полностью прочитаны [`ProductionCodeArchitectureBase`, 46 строк](https://github.com/apache/flink/blob/068f428b29f4d9ee587fa1006d2f1a69b43c0000/flink-architecture-tests/flink-architecture-tests-production/src/main/java/org/apache/flink/architecture/ProductionCodeArchitectureBase.java), [`ArchitectureTest`, 40 строк](https://github.com/apache/flink/blob/068f428b29f4d9ee587fa1006d2f1a69b43c0000/flink-architecture-tests/flink-architecture-tests-production/src/test/java/org/apache/flink/architecture/ArchitectureTest.java) и все четыре подключённые rule-класса.

`ArchitectureTest` импортирует `org.apache.flink`, исключает test, Scala и shaded code. Base подключает API annotations, Table API, connectors и checkpoint configuration. Это не универсальное доказательство архитектуры для всех языков и всех зависимостей. README production-модуля отдельно требует добавлять проверяемые модули test dependencies; фактический список ещё предстоит сопоставить с reactor.

[`ApiAnnotationRules`, 178 строк](https://github.com/apache/flink/blob/068f428b29f4d9ee587fa1006d2f1a69b43c0000/flink-architecture-tests/flink-architecture-tests-production/src/main/java/org/apache/flink/architecture/rules/ApiAnnotationRules.java#L48) требует явной visibility-аннотации публичных API-классов вне internal packages. Public methods ограничивают leaf-типы аргументов/возврата public API, а PublicEvolving допускает PublicEvolving. Есть исключения для deprecated/experimental/internal, внешних и shaded типов. Production-вызовы `VisibleForTesting` ограничены с исключениями для самого тестового метода, своего класса и пары enclosing/enclosed class.

Ряд правил обёрнут `freeze(...)`. Это допускает сохранённый baseline существующих нарушений, а не означает абсолютную чистоту дерева. Конкретные violation stores входят в обязательство чтения и пока не разобраны.

[`ConnectorRules`, 81 строка](https://github.com/apache/flink/blob/068f428b29f4d9ee587fa1006d2f1a69b43c0000/flink-architecture-tests/flink-architecture-tests-production/src/main/java/org/apache/flink/architecture/rules/ConnectorRules.java#L61) ограничивает non-deprecated connector-классы public/public-evolving типами Flink за пределами connector packages. Но разрешает другие connectors, `org.apache.flink.util..` и внешние типы; вложенные public-классы тоже учитываются. Краткая формула «connector зависит только от public API» требует этих оговорок и frozen baseline.

## 5. Конфигурация checkpoint имеет узкую статическую защиту

[`CheckpointingConfigurationAccessRules`, 195 строк](https://github.com/apache/flink/blob/068f428b29f4d9ee587fa1006d2f1a69b43c0000/flink-architecture-tests/flink-architecture-tests-production/src/main/java/org/apache/flink/architecture/rules/CheckpointingConfigurationAccessRules.java#L64) ограничивает прямые `Configuration.get/getOptional` для трёх checkpoint options, направляя вызовы к helper-методам с валидацией. Reflection на поля/сигнатуры делает переименование fail-fast при загрузке rule-класса.

Это **эвристика уровня метода**: она ищет доступ к запрещённому полю в том же origin method, не доказывает dataflow конкретного аргумента `get`. Вызовы из `CheckpointingOptions` целиком разрешены; whitelist методов дополнительно разрешает `CheckpointConfig.configure`. Нельзя приписывать этому правилу полную проверку всех путей конфигурации checkpoint или корректность самого snapshot-протокола.

[`TableApiRules`, 131 строка](https://github.com/apache/flink/blob/068f428b29f4d9ee587fa1006d2f1a69b43c0000/flink-architecture-tests/flink-architecture-tests-production/src/main/java/org/apache/flink/architecture/rules/TableApiRules.java#L57) отделяет public static `ConfigOption` в `*Options`/FactoryUtil от DynamicTableFactory, проверяет package/visibility connector/format options и visibility публичных Table API модулей по source location. Это статическая дисциплина API, отдельная от Table planner/runtime в главе 05.

## 6. Unit и integration выполняются разными execution одной системы

[Surefire, строки 1766–1853](https://github.com/apache/flink/blob/068f428b29f4d9ee587fa1006d2f1a69b43c0000/pom.xml#L1766) запускает unit-pattern `*Test`/`*Tests` в phase `test`, остальные классы в `integration-test`, исключая unit-pattern и Scala-generated `$` classes. Integration execution отключает reuseForks и использует отдельные fork/memory limits. Это Surefire, не предположение о Maven Failsafe по названию integration.

По умолчанию передаются checkpoint/buffer-debloat randomization, случайное включение changelog backend и опциональный seed. JUnit parallel feature включён, но classes/methods по умолчанию `same_thread`; concurrency opt-in. Поэтому включённый parallel engine не доказывает параллельное исполнение каждого теста. Чтение этих настроек не заменяет запуск и не даёт pass/fail результата.

## 7. Web-модуль собирает отдельный клиент и Java-обработчики

Полностью прочитан [`flink-runtime-web/pom.xml`, 360 строк](https://github.com/apache/flink/blob/068f428b29f4d9ee587fa1006d2f1a69b43c0000/flink-runtime-web/pom.xml). Java-артефакт зависит от runtime/clients и shaded сетевых/JSON библиотек; из dashboard включается ресурс `web/**`. Frontend Maven plugin задаёт Node 22.16.0/npm 10.9.0, `npm ci`, lint, `test:ci`, build. Профиль `fast` отключает Node tests; `skip-webui-build` пропускает frontend plugin с отдельным skip для lint/test executions. Наличие web JAR и backend-сборки само по себе не доказывает свежесть или проверку UI.

Java jar handlers, frontend HTTP services, обновление метрик, graph rendering и связанные тесты ещё не прочитаны. Здесь не утверждается, что web управляет per-record исполнением: для такого утверждения пришлось бы проследить вызовы до runtime, а не вывести его из зависимости в POM.

## Оставшееся чтение

Следующие цепочки: architecture import predicates/conditions и violation stores; production-module POM; test architecture rules; CI tools/workflows; runtime-web bootstrap/services/handlers/frontend; integration и end-to-end tests; остальные docs/build/resources по inventory. Все они остаются в исходном объёме задания. Полный текст прочитанных файлов и краткие индивидуальные выводы зарегистрированы в [root ledger](../readers/root.confirmed.jsonl), уникальное покрытие вычисляется [инструментом](../tools/coverage.py).
