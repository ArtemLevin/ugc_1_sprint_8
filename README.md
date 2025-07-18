https://github.com/ArtemLevin/ugc_sprint_1.git


# _Функциональные требования_ #
### Основной функционал
- [ ] Добавление пользовательского рейтинга фильмов
- [ ] Возможность ставить лайки/дизлайки/эмодзи-реакции на фильмы
- [ ] Возможность оставлять отзывы к фильмам
- [ ] Отображение ежегодной/ежемесячной/ежедневной истории просмотров пользователя
- [ ] Возможность добавления/удаления фильмов в закладки
- [ ] Отображение списка недосмотренных фильмов
- [ ] Учет действий пользователя в плеере (например: смена языка, субтитров, прогресс просмотра, разрешение (hd, fhd, etc))
- [ ] Передача данных аналитикам для обработки (куда/как?)
- [ ] Поддержка пользовательских метаданных (например: теги, пометки, списки)
- [ ] Уведомления о продолжении просмотра
- [ ] Пользовательский профиль с историей активности


# _Нефункциональные требования_ #

### Производительность 
- [ ] Система должна поддерживать до **10 000 RPS** в пиковые часы с учетом учётом фоновых операций, аналитики, кэширования, синхронизации
- [ ] Поддержка **DAU = 100 000** (ежедневно активных пользователей)
- [ ] Поддержка **MAU = 1 000 000** (ежемесячно активных пользователей)
- [ ] Размер среднего сообщения при взаимодействии с сервисами — около **2 KB**

_для оценки использовался **DAU** Netflix на основе данных Statista. Принял **вовлеченность** равной 0,1 c корректировкой на стартап_

_**среднее число запросов пользователя в день** принял равным 20_

_**пиковый коэффициент** принял равным 3_

### Надежность (Reliability)
- [ ] Время безотказной работы должно быть не менее **99.95% в месяц**
- [ ] Обработка ошибок с возвратом понятного ответа пользователю
- [ ] Логирование всех событий с возможностью восстановления состояния

### Доступность (Availability)
- [ ] Система должна быть доступна **99.9% времени** в год (включая плановые обновления)
- [ ] Минимальное время простоя при обновлениях
- [ ] Резервное копирование данных минимум раз в сутки

### Масштабируемость (Scalability)
- [ ] Архитектура должна позволять горизонтальное масштабирование сервисов
- [ ] Поддержка роста DAU до **1 000 000+** без существенной переработки архитектуры
- [ ] Базы данных должны поддерживать шардинг и репликацию

### Ремонтопригодность (Maintainability)
- [ ] Простота обновления отдельных компонентов без полного дублирования системы
- [ ] Четкое разделение микросервисов по зонам ответственности
- [ ] Инструменты мониторинга, логирования и трассировки

### Устойчивость к сбоям (Fault Tolerance)
- [ ] Система должна корректно обрабатывать частичные сбои (например: потеря связи с одним сервисом)
- [ ] Использование retry-логики, fallback-ответов, circuit breakers
- [ ] Обеспечение отказоустойчивости через репликацию сервисов и баз данных

### Безопасность
- [ ] Аутентификация и авторизация через JWT / OAuth2
- [ ] Шифрование данных
- [ ] Защита от DDoS и злоупотребления API (rate limiting, IP blocking)
- [ ] Соответствие локальным стандартам (GDPR, etc)

### Выбор между Consistency, Availability и Partition Tolerance
- [ ] Для **истории просмотров, закладок и лайков** — **Availability + Partition tolerance**:
    - Приемлемо небольшое запаздывание обновлений

- [ ] Для **системы рейтингов и отзывов** — **Consistency + Partition tolerance**:
    - Критически важно, чтобы пользователи видели актуальные данные

- [ ] Для **аналитики поведения** — **Availability + Partition tolerance**
    - Аналитика пользовательского поведения — это не критичная по времени операция
