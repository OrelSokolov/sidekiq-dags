# Changelog

## [Unreleased]

### Added
- **Batch Coordination Middleware** - решает проблему race condition при использовании rate limiting в нодах
  - `BatchCoordinationClientMiddleware` - отслеживает регистрацию реальных джоб в batch
  - `BatchCoordinationServerMiddleware` - координирует выполнение DummyJob, ожидая стабильности batch
  - Автоматическая регистрация middleware при загрузке гема
  - Поддержка любых rate limiters (sidekiq-uniform, sidekiq-throttled, etc.)
  
### Fixed
- Race condition когда DummyJob завершается до регистрации джоб с rate limiting
- Преждевременное срабатывание batch callbacks в нодах с медленной регистрацией джоб

### Technical Details
- DummyJob теперь ожидает "стабильности" batch перед завершением
- Стабильность = 200ms без новых регистраций джоб
- Максимальное ожидание = 5 секунд (таймаут безопасности)
- Минимальный overhead: ~0.1ms на джобу для client middleware
- См. `docs/batch_coordination.md` для деталей

## [Previous versions]
...

