# frozen_string_literal: true

require 'sidekiq'
require 'active_support/core_ext'

require 'sidekiq/batch'
require 'sidekiq/dummy_job'
require 'sidekiq/batch_coordination_middleware'
require 'sidekiq/pipeline_dsl_extension'
require 'sidekiq/pipeline_visualizer'

# Загружаем pipeline компоненты только если ActiveRecord доступен
begin
  require 'active_record'
  require 'sidekiq/pipeline'
  require 'sidekiq/pipeline_node'
  require 'sidekiq/pipeline_callback'
  require 'sidekiq/pipeline_tracking'
rescue LoadError
  # ActiveRecord не доступен, пропускаем загрузку pipeline компонентов
  Sidekiq.logger.debug "ActiveRecord not available, pipeline tracking disabled" if defined?(Sidekiq.logger)
end

module SidekiqDags
  # Модуль для работы с пайплайнами Sidekiq
  # Используется для создания и управления пайплайнами из батчей
end

# Автоматически регистрируем middleware для координации батчей
# Это решает проблему race condition когда джобы создаются с rate limiting
Sidekiq.configure_server do |config|
  config.server_middleware do |chain|
    chain.add Sidekiq::BatchCoordinationServerMiddleware
  end
  # Client middleware нужен и на сервере, т.к. сервер тоже создаёт джобы
  config.client_middleware do |chain|
    chain.add Sidekiq::BatchCoordinationClientMiddleware
  end
end

Sidekiq.configure_client do |config|
  config.client_middleware do |chain|
    chain.add Sidekiq::BatchCoordinationClientMiddleware
  end
end

