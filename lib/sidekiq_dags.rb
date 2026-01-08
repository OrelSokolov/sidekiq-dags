# frozen_string_literal: true

require 'sidekiq'
require 'active_support/core_ext'

require 'sidekiq/batch'
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

