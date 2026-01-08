# frozen_string_literal: true

module Sidekiq
  module Pipeline
    # Railtie для автоматической загрузки моделей в Rails приложении
    class Railtie < Rails::Railtie
      initializer 'sidekiq.pipeline.autoload', before: :set_autoload_paths do |app|
        # Автоматически загружаем модели при старте Rails
        app.config.autoload_paths += %W[#{File.dirname(__FILE__)}/../../]
      end
    end
  end
end

