# frozen_string_literal: true

module Sidekiq
  # Concern для отслеживания статусов пайплайна в базе данных
  # Включается в Node каждого пайплайна
  module PipelineTracking
    extend ::ActiveSupport::Concern

    included do
      # Используем around_perform для гарантированного обновления статуса
      # даже при ошибках
    end

    # Получить имя пайплайна из модуля
    def pipeline_name
      class_name = self.class.name
      if class_name.include?('::')
        namespace = class_name.split('::')[0..-2].join('::')
        underscore(namespace)
      else
        underscore(class_name)
      end
    end

    def underscore(string)
      string.to_s.gsub(/::/, '/')
            .gsub(/([A-Z]+)([A-Z][a-z])/, '\1_\2')
            .gsub(/([a-z\d])([A-Z])/, '\1_\2')
            .tr('-', '_')
            .downcase
    end

    # Получить имя ноды из класса
    def node_name
      self.class.name.split('::').last
    end

    # Проверить запущен ли пайплайн
    def pipeline_running?
      SidekiqPipeline.for(pipeline_name).running?
    end

    # Получить объект текущего пайплайна
    def current_pipeline
      return nil unless defined?(SidekiqPipeline) && SidekiqPipeline.table_exists?

      @current_pipeline ||= SidekiqPipeline.for(pipeline_name)
    end

    # Получить объект текущей ноды
    def current_node_record
      return nil unless defined?(SidekiqPipelineNode) && SidekiqPipelineNode.table_exists?

      @current_node_record ||= SidekiqPipelineNode.for(pipeline_name, node_name)
    end

    # Гарантировать существование записи SidekiqPipelineNode
    # Используется для автоматического создания записи при первом обращении
    def ensure_node_record_exists!
      current_node_record
    end

    # Вызывается в начале выполнения ноды
    def mark_node_started!
      return false unless current_pipeline && current_node_record

      # Гарантируем создание SidekiqPipelineNode перед любыми операциями
      # Это важно, так как запись может использоваться в mark_node_completed! или mark_node_failed!
      ensure_node_record_exists!

      if node_name == 'RootNode'
        # Для RootNode - запускаем весь пайплайн
        if current_pipeline.running?
          Sidekiq.logger.warn "🛑 Pipeline #{pipeline_name} already running, skipping start"
          return false
        end
        current_pipeline.start!
      end

      current_node_record.start!
      true
    rescue ::ActiveRecord::StatementInvalid, ::ActiveRecord::NoDatabaseError => e
      Sidekiq.logger.debug "Pipeline tracking disabled: #{e.message}"
      false
    end

    # Вызывается при успешном завершении ноды
    def mark_node_completed!
      puts "CURRENT NODE RECORD: #{current_node_record.inspect}"
      return unless current_node_record

      ensure_node_record_exists!
      current_node_record.complete!

      # Если это EndNode - завершаем пайплайн
      current_pipeline.finish!(success: true) if node_name == 'EndNode' && current_pipeline
    rescue ::ActiveRecord::StatementInvalid, ::ActiveRecord::NoDatabaseError => e
      Sidekiq.logger.debug "Pipeline tracking disabled: #{e.message}"
    end

    # Вызывается при ошибке в ноде
    def mark_node_failed!(error)
      return unless current_node_record && current_pipeline

      ensure_node_record_exists!
      current_node_record.fail!(error.message)
      current_pipeline.finish!(success: false, error: error.message)
    rescue ::ActiveRecord::StatementInvalid, ::ActiveRecord::NoDatabaseError => e
      Sidekiq.logger.debug "Pipeline tracking disabled: #{e.message}"
    end

    # Получить активные ноды для UI (совместимость со старым кодом)
    # Возвращает массив строк в формате "Skillcorner_RootNode"
    def self.get_active_nodes_for_ui(pipeline_name = nil)
      scope = SidekiqPipelineNode.joins(:sidekiq_pipeline).where(status: %i[running completed])

      scope = scope.where(sidekiq_pipelines: { pipeline_name: pipeline_name.to_s.downcase }) if pipeline_name

      scope.map(&:node_id)
    end
  end
end
