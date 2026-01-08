# frozen_string_literal: true

module Sidekiq
  # Модель для хранения состояния отдельных узлов (нод) пайплайна
  # Каждая нода имеет ровно одну запись для каждого пайплайна
  class SidekiqPipelineNode < ::ActiveRecord::Base
    self.table_name = 'sidekiq_pipeline_nodes'

    belongs_to :sidekiq_pipeline, class_name: 'Sidekiq::SidekiqPipeline', foreign_key: 'sidekiq_pipeline_id'

    # Статусы ноды
    enum :status, { pending: 0, running: 1, completed: 2, failed: 3, skipped: 4 }

    validates :node_name, presence: true
    validates :node_name, uniqueness: { scope: :sidekiq_pipeline_id }

    # Получить singleton для ноды
    def self.for(pipeline_name, node_name)
      return nil unless table_exists?
      pipeline = SidekiqPipeline.for(pipeline_name)
      return nil unless pipeline
      pipeline.sidekiq_pipeline_nodes.find_or_create_by!(node_name: node_name.to_s)
    rescue ::ActiveRecord::StatementInvalid, ::ActiveRecord::NoDatabaseError
      nil
    end

    # Отметить начало выполнения ноды
    def start!
      update!(status: :running, run_at: Time.current, error_message: nil)
      Sidekiq.logger.info "▶️ Node #{full_name} started"
    end

    # Отметить успешное завершение ноды
    def complete!
      update!(status: :completed)
      Sidekiq.logger.info "✅ Node #{full_name} completed"
    end

    # Отметить ошибку в ноде
    def fail!(error = nil)
      error_msg = error.to_s
      error_msg = error_msg[0..999] if error_msg.length > 1000
      update!(status: :failed, error_message: error_msg)
      Sidekiq.logger.info "❌ Node #{full_name} failed: #{error}"
    end

    # Пропустить ноду
    def skip!
      update!(status: :skipped)
      Sidekiq.logger.info "⏭️ Node #{full_name} skipped"
    end

    # Полное имя ноды (для логирования)
    def full_name
      "#{sidekiq_pipeline.pipeline_name}::#{node_name}"
    end

    # ID для совместимости со старым кодом (использовался в UI)
    # Формат должен совпадать с PipelineVisualizer: "pipeline_id_NodeName"
    def node_id
      "#{sidekiq_pipeline.pipeline_name}_#{node_name}"
    end
  end
end

