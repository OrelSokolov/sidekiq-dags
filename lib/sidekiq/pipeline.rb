# frozen_string_literal: true

require 'set'

module Sidekiq
  # Модель для хранения состояния пайплайнов
  # Каждый пайплайн имеет ровно одну запись (singleton)
  #
  # ВАЖНО: Статус пайплайна определяется динамически на основе статусов нод,
  # а не из поля status в БД. Это предотвращает зависание пайплайна в статусе running.
  class SidekiqPipeline < ::ActiveRecord::Base
    self.table_name = 'sidekiq_pipelines'

    has_many :sidekiq_pipeline_nodes, class_name: 'Sidekiq::SidekiqPipelineNode', dependent: :destroy, foreign_key: 'sidekiq_pipeline_id'

    validates :pipeline_name, presence: true, uniqueness: true

    # Переменная класса для тестов - хранит очереди, которые должны считаться занятыми
    # Используется в тестах для симуляции наличия задач в очереди
    @test_queues_with_jobs = Set.new

    class << self
      attr_accessor :test_queues_with_jobs
    end

    # Получить singleton для пайплайна
    def self.for(name)
      return nil unless table_exists?
      find_or_create_by!(pipeline_name: name.to_s.downcase)
    rescue ::ActiveRecord::StatementInvalid, ::ActiveRecord::NoDatabaseError
      nil
    end

    # Запустить пайплайн
    def start!
      transaction do
        # Обновляем только run_at, статус определяется динамически на основе нод
        update!(run_at: Time.current)
        # Сбрасываем статусы всех нод
        sidekiq_pipeline_nodes.update_all(status: 0, run_at: nil, error_message: nil)
      end
      Sidekiq.logger.info "🚀 Pipeline #{pipeline_name} started"
    end

    # Завершить пайплайн (больше не обновляем статус в БД)
    # Статус определяется автоматически на основе нод
    def finish!(success: true, error: nil)
      # Статус теперь определяется динамически, ничего не делаем
      new_status = success ? 'completed' : 'failed'
      Sidekiq.logger.info "#{success ? '✅' : '❌'} Pipeline #{pipeline_name} finished with status: #{new_status}"
    end

    # Переопределяем методы статусов - определяем их динамически на основе нод
    # Это предотвращает зависание пайплайна в статусе running
    
    # Проверить запущен ли пайплайн
    # Пайплайн считается running если:
    # 1. Есть хотя бы одна running нода ИЛИ
    # 2. Есть задачи в очереди для данного провайдера
    def running?
      return true if sidekiq_pipeline_nodes.where(status: :running).exists?
      return true if has_jobs_in_queue?
      false
    end

    # Проверить завершен ли пайплайн (все ноды completed или skipped, и нет running/failed)
    def completed?
      return false if sidekiq_pipeline_nodes.empty?
      return false if sidekiq_pipeline_nodes.where(status: [:running, :pending, :failed]).exists?
      sidekiq_pipeline_nodes.where(status: [:completed, :skipped]).exists?
    end

    # Проверить есть ли ошибки (хотя бы одна failed нода)
    def failed?
      sidekiq_pipeline_nodes.where(status: :failed).exists?
    end

    # Проверить в режиме ожидания (нет running/completed/failed нод, только pending или пусто)
    def idle?
      return false if has_jobs_in_queue?
      return true if sidekiq_pipeline_nodes.empty?
      # Если есть running ноды, пайплайн не idle
      return false if sidekiq_pipeline_nodes.where(status: :running).exists?
      # Если есть completed или failed ноды, пайплайн не idle
      return false if sidekiq_pipeline_nodes.where(status: [:completed, :failed]).exists?
      # Если все ноды pending, пайплайн idle
      true
    end

    # Проверить запущен ли пайплайн (алиас для running?)
    def active?
      running?
    end

    # Получить текущий статус как строку (для обратной совместимости)
    def status
      return 'failed' if failed?
      return 'running' if running?
      return 'completed' if completed?
      'idle'
    end

    # Получить текущую выполняющуюся ноду
    def current_node
      sidekiq_pipeline_nodes.running.first
    end

    # Получить завершённые ноды
    def completed_nodes
      sidekiq_pipeline_nodes.completed
    end

    # Получить прогресс пайплайна (процент завершённых нод)
    def progress_percent
      total = sidekiq_pipeline_nodes.count
      return 0 if total.zero?
      
      completed_count = sidekiq_pipeline_nodes.completed.count
      ((completed_count.to_f / total) * 100).round(1)
    end

    private

    # Определить имя очереди Sidekiq для данного провайдера
    def queue_name
      # Имя очереди обычно совпадает с именем провайдера
      # Для специальных случаев можно расширить логику
      pipeline_name
    end

    # Проверить наличие задач в очереди для данного провайдера
    # Проверяет как pending задачи в очереди, так и выполняющиеся задачи
    def has_jobs_in_queue?
      queue_name = self.queue_name
      return false if queue_name.blank?

      # В тестовом окружении проверяем переменную класса для симуляции
      if defined?(Rails) && Rails.env.test? && self.class.test_queues_with_jobs&.include?(queue_name)
        return true
      end

      begin
        # Проверяем pending задачи в очереди
        queue = Sidekiq::Queue.new(queue_name)
        return true if queue.size > 0

        # Проверяем выполняющиеся задачи (busy workers)
        workers = Sidekiq::Workers.new
        workers.each do |_process_id, _thread_id, work|
          return true if work['queue'] == queue_name
        end

        false
      rescue => e
        # Если очередь не существует или произошла ошибка, считаем что задач нет
        Sidekiq.logger.debug "Queue #{queue_name} check failed: #{e.message}"
        false
      end
    end
  end
end

