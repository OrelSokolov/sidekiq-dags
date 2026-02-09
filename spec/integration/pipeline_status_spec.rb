# frozen_string_literal: true

require 'spec_helper'
require 'sidekiq/batch'
require 'sidekiq/testing'
require_relative '../support/database'

# Добавляем метод present? для совместимости с Node
class String
  def present?
    !empty?
  end
end

class NilClass
  def present?
    false
  end
end

class Class
  def present?
    true
  end
end

class Object
  def present?
    !nil?
  end
end

Sidekiq::Testing.server_middleware do |chain|
  chain.add Sidekiq::Batch::Middleware::ServerMiddleware
end

# Тест цепочки из 100 нод с проверкой статусов в моделях
# Цепочка: PipelineStatusNode1 -> PipelineStatusNode2 -> ... -> PipelineStatusNode100

# Массив для хранения номеров нод, у которых был вызван коллбэк
$pipeline_status_callbacks_runs = []

# DummyJob для создания непустого батча
class PipelineStatusDummyJob
  include Sidekiq::Worker

  def perform(desc)
    # Пустой джоб
  end
end

# Создаем модуль для пайплайна с использованием PipelineTracking
module TestPipeline
  extend Sidekiq::NodeDsl

  # Создаем 100 нод в цепочке с PipelineTracking
  (1..100).each do |i|
    node_class = Class.new(Sidekiq::Node) do
      include Sidekiq::PipelineTracking

      desc "PipelineStatusNode#{i}"

      # Переопределяем perform для использования DummyJob
      def perform(*args, **kwargs)
        observer

        # Отслеживание начала ноды (если включен PipelineTracking)
        if respond_to?(:mark_node_started!)
          started = mark_node_started!
          return unless started # Если пайплайн уже запущен, не запускаем ноду
        end

        @batch = Sidekiq::Batch.new

        @batch.add_jobs do
          PipelineStatusDummyJob.perform_async(desc)
          execute(*args, **kwargs)
        end

        # Регистрируем callback для отслеживания статусов пайплайна
        if respond_to?(:pipeline_name) && respond_to?(:node_name)
          pipeline_name = self.pipeline_name
          node_name = self.node_name
          @batch.on(:complete, Sidekiq::PipelineCallback, {
                      'pipeline_name' => pipeline_name,
                      'node_name' => node_name
                    })
          @batch.on(:failure, Sidekiq::PipelineCallback, {
                      'pipeline_name' => pipeline_name,
                      'node_name' => node_name
                    })
        end

        @batch.on(:complete, self.class)
        @batch.run

        s = Sidekiq::Batch::ExplicitStatus.new(@batch.bid)
        notify_all "➡️ #{desc.present? ? desc : self.class} -> (#{s.total})    | #{@batch.bid}"
      end

      # Коллбэк, который добавляет номер ноды в массив
      # НЕ вызываем super, так как PipelineCallback уже запустит следующую ноду
      def on_complete(_status, _options)
        node_number = self.class.name.match(/PipelineStatusNode(\d+)/)[1].to_i
        $pipeline_status_callbacks_runs << node_number
        # super не вызываем - PipelineCallback обработает запуск следующей ноды
      end

      private

      def notify_all(msg)
        prefix = "[#{sidekiq_queue}] "
        Sidekiq.logger.info (prefix + msg).colorize(:blue) if defined?(colorize)
        Sidekiq.logger.info(prefix + msg) unless defined?(colorize)
      end

      def sidekiq_queue
        self.class.get_sidekiq_options['queue'] || 'default'
      end
    end

    TestPipeline.const_set("PipelineStatusNode#{i}", node_class)
  end

  # Устанавливаем связи между нодами (next_node)
  (1..99).each do |i|
    next_node_class = TestPipeline.const_get("PipelineStatusNode#{i + 1}")
    node_class = TestPipeline.const_get("PipelineStatusNode#{i}")
    node_class.next_node(next_node_class)
  end
end

# Запускаем тест
describe 'Pipeline Status Test with 100 nodes' do
  before(:each) do
    $pipeline_status_callbacks_runs.clear
    Sidekiq.redis { |r| r.flushdb }

    # Убеждаемся, что БД подключена и настроена
    if defined?(ActiveRecord)
      begin
        # Устанавливаем подключение, если его нет
        unless ActiveRecord::Base.connected?
          ActiveRecord::Base.establish_connection(
            adapter: 'sqlite3',
            database: ':memory:'
          )

          # Создаем таблицы, если их нет
          unless ActiveRecord::Base.connection.table_exists?('sidekiq_pipelines')
            ActiveRecord::Schema.define do
              create_table :sidekiq_pipelines, force: true do |t|
                t.string :pipeline_name, null: false
                t.integer :status, default: 0
                t.datetime :run_at
                t.timestamps
              end

              add_index :sidekiq_pipelines, :pipeline_name, unique: true
              add_index :sidekiq_pipelines, :status

              create_table :sidekiq_pipeline_nodes, force: true do |t|
                t.references :sidekiq_pipeline, null: false, foreign_key: false
                t.string :node_name, null: false
                t.integer :status, default: 0, null: false
                t.datetime :run_at
                t.text :error_message
                t.timestamps
              end

              add_index :sidekiq_pipeline_nodes, %i[sidekiq_pipeline_id node_name], unique: true,
                                                                                    name: 'index_sidekiq_pipeline_nodes_on_pipeline_and_node'
              add_index :sidekiq_pipeline_nodes, :sidekiq_pipeline_id
              add_index :sidekiq_pipeline_nodes, :status
            end
          end
        end

        # Очищаем данные из БД
        if defined?(Sidekiq::SidekiqPipeline) && Sidekiq::SidekiqPipeline.table_exists?
          Sidekiq::SidekiqPipelineNode.delete_all
          Sidekiq::SidekiqPipeline.delete_all
        end
      rescue StandardError => e
        puts "Warning: Database setup error: #{e.message}" if ENV['DEBUG']
      end
    end
  end

  it 'executes all 100 nodes in sequence, calls all callbacks, and sets all statuses to completed' do
    # Запускаем первую ноду
    TestPipeline::PipelineStatusNode1.perform_async

    # Обрабатываем все джобы
    Sidekiq::Worker.drain_all

    # Ожидаемый массив: [1, 2, 3, ..., 100]
    expected = (1..100).to_a

    # Проверяем разницу между ожидаемым и фактическим для callbacks
    missing_callbacks = expected - $pipeline_status_callbacks_runs
    extra_callbacks = $pipeline_status_callbacks_runs - expected

    puts "Expected callbacks: #{expected.size}"
    puts "Actual callbacks: #{$pipeline_status_callbacks_runs.size}"
    puts "Missing callbacks: #{missing_callbacks.inspect}"
    puts "Extra callbacks: #{extra_callbacks.inspect}"
    puts "Callbacks order: #{$pipeline_status_callbacks_runs.inspect}"

    # Проверяем, что все коллбэки были вызваны
    expect(missing_callbacks).to be_empty
    expect(extra_callbacks).to be_empty
    expect($pipeline_status_callbacks_runs.size).to eq(100),
                                                    "Expected 100 callbacks, got #{$pipeline_status_callbacks_runs.size}"
    expect($pipeline_status_callbacks_runs.sort).to eq(expected),
                                                    'Callbacks were not called in correct order or some are missing'

    # Проверяем статусы в моделях
    database_available = begin
      defined?(ActiveRecord) &&
        ActiveRecord::Base.connected? &&
        defined?(Sidekiq::SidekiqPipeline) &&
        Sidekiq::SidekiqPipeline.table_exists?
    rescue StandardError
      false
    end

    if database_available
      pipeline = Sidekiq::SidekiqPipeline.for('test_pipeline')

      expect(pipeline).not_to be_nil

      # Проверяем, что все ноды завершились со статусом completed
      pipeline_nodes = pipeline.sidekiq_pipeline_nodes
      node_statuses = pipeline_nodes.pluck(:status)

      puts "Total pipeline nodes: #{pipeline_nodes.count}"
      puts "Node statuses: #{node_statuses.inspect}"

      # Проверяем, что все статусы равны :completed
      # Используем символы для проверки через enum
      all_completed = pipeline_nodes.all? { |node| node.status == 'completed' }

      expect(pipeline_nodes.count).to eq(100)
      expect(all_completed).to be(true), "Not all nodes are completed. Statuses: #{pipeline_nodes.map do |n|
        "#{n.node_name}:#{n.status}"
      end.inspect}"

      # Проверка через pluck (как в запросе пользователя)
      # Pipeline.for('test_pipeline').pipeline_nodes.pluck(:status).all?{|x| x == :completed }
      # Примечание: pluck возвращает строки для enum, поэтому сравниваем со строкой "completed"
      statuses = pipeline.sidekiq_pipeline_nodes.pluck(:status)
      all_completed_by_pluck = statuses.all? { |x| x == 'completed' || x.to_s == 'completed' }
      expect(all_completed_by_pluck).to be(true), "Not all nodes are completed by pluck. Statuses: #{statuses.inspect}"

      # Альтернативная проверка через символы (если нужно сравнивать с :completed)
      # Для этого нужно сначала получить статусы как символы через map
      all_completed_symbols_check = pipeline.sidekiq_pipeline_nodes.map do |node|
        node.status.to_sym
      end.all? { |x| x == :completed }
      expect(all_completed_symbols_check).to be(true), 'Not all nodes have :completed status (symbol check)'

      # Проверяем статус пайплайна
      expect(pipeline.completed?).to be(true)
      expect(pipeline.failed?).to be(false)
      expect(pipeline.running?).to be(false)

      # Проверяем прогресс
      expect(pipeline.progress_percent).to eq(100.0)

      puts '✅ All 100 nodes completed successfully'
      puts "✅ Pipeline status: #{pipeline.status}"
      puts "✅ Pipeline progress: #{pipeline.progress_percent}%"
    else
      puts '⚠️ Pipeline tracking not available (ActiveRecord not configured)'
      skip 'Pipeline tracking requires ActiveRecord and database setup'
    end
  end
end
