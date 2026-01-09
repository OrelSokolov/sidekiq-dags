# frozen_string_literal: true

require 'json'

module Sidekiq
  # Класс для обработки событий батчей нод пайплайна
  # Вызывается автоматически при событиях :complete, :failure батча
  # Используется только :complete для завершения ноды и запуска следующей (избегаем дублирования)
  class PipelineCallback
    def on_success(status, options)
      # on_success больше не используется - вся логика в on_complete
      # Оставляем метод для совместимости, если sidekiq-batch все еще вызывает его
      Sidekiq.logger.debug "📞 PipelineCallback.on_success called for bid: #{status.bid} (ignored, using on_complete instead)"
    end

    def on_complete(status, options)
      Sidekiq.logger.info "📞 PipelineCallback.on_complete called for bid: #{status.bid}"
      handle_event(status, options, 'complete')
    end

    def on_failure(status, options)
      Sidekiq.logger.info "📞 PipelineCallback.on_failure called for bid: #{status.bid}"
      handle_event(status, options, 'failure')
    end

    private

    def handle_event(status, options, event_type)
      pipeline_name = options['pipeline_name'] || options[:pipeline_name]
      node_name = options['node_name'] || options[:node_name]
      
      return unless pipeline_name && node_name
      
      # Проверяем доступность БД перед использованием моделей
      return unless defined?(ActiveRecord) && ActiveRecord::Base.connected?
      
      begin
        node_record = SidekiqPipelineNode.for(pipeline_name, node_name)
        pipeline = SidekiqPipeline.for(pipeline_name)
        
        return unless node_record && pipeline
      rescue ::ActiveRecord::ConnectionNotDefined, ::ActiveRecord::NoDatabaseError => e
        Sidekiq.logger.debug "Pipeline tracking disabled: #{e.message}"
        return
      end
      
      # Проверяем, что батч действительно завершен
      # Это важно, так как коллбэки могут срабатывать преждевременно из-за race conditions
      # в sidekiq-batch
      
      # Получаем pending напрямую из Redis для более надежной проверки
      batch_pending = begin
        Sidekiq.redis do |conn|
          bidkey = "BID-#{status.bid}"
          pending_str = conn.hget(bidkey, "pending")
          pending_str ? pending_str.to_i : nil
        end
      rescue => e
        Sidekiq.logger.warn "⚠️ Could not get pending count for batch #{status.bid}: #{e.message}"
        nil
      end
      
      # Также проверяем через status.pending для логирования
      status_pending = begin
        status.pending
      rescue => e
        nil
      end
      
      # Получаем количество failures для проверки
      batch_failures = begin
        status.failures
      rescue => e
        Sidekiq.logger.debug "⚠️ Could not get failures from batch status: #{e.message}"
        []
      end
      
      # Проверяем, что failures - это массив
      batch_failures = [] unless batch_failures.is_a?(Array)
      failures_count = batch_failures.size
      
      # Логируем информацию о батче
      Sidekiq.logger.info "🔔 Batch callback #{event_type} for #{pipeline_name}::#{node_name} (bid: #{status.bid}, pending: #{batch_pending}, status.pending: #{status_pending}, failures: #{failures_count})"
      
      # Если pending не nil и не 0, проверяем, не равен ли он количеству failures
      # Если pending == failures, то это нормальная ситуация (все pending jobs - это failed jobs)
      # Это НЕ race condition!
      if batch_pending && batch_pending > 0
        if batch_pending == failures_count
          # Это нормально: pending == failures, все pending jobs - это failed jobs
          Sidekiq.logger.info "✅ Batch #{status.bid} has #{batch_pending} pending jobs, but all are failures (#{failures_count}) - this is normal, not a race condition"
        elsif batch_pending > failures_count
          # Это race condition: pending > failures, значит есть еще работающие джобы
          error_message = "⏸️ Ignoring #{event_type} callback - batch #{status.bid} still has #{batch_pending} pending jobs (#{failures_count} failures) (race condition detected)"
          Sidekiq.logger.error error_message
          raise RuntimeError, error_message
        end
      end
      
      # Дополнительная проверка: если pending = nil, это может означать что батч еще не инициализирован
      # или был удален. Проверяем статус ноды в БД:
      # - Если нода в running, значит батч был создан и, вероятно, уже завершился (удален из Redis)
      # - Если нода уже completed/failed, событие уже обработано - можно пропустить
      if batch_pending.nil? && event_type != 'failure'
        node_record.reload
        if node_record.completed? || node_record.failed?
          Sidekiq.logger.debug "⚠️ Batch #{status.bid} pending is nil, but node already #{node_record.status} - skipping #{event_type} callback"
          return
        elsif node_record.running?
          # Батч был удален из Redis, но нода еще в running - обрабатываем событие
          # Это нормальная ситуация: батч завершился и был удален до того, как callback выполнился
          Sidekiq.logger.info "⚠️ Batch #{status.bid} pending is nil (batch deleted from Redis), but node is running - processing #{event_type} callback"
        else
          # Нода еще не запущена - батч еще не инициализирован
          Sidekiq.logger.warn "⚠️ Batch #{status.bid} pending is nil and node is #{node_record.status}, skipping #{event_type} callback (batch may not be initialized yet)"
          return
        end
      end
      
      case event_type
      when 'success'
        # on_success больше не обрабатывается - вся логика в on_complete
        # Этот case не должен вызываться, так как on_success не регистрируется
        Sidekiq.logger.debug "⚠️ on_success event received but not registered - ignoring"
        return
        
      when 'failure'
        # Батч завершился с ошибкой
        # Сначала проверяем, передано ли сообщение об ошибке напрямую через опции (для тестов)
        error_msg = options['error_message'] || options[:error_message]
        
        # Если сообщение не передано напрямую, пытаемся получить из status.failures
        unless error_msg
          failures = begin
            status.failures
          rescue => e
            Sidekiq.logger.debug "⚠️ Could not get failures from batch status: #{e.message}"
            []
          end
          
          # Проверяем, что failures - это массив
          failures = [] unless failures.is_a?(Array)
          
          if failures.any?
            # Получаем первую ошибку
            first_failure = failures.first
            
            # Ошибка может быть строкой (JSON) или хешем
            error_msg = if first_failure.is_a?(String)
              # Парсим JSON строку
              parsed = JSON.parse(first_failure) rescue {}
              parsed['errmsg'] || parsed[:errmsg] || 'Batch failed'
            elsif first_failure.is_a?(Hash)
              first_failure['errmsg'] || first_failure[:errmsg] || 'Batch failed'
            else
              'Batch failed'
            end
          else
            error_msg = 'Batch failed'
          end
        end
        
        # Перезагружаем ноду из БД, чтобы убедиться, что у нас актуальное состояние
        node_record.reload
        
        node_record.fail!(error_msg)
        pipeline.finish!(success: false, error: error_msg)
        Sidekiq.logger.error "❌ Node #{pipeline_name}::#{node_name} failed via batch failure event (bid: #{status.bid}): #{error_msg}"
        
      when 'complete'
        # on_complete вызывается всегда, даже если были ошибки
        # Проверяем, не было ли ошибок
        failures = begin
          status.failures
        rescue => e
          Sidekiq.logger.debug "⚠️ Could not get failures from batch status: #{e.message}"
          []
        end
        
        # Проверяем, что failures - это массив и он не пустой
        failures = [] unless failures.is_a?(Array)
        
        if failures.any?
          # Ошибка уже обработана в on_failure
          Sidekiq.logger.debug "⚠️ Batch complete event ignored - failures present: #{failures.size}"
          return
        end
        
        # on_complete - единственное место для завершения ноды и запуска следующей
        # Перезагружаем ноду из БД, чтобы убедиться, что у нас актуальное состояние
        node_record.reload
        
        # Проверяем, что нода действительно в статусе running или pending перед завершением
        # Если нода в pending, значит mark_node_started! не был вызван, но батч завершился - помечаем как running и затем completed
        if node_record.pending?
          node_record.start!
          Sidekiq.logger.debug "⚠️ Node #{pipeline_name}::#{node_name} was pending, marking as running"
        end
        
        unless node_record.running?
          Sidekiq.logger.debug "⚠️ Batch complete event ignored - node #{pipeline_name}::#{node_name} is not in running status (current: #{node_record.status})"
          return
        end
        
        unless node_record.completed? || node_record.failed?
          node_record.complete!
          Sidekiq.logger.info "✅ Node #{pipeline_name}::#{node_name} completed via batch complete event (bid: #{status.bid})"
          
          if node_name == 'EndNode'
            pipeline.finish!(success: true)
            Sidekiq.logger.info "🏁 Pipeline #{pipeline_name} finished successfully"
          else
            # Запускаем следующую ноду
            trigger_next_node(pipeline_name, node_name)
          end
        else
          Sidekiq.logger.debug "⚠️ Batch complete event ignored - node already #{node_record.status}"
        end
      end
    rescue RuntimeError => e
      # Пробрасываем RuntimeError дальше (для race condition detection в тестах)
      raise
    rescue => e
      Sidekiq.logger.error "💥 Error in PipelineCallback for #{pipeline_name}::#{node_name}: #{e.message}"
      Sidekiq.logger.error e.backtrace.join("\n")
    end

    # Запускает следующую ноду пайплайна
    # pipeline_name: например "bsight" (lowercase)
    # node_name: например "RootNode"
    def trigger_next_node(pipeline_name, node_name)
      # Ищем класс ноды по имени в правильном модуле (используя pipeline_name)
      current_node_class = find_node_class_by_name(node_name, pipeline_name)
      
      unless current_node_class
        Sidekiq.logger.error "❌ Could not find node class with name: #{node_name} in pipeline: #{pipeline_name}"
        return
      end
      
      begin
        node_instance = current_node_class.new
        
        # Получаем следующую ноду
        next_node_class = node_instance.next_node
        
        if next_node_class && (next_node_class.respond_to?(:present?) ? next_node_class.present? : !next_node_class.nil?)
          puts "[MOVE TO THE NEXT NODE]"
          Sidekiq.logger.info "➡️ Triggering next node: #{next_node_class.name}"
          next_node_class.perform_async
        else
          Sidekiq.logger.info "🏁 No next node after #{current_node_class.name}, pipeline flow complete"
        end
      rescue => e
        Sidekiq.logger.error "❌ Error triggering next node after #{current_node_class.name}: #{e.message}"
        Sidekiq.logger.error e.backtrace.first(5).join("\n")
      end
    end
    
    # Поиск класса ноды по имени в конкретном модуле пайплайна
    # pipeline_name: например "bsight" (lowercase)
    # node_name: например "RootNode"
    def find_node_class_by_name(node_name, pipeline_name = nil)
      # Сначала пытаемся найти в конкретном модуле пайплайна
      if pipeline_name
        # Преобразуем pipeline_name в CamelCase (bsight -> Bsight, rustat -> Rustat)
        module_name = pipeline_name.to_s.split('_').map(&:capitalize).join
        
        begin
          # Пытаемся получить модуль по имени
          if Object.const_defined?(module_name, false)
            pipeline_module = Object.const_get(module_name, false)
            if pipeline_module.is_a?(Module) && pipeline_module.const_defined?(node_name, false)
              node_class = pipeline_module.const_get(node_name, false)
              if node_class.is_a?(Class) && node_class < Sidekiq::Node
                Sidekiq.logger.debug "Found node class in pipeline module: #{node_class.name}"
                return node_class
              end
            end
          end
        rescue => e
          Sidekiq.logger.debug "Error finding node in pipeline module #{module_name}: #{e.message}"
        end
      end
      
      # Fallback: ищем во всех модулях верхнего уровня (для совместимости)
      Object.constants.each do |const_name|
        begin
          const = Object.const_get(const_name)
          next unless const.is_a?(Module)
          
          # Проверяем, есть ли в этом модуле класс с нужным именем
          if const.const_defined?(node_name, false)
            node_class = const.const_get(node_name, false)
            # Проверяем, что это класс и он наследуется от Sidekiq::Node
            if node_class.is_a?(Class) && node_class < Sidekiq::Node
              Sidekiq.logger.debug "Found node class (fallback): #{node_class.name}"
              return node_class
            end
          end
        rescue => e
          # Игнорируем ошибки при поиске
          next
        end
      end
      
      nil
    end
  end
end

