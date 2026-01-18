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
      Sidekiq.logger.info "HANDLE EVENT: #{event_type} for node: #{node_name}"
      
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

      Sidekiq.logger.info "Collecting pending data for #{node_name}"

      redis_data = {}
      
      # Получаем pending напрямую из Redis для более надежной проверки
      # Используем блокировку Redis для атомарного чтения данных (как в других местах гема)
      Sidekiq::Batch.with_redis_lock("batch-lock-#{status.bid}", timeout: 5) do
        redis_data[:batch_pending] = begin
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
        redis_data[:status_pending] = begin
          status.pending
        rescue => e
          nil
        end
        
        # Получаем количество failures для проверки
        # status.failures возвращает число (Integer), а не массив!
        redis_data[:failures_count] = begin
          status.failures
        rescue => e
          Sidekiq.logger.debug "⚠️ Could not get failures from batch status: #{e.message}"
          0
        end.to_i
      end

      batch_pending = redis_data[:batch_pending]
      status_pending = redis_data[:status_pending]
      failures_count = redis_data[:failures_count]


      Sidekiq.logger.info "PENDING DATA RECEIVED for #{node_name}"
      
      # Логируем информацию о батче
      Sidekiq.logger.info "🔔 Batch callback #{event_type} for #{pipeline_name}::#{node_name} (bid: #{status.bid}, pending: #{batch_pending}, status.pending: #{status_pending}, failures: #{failures_count})"
      
      # Если pending не nil и не 0, проверяем, не равен ли он количеству failures
      # Если pending == failures, то это нормальная ситуация (все pending jobs - это failed jobs)
      # Это НЕ race condition!

      Sidekiq.logger.info "Batch pending is #{batch_pending.inspect} and failures count is #{failures_count.inspect}".colorize(:light_yellow)

      if batch_pending.present? && batch_pending > 0
        if batch_pending == failures_count
          # Это нормально: pending == failures, все pending jobs - это failed jobs
          Sidekiq.logger.info "✅ Batch #{status.bid} has #{batch_pending} pending jobs, but all are failures (#{failures_count}) - this is normal, not a race condition"
        elsif batch_pending > failures_count
          # Это race condition: pending > failures, значит есть еще работающие джобы
          error_message = "⏸️ Ignoring #{event_type} callback - batch #{status.bid} still has #{batch_pending} pending jobs (#{failures_count} failures) (race condition detected)"
          Sidekiq.logger.error error_message
          raise RuntimeError, error_message
        end
      else
        Sidekiq.logger.info "Strange!".colorize(:red)
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
      else
        Sidekiq.logger.info "Strange too! Pending is #{batch_pending.inspect} and event type is #{event_type}".colorize(:red)
      end

      Sidekiq.logger.info "[Processing event type]".colorize(:yellow)
      
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
        
        # Если сообщение не передано напрямую, пытаемся получить из status.failure_info
        # status.failures возвращает число, а status.failure_info возвращает массив JID'ов
        unless error_msg
          failure_info = begin
            status.failure_info
          rescue => e
            Sidekiq.logger.debug "⚠️ Could not get failure_info from batch status: #{e.message}"
            []
          end
          
          # Проверяем, что failure_info - это массив
          failure_info = [] unless failure_info.is_a?(Array)
          
          if failure_info.any?
            # Получаем первый JID failed job
            first_failure_jid = failure_info.first
            
            # JID может быть строкой (JSON) или просто строкой
            # Пытаемся извлечь сообщение об ошибке, если оно есть в JID
            error_msg = if first_failure_jid.is_a?(String)
              # Пытаемся распарсить как JSON, если не получится - используем как есть
              parsed = JSON.parse(first_failure_jid) rescue nil
              if parsed && (parsed['errmsg'] || parsed[:errmsg])
                parsed['errmsg'] || parsed[:errmsg]
              else
                "Batch failed (failed job: #{first_failure_jid})"
              end
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
        # status.failures возвращает число (Integer), а не массив!
        Sidekiq.logger.info "[ROCESSING COMPLETE EVENT]".colorize(:green)
        failures_count = begin
          status.failures
        rescue => e
          Sidekiq.logger.info "⚠️ Could not get failures from batch status: #{e.message}"
          0
        end
        
        failures_count = failures_count.to_i
        
        
        # on_complete - единственное место для завершения ноды и запуска следующей
        # Перезагружаем ноду из БД, чтобы убедиться, что у нас актуальное состояние
        node_record.reload

        Sidekiq.logger.info "Node record loaded".colorize(:light_yellow)
        Sidekiq.logger.info "Node: #{node_record.inspect}".colorize(:light_yellow)
        
        # Проверяем, что нода действительно в статусе running или pending перед завершением
        # Если нода в pending, значит mark_node_started! не был вызван, но батч завершился - помечаем как running и затем completed
        if node_record.pending?
          node_record.start!
          Sidekiq.logger.info "⚠️ Node #{pipeline_name}::#{node_name} was pending, marking as running"
        end
        
        unless node_record.running?
          Sidekiq.logger.info "⚠️ Batch complete event ignored - node #{pipeline_name}::#{node_name} is not in running status (current: #{node_record.status})"
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
          Sidekiq.logger.info "⚠️ Batch complete event ignored - node already #{node_record.status}"
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

