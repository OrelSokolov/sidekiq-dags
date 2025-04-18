require 'sidekiq/batch'

module Sidekiq
  class Node
    include Sidekiq::Worker
    include Sidekiq::Batch::Callback

    def self.execute &block
      define_method(:execute) do
        instance_exec(&block)
      end
    end

    def self.desc str
      define_method(:desc) do
        str
      end
    end

    def self.next_node arg
      define_method(:next_node) do
        klass = arg
        if klass.kind_of?(Symbol)
          namespace = self.class.name.deconstantize # "Rustat::EndNode" → "Rustat"
          (namespace + "::" + klass.to_s).constantize
        else
          klass
        end
      end
    end

    def self.observer &block
      define_method(:observer) do
        instance_exec(&block)
      end
    end

    def desc
      "Sidekiq::Node"
    end

    def execute(*args, **kwargs)

    end

    def next_node
      nil
    end

    def observer

    end

    def custom_notifiers(prefix, msg)

    end

    def perform(*args, **kwargs)
      observer
      @batch = Sidekiq::Batch.new

      @batch.jobs do
        DummyJob.perform_async(desc) # Needed for not empty job list
        execute(*args, **kwargs)
      end

      @batch.on(:complete, self.class)

      s = Sidekiq::Batch::ExplicitStatus.new(@batch.bid)
      # Sidekiq.logger.info "#{Time.current.to_f} 🔥 EXISTS? #{s.exists?}".colorize(:red)
      # Sidekiq.logger.info s.data.to_s.colorize(:light_yellow)

      notify_all "➡️ #{desc.present? ? desc : self.class} -> (#{s.total})    | #{@batch.bid}"
    end

    def on_complete(status, options)
      # Sidekiq.logger.info "#{Time.current.to_f} 🔥 ON COMPLETE EXISTS? #{status.exists?}".colorize(:red)
      # Sidekiq.logger.info status.data.to_s.colorize(:yellow)

      notify_all "✔️ #{desc.present? ? desc : self.class} (#{status.total})   |  #{status.bid}"
      if next_node.present?
        notify_all "➕ #{next_node} "
        next_node&.perform_async
      else
        notify_all "✅ Конец графа #{sidekiq_queue}."
      end
    end

    private

    def notify_all(msg)
      prefix = "[#{sidekiq_queue}] "
      Sidekiq.logger.info (prefix + msg).colorize(:blue)
      custom_notifiers(prefix, msg)
    end

    def sidekiq_queue
      self.class.get_sidekiq_options['queue']
    end
  end
end