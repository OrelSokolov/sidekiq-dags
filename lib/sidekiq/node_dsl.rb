module Sidekiq
  module NodeDsl
    def node(name, base = BaseNode, &block)
      klass = Class.new(base)
      klass.class_eval(&block)
      Sidekiq.logger.debug "Setting node #{self}::#{name}"
      const_set(name, klass)
    end
  end
end