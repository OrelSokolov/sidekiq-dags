# frozen_string_literal: true

module Sidekiq
  # Внутренний класс для работы с Redis
  class PipelineStatus
    class << self
      def redis
        Sidekiq.redis { |conn| conn }
      end

      # Pipeline level
      def running?(pipeline_name)
        redis.get("pipeline:#{pipeline_name}:status") == "running"
      end

      def start!(pipeline_name)
        redis.multi do |pipe|
          pipe.set("pipeline:#{pipeline_name}:status", "running")
          pipe.set("pipeline:#{pipeline_name}:run_at", Time.current.iso8601)
        end
      end

      def finish!(pipeline_name, success: true)
        redis.set("pipeline:#{pipeline_name}:status", success ? "completed" : "failed")
      end

      def reset!(pipeline_name)
        keys = redis.keys("pipeline:#{pipeline_name}:nodes:*")
        redis.del(*keys) if keys.any?
        redis.del("pipeline:#{pipeline_name}:status")
        redis.del("pipeline:#{pipeline_name}:run_at")
      end

      # Node level
      def node_status(pipeline_name, node_name)
        redis.get("pipeline:#{pipeline_name}:nodes:#{node_name}:status") || "pending"
      end

      def node_start!(pipeline_name, node_name, bid: nil)
        redis.multi do |pipe|
          pipe.set("pipeline:#{pipeline_name}:nodes:#{node_name}:status", "running")
          pipe.set("pipeline:#{pipeline_name}:nodes:#{node_name}:run_at", Time.current.iso8601)
          if bid
            pipe.set("pipeline:#{pipeline_name}:nodes:#{node_name}:bid", bid)
            pipe.set("pipeline:bid:#{bid}", "#{pipeline_name}:#{node_name}")
          end
        end
      end

      def node_complete!(pipeline_name, node_name)
        redis.set("pipeline:#{pipeline_name}:nodes:#{node_name}:status", "completed")
      end

      def node_fail!(pipeline_name, node_name, error = nil)
        redis.multi do |pipe|
          pipe.set("pipeline:#{pipeline_name}:nodes:#{node_name}:status", "failed")
          pipe.set("pipeline:#{pipeline_name}:nodes:#{node_name}:error", error.to_s) if error
        end
      end

      def node_bid(pipeline_name, node_name)
        redis.get("pipeline:#{pipeline_name}:nodes:#{node_name}:bid")
      end

      def save_bid!(pipeline_name, node_name, bid)
        return unless bid
        redis.multi do |pipe|
          pipe.set("pipeline:#{pipeline_name}:nodes:#{node_name}:bid", bid)
          pipe.set("pipeline:bid:#{bid}", "#{pipeline_name}:#{node_name}")
        end
      end

      def all_node_statuses(pipeline_name)
        keys = redis.keys("pipeline:#{pipeline_name}:nodes:*:status")
        return {} if keys.empty?
        
        values = redis.mget(*keys)
        keys.each_with_object({}).with_index do |(key, hash), index|
          node_name = key.match(/nodes:([^:]+):status/)&.[](1)
          hash[node_name] = values[index] if node_name
        end
      end

      def running_node_ids(pipeline_name)
        all_node_statuses(pipeline_name)
          .select { |_, status| status == "running" }
          .keys
      end

      def completed_nodes_count(pipeline_name)
        all_node_statuses(pipeline_name)
          .count { |_, status| %w[completed skipped].include?(status) }
      end

      def node_progress(pipeline_name, node_name)
        data = redis.hgetall("pipeline:#{pipeline_name}:nodes:#{node_name}:progress")
        return nil if data.empty?
        
        {
          bid: data['bid'],
          max: data['total'].to_i,
          pending: data['pending'].to_i,
          done: data['done'].to_i,
          progress_percentage: data['progress_percentage'].to_f
        }
      end

      def find_node_by_bid(bid)
        data = redis.get("pipeline:bid:#{bid}")
        return nil unless data
        
        parts = data.split(":", 2)
        { pipeline_name: parts[0], node_id: parts[1] }
      end
    end
  end
end
