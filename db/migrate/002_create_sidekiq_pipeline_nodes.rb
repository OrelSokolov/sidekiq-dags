# frozen_string_literal: true

class CreateSidekiqPipelineNodes < ActiveRecord::Migration[7.0]
  def change
    create_table :sidekiq_pipeline_nodes do |t|
      t.references :sidekiq_pipeline, null: false, foreign_key: { to_table: :sidekiq_pipelines }
      t.string :node_name, null: false
      t.string :bid
      t.integer :status, default: 0, null: false
      t.datetime :run_at
      t.text :error_message
      t.timestamps
    end

    add_index :sidekiq_pipeline_nodes, [:sidekiq_pipeline_id, :node_name], unique: true, name: 'index_sidekiq_pipeline_nodes_on_pipeline_and_node'
    add_index :sidekiq_pipeline_nodes, :sidekiq_pipeline_id
    add_index :sidekiq_pipeline_nodes, :status
  end
end
