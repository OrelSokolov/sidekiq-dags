# frozen_string_literal: true

class CreateSidekiqPipelines < ActiveRecord::Migration[7.0]
  def change
    create_table :sidekiq_pipelines do |t|
      t.string :pipeline_name, null: false
      t.integer :status, default: 0
      t.datetime :run_at
      t.timestamps
    end

    add_index :sidekiq_pipelines, :pipeline_name, unique: true
    add_index :sidekiq_pipelines, :status
  end
end

