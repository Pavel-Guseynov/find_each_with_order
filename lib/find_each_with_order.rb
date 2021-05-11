# frozen_string_literal: true

require_relative "find_each_with_order/version"

module ActiveRecord
  module FindEachWithOrder
    def find_each_with_order(batch_size: 1000, order_key: :id, order: :asc, &block)
      find_in_batches_with_order(batch_size: batch_size, order_key: order_key, order: order) do |records|
        records.each(&block)
      end
    end

    def find_in_batches_with_order(batch_size: 1000, order_key: :id, order: :asc)
      in_batches_with_order(of: batch_size, order_key: order_key, order: order) do |batch|
        yield batch.to_a
      end
    end

    def in_batches_with_order(of: 1000, order_key: :id, order: :asc)
      relation = self

      batch_limit = of

      relation = relation.reorder(table[order_key].public_send(order)).limit(batch_limit)
      relation.skip_query_cache! # Retaining the results in the query cache would undermine the point of batching
      batch_relation = relation

      loop do
        records = batch_relation.records
        ids = records.map(&:id)
        yielded_relation = where(primary_key => ids)
        yielded_relation.load_records(records)

        break if ids.empty?

        primary_key_offset = records.last.public_send(order_key)
        raise ArgumentError, "Primary key not included in the custom select clause" unless primary_key_offset

        yield yielded_relation

        break if ids.length < batch_limit

        batch_relation = relation.where(
          predicate_builder[order_key, primary_key_offset, order == :desc ? :lt : :gt]
        )
      end
    end
  end

  class Relation
    include FindEachWithOrder
  end
end
