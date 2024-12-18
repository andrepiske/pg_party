# frozen_string_literal: true

class BigintTimestampRange < ApplicationRecord
  range_partition_by :created_at
end
