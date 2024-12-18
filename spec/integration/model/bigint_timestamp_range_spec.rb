# frozen_string_literal: true

require "spec_helper"

RSpec.describe BigintTimestampRange do
  let(:current_time) { Time.current }
  let(:connection) { described_class.connection }
  let(:schema_cache) { connection.schema_cache }
  let(:table_name) { described_class.table_name }

  around(:each) do |example|
    # This class is sensitive to time but spec_helper freezes it to a
    # fixed hour. Let's undo that for the specs here.
    # Also, let's reduce precision to millisecond.
    t = Time.at(Time.now_without_mock_time.to_i)
    Timecop.freeze(t) do
      example.run
    end
  end

  describe ".primary_key" do
    subject { described_class.primary_key }

    it { is_expected.to eq(%w[id created_at]) }
  end

  describe ".create" do
    let(:created_at) { current_time }

    subject { described_class.create!(created_at: created_at) }

    context "when partition key in range" do
      it "has the right id types" do
        expect(subject.id).to be_an(Array)
        expect(subject.id.length).to eq(2)
        expect(subject.id[0]).to be_an(Integer)
        expect(subject.id[1]).to be_an(ActiveSupport::TimeWithZone)
      end

      it "has the right id values" do
        expect(subject.id).not_to be_nil
        expect(subject.id[1]).to eq(created_at)
      end

      its(:created_at) { is_expected.to eq(created_at) }
    end

    context "when partition key outside range" do
      let(:created_at) { current_time - 10.days }

      it "raises error" do
        expect { subject }.to raise_error(ActiveRecord::StatementInvalid, /PG::CheckViolation/)
      end
    end
  end

  describe ".partitions" do
    subject { described_class.partitions }

    context "when query successful" do
      it { is_expected.to contain_exactly("#{table_name}_a", "#{table_name}_b") }
    end

    context "when an error occurs" do
      before { allow(PgParty.cache).to receive(:fetch_partitions).and_raise("boom") }

      it { is_expected.to eq([]) }
    end
  end

  describe ".create_partition" do
    let(:start_range) { current_time + (2 * 24 * 3600) }
    let(:end_range) { current_time + (3 * 24 * 3600) }
    let(:child_table_name) { "#{table_name}_c" }

    subject(:create_partition) do
      described_class.create_partition(
        start_range: start_range,
        end_range: end_range,
        name: child_table_name
      )
    end

    subject(:partitions) { described_class.partitions }
    subject(:child_table_exists) { schema_cache.data_source_exists?(child_table_name) }

    before do
      schema_cache.clear!
      described_class.partitions
    end

    after { connection.drop_table(child_table_name) if child_table_exists }

    context "when ranges do not overlap" do
      it "returns table name and adds it to partition list" do
        expect(create_partition).to eq(child_table_name)

        expect(partitions).to contain_exactly(
          "#{table_name}_a",
          "#{table_name}_b",
          "#{table_name}_c"
        )
      end
    end

    context "when name not provided" do
      let(:child_table_name) { create_partition }

      subject(:create_partition) do
        described_class.create_partition(
          start_range: start_range,
          end_range: end_range,
        )
      end

      it "returns table name and adds it to partition list" do
        expect(create_partition).to match(/^#{table_name}_\w{7}$/)

        expect(partitions).to contain_exactly(
          "#{table_name}_a",
          "#{table_name}_b",
          child_table_name,
        )
      end
    end

    context "when ranges overlap" do
      let(:start_range) { current_time }

      it "raises error and cleans up intermediate table" do
        expect { create_partition }.to raise_error(ActiveRecord::StatementInvalid, /PG::InvalidObjectDefinition/)
        expect(child_table_exists).to eq(false)
      end
    end
  end

  describe ".in_partition" do
    let(:child_table_name) { "#{table_name}_a" }

    subject { described_class.in_partition(child_table_name) }

    its(:table_name) { is_expected.to eq(child_table_name) }
    its(:name)       { is_expected.to eq(described_class.name) }
    its(:new)        { is_expected.to be_an_instance_of(described_class) }
    its(:allocate)   { is_expected.to be_an_instance_of(described_class) }

    describe "query methods" do
      let!(:record_one) { described_class.create!(created_at: current_time) }
      let!(:record_two) { described_class.create!(created_at: current_time + 1.minute) }
      let!(:record_three) { described_class.create!(created_at: current_time + 30.hours) }

      describe ".all" do
        subject { described_class.in_partition(child_table_name).all }

        it { is_expected.to contain_exactly(record_one, record_two) }
      end

      describe ".where" do
        subject { described_class.in_partition(child_table_name).where(id: record_one.id) }

        it { is_expected.to contain_exactly(record_one) }
      end
    end
  end

  describe ".partition_key_in" do
    let(:start_range) { current_time }
    let(:end_range) { current_time + 1.day }

    let!(:record_one) { described_class.create!(created_at: current_time) }
    let!(:record_two) { described_class.create!(created_at: current_time + 1.minute) }
    let!(:record_three) { described_class.create!(created_at: current_time + 30.hours) }

    subject { described_class.partition_key_in(start_range, end_range) }

    context "when spanning a single partition" do
      it { is_expected.to contain_exactly(record_one, record_two) }
    end

    it 'foo' do
      expect(subject[0].id).not_to be_nil
      expect(subject[1].id).not_to be_nil
    end

    context "when spanning multiple partitions" do
      let(:end_range) { current_time + 32.hours }

      it { is_expected.to contain_exactly(record_one, record_two, record_three) }
    end

    context "when chaining methods" do
      subject { described_class.partition_key_in(start_range, end_range).where(id: record_one.id) }

      it { is_expected.to contain_exactly(record_one) }
    end
  end

  xdescribe ".partition_key_eq" do
    let(:partition_key) { current_time }

    let!(:record_one) { described_class.create!(created_at: current_time) }
    let!(:record_two) { described_class.create!(created_at: current_time + 1.minute) }
    let!(:record_three) { described_class.create!(created_at: current_time + 1.day) }

    subject { described_class.partition_key_eq(partition_key) }

    context "when partition key in first partition" do
      it { is_expected.to contain_exactly(record_one, record_two) }
    end

    context "when partition key in second partition" do
      let(:partition_key) { current_date + 1.day }

      it { is_expected.to contain_exactly(record_three) }
    end

    context "when chaining methods" do
      subject do
        described_class
          .in_partition("#{table_name}_b")
          .unscoped
          .partition_key_eq(partition_key)
      end

      it { is_expected.to be_empty }
    end

    context "when table is aliased" do
      subject do
        described_class
          .select("*")
          .from(described_class.arel_table.alias)
          .partition_key_eq(partition_key)
      end

      it { is_expected.to contain_exactly(record_one, record_two) }
    end

    context "when table alias not resolvable" do
      subject do
        described_class
          .select("*")
          .from("garbage")
          .partition_key_eq(partition_key)
      end

      it { expect { subject }.to raise_error("could not find arel table in current scope") }
    end
  end
end
