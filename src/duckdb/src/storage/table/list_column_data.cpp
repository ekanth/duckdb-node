#include "duckdb/storage/table/list_column_data.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"
#include "duckdb/common/enums/vector_type.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include <iostream>

namespace duckdb {

ListColumnData::ListColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index, idx_t start_row,
                               LogicalType type_p, optional_ptr<ColumnData> parent)
    : ColumnData(block_manager, info, column_index, start_row, std::move(type_p), parent),
      validity(block_manager, info, 0, start_row, *this) {
	D_ASSERT(type.InternalType() == PhysicalType::LIST);
	auto &child_type = ListType::GetChildType(type);
	// the child column, with column index 1 (0 is the validity mask)
	child_column = ColumnData::CreateColumnUnique(block_manager, info, 1, start_row, child_type, this);
	// the offset_length column, with column index 2 (0 is the validity mask)
	// Use GetOffsetLengthType here.
	child_list_t<LogicalType> make_struct_children {
	    {"offset", LogicalType::BIGINT}, {"length", LogicalType::BIGINT}};
	auto offset_length_type = LogicalType::STRUCT(make_struct_children);
	offset_length_column = ColumnData::CreateColumnUnique(block_manager, info, 2, start_row, offset_length_type, this);
}

void ListColumnData::SetStart(idx_t new_start) {
	this->start = new_start;
	child_column->SetStart(new_start);
	validity.SetStart(new_start);
	offset_length_column->SetStart(new_start);
}

bool ListColumnData::CheckZonemap(ColumnScanState &state, TableFilter &filter) {
	// table filters are not supported yet for list columns
	return false;
}

void ListColumnData::InitializeScan(ColumnScanState &state) {
	state.row_index = 0;
	state.current = nullptr;

	// initialize the validity segment
	D_ASSERT(state.child_states.size() == 3);
	validity.InitializeScan(state.child_states[0]);

	// initialize the child scan
	child_column->InitializeScan(state.child_states[1]);

	//state.child_states[2].Initialize(offset_length_column->type);
	offset_length_column->InitializeScan(state.child_states[2]);
}

list_entry_t ListColumnData::FetchListOffset(idx_t row_idx) {
	Vector offset_length_vector(offset_length_column->type);
	ColumnScanState state;
	auto fetch_count = offset_length_column->Fetch(state, row_idx, offset_length_vector);
	D_ASSERT(fetch_count > 0);

	auto offset_length_value = StructValue::GetChildren(offset_length_vector.GetValue(0));
    list_entry_t offset_length;
	offset_length.offset = offset_length_value[0].GetValue<uint64_t>();
	offset_length.length = offset_length_value[1].GetValue<uint64_t>();

	return offset_length;
}

void ListColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	if (row_idx == 0) {
		InitializeScan(state);
		return;
	}
	state.row_index = 0;
	state.current = nullptr;

	// initialize the validity segment
	D_ASSERT(state.child_states.size() == 3);
	validity.InitializeScanWithOffset(state.child_states[0], row_idx);

	offset_length_column->InitializeScanWithOffset(state.child_states[2], row_idx);

	// we need to read the list at position row_idx to get the correct row offset of the child
	auto child_offset = FetchListOffset(row_idx);
	D_ASSERT(child_offset.offset <= child_column->GetMaxEntry());
	if (child_offset.offset < child_column->GetMaxEntry()) {
		child_column->InitializeScanWithOffset(state.child_states[1], start + child_offset.offset);
	}
	state.last_offset = child_offset.offset;
}

idx_t ListColumnData::ScanHelper(Vector &offset_length_vector, idx_t scan_count, ColumnScanState &state, Vector &result) {
	uint64_t max_offset = 0;
	auto base_offset = this->start;
	auto min_offset = state.last_offset;

	// shift all offsets so they are 0 at the first entry
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	for (idx_t i = 0; i < scan_count; i++) {
		auto values = StructValue::GetChildren(offset_length_vector.GetValue(i));
		result_data[i].offset = values[0].GetValue<uint64_t>();
		result_data[i].length = values[1].GetValue<uint64_t>();
		max_offset = std::max(max_offset, result_data[i].offset + result_data[i].length);
		min_offset = std::min(min_offset, values[0].GetValue<uint64_t>());
	}

	// Make offsets relative to the min_offset
	for (idx_t i = 0; i < scan_count; i++) {
		result_data[i].offset -= min_offset;
	}
	// Set child_state's row_index appropriately to min_offset
	D_ASSERT(min_offset <= child_column->GetMaxEntry());
	if (min_offset < child_column->GetMaxEntry()) {
		child_column->InitializeScanWithOffset(state.child_states[1], base_offset + min_offset);
	}

	D_ASSERT(max_offset >= min_offset);
	idx_t child_scan_count = max_offset - min_offset;
	ListVector::Reserve(result, child_scan_count);

	ListVector::SetListSize(result, child_scan_count);
	state.last_offset = max_offset;

	return child_scan_count;
}

idx_t ListColumnData::Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result) {
	auto scan_count = validity.Scan(transaction, vector_index, state.child_states[0], result);

	Vector offset_length_vector(offset_length_column->type, scan_count);
	offset_length_column->Scan(transaction, vector_index, state.child_states[2], offset_length_vector);

	auto child_scan_count = ScanHelper(offset_length_vector, scan_count, state, result);

	// D_ASSERT(child_scan_count > 0);
	if (child_scan_count > 0) {
		auto &child_entry = ListVector::GetEntry(result);
		if (child_entry.GetType().InternalType() != PhysicalType::STRUCT &&
		    child_entry.GetType().InternalType() != PhysicalType::ARRAY &&
		    state.child_states[1].row_index + child_scan_count > child_column->start + child_column->GetMaxEntry()) {
			std::cout << "In LCD::Scan - state.child_states[1].row_index: " << state.child_states[1].row_index
					  << " child_scan_count: " << child_scan_count
					  << " child_column->start: " << child_column->start
					  << " child_column->GetMaxEntry(): " << child_column->GetMaxEntry()
					  << std::endl;
			throw InternalException("ListColumnData::Scan - internal list scan offset is out of range");
		}
		child_column->ScanCount(state.child_states[1], child_entry, child_scan_count);
	}

	return scan_count;
}

idx_t ListColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates) {
	auto scan_count = validity.ScanCommitted(vector_index, state.child_states[0], result, allow_updates);

	Vector offset_length_vector(offset_length_column->type, scan_count);
	offset_length_column->ScanCommitted(vector_index, state.child_states[2], offset_length_vector, allow_updates);

	auto child_scan_count = ScanHelper(offset_length_vector, scan_count, state, result);

	//D_ASSERT(child_scan_count > 0);
	if (child_scan_count > 0) {
		auto &child_entry = ListVector::GetEntry(result);
		if (child_entry.GetType().InternalType() != PhysicalType::STRUCT &&
		    child_entry.GetType().InternalType() != PhysicalType::ARRAY &&
		    state.child_states[1].row_index + child_scan_count > child_column->start + child_column->GetMaxEntry()) {
			throw InternalException("ListColumnData::ScanCommitted - internal list scan offset is out of range");
		}
		child_column->ScanCount(state.child_states[1], child_entry, child_scan_count);
	}

	return scan_count;
}

idx_t ListColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t count) {
	if (count == 0) {
		return 0;
	}

	auto scan_count = validity.ScanCount(state.child_states[0], result, count);

	Vector offset_length_vector(offset_length_column->type, count);
	offset_length_column->ScanCount(state.child_states[2], offset_length_vector, count);

	auto child_scan_count = ScanHelper(offset_length_vector, scan_count, state, result);

	//D_ASSERT(child_scan_count > 0);
	if (child_scan_count > 0) {
		auto &child_entry = ListVector::GetEntry(result);
		if (child_entry.GetType().InternalType() != PhysicalType::STRUCT &&
		    child_entry.GetType().InternalType() != PhysicalType::ARRAY &&
		    state.child_states[1].row_index + child_scan_count > child_column->start + child_column->GetMaxEntry()) {
			throw InternalException("ListColumnData::ScanCount - internal list scan offset is out of range");
		}
		child_column->ScanCount(state.child_states[1], child_entry, child_scan_count);
	}

	return scan_count;
}

void ListColumnData::Skip(ColumnScanState &state, idx_t count) {
	// skip inside the validity segment
	validity.Skip(state.child_states[0], count);

	offset_length_column->Skip(state.child_states[2], count);
	if (!state.child_states[2].current) {
		// we have gone past all the segments
		return;
	}

	// we need to read the list entries/offsets to figure out how much to skip
	// note that we only need to read the first and last entry
	// however, let's just read all "count" entries for now
	Vector offset_length_vector(offset_length_column->type, 1);
	idx_t scan_count = offset_length_column->ScanCount(state.child_states[2], offset_length_vector, 1);
	if (scan_count == 0) {
		return;
	}

	auto values = StructValue::GetChildren(offset_length_vector.GetValue(0));
	list_entry_t offset_length;
	offset_length.offset = values[0].GetValue<uint64_t>();
	offset_length.length = values[1].GetValue<uint64_t>();

	idx_t child_scan_count = offset_length.offset - state.last_offset;

	if (child_scan_count == 0) {
		return;
	}
	state.last_offset = offset_length.offset;

	// skip the child state forward by the child_scan_count
	child_column->Skip(state.child_states[1], child_scan_count);
}

void ListColumnData::InitializeAppend(ColumnAppendState &state) {
	// initialize the validity append
	ColumnAppendState validity_append_state;
	validity.InitializeAppend(validity_append_state);
	state.child_appends.push_back(std::move(validity_append_state));

	// initialize the child column append
	ColumnAppendState child_append_state;
	child_column->InitializeAppend(child_append_state);
	state.child_appends.push_back(std::move(child_append_state));

	// initialize the offset length append
	ColumnAppendState offset_length_append_state;
	offset_length_column->InitializeAppend(offset_length_append_state);
	state.child_appends.push_back(std::move(offset_length_append_state));
}

void ListColumnData::Append(BaseStatistics &stats, ColumnAppendState &state, Vector &vector, idx_t count) {
	D_ASSERT(count > 0);

	UnifiedVectorFormat list_data;
	vector.ToUnifiedFormat(count, list_data);
	auto &list_validity = list_data.validity;

	// construct the list_entry_t entries to append to the column data
	auto input_offsets = UnifiedVectorFormat::GetData<list_entry_t>(list_data);
	auto start_offset = child_column->GetMaxEntry();
	idx_t child_count = 0;

	bool child_contiguous = true;
	duckdb::vector<Value> offset_length_entries;
	for (idx_t i = 0; i < count; i++) {
		auto input_idx = list_data.sel->get_index(i);
		if (list_validity.RowIsValid(input_idx)) {
			auto &input_list = input_offsets[input_idx];
			if (input_list.offset != child_count) {
				child_contiguous = false;
			}
			child_list_t<Value> offset_length;
			offset_length.push_back(make_pair("offset", Value::BIGINT(start_offset + child_count)));
			offset_length.push_back(make_pair("length", Value::BIGINT(input_list.length)));
			offset_length_entries.push_back(Value::STRUCT(std::move(offset_length)));
			child_count += input_list.length;
		} else {
			child_list_t<Value> offset_length;
			offset_length.push_back(make_pair("offset", Value::BIGINT(start_offset + child_count)));
			offset_length.push_back(make_pair("length", Value::BIGINT(0)));
			offset_length_entries.push_back(Value::STRUCT(std::move(offset_length)));
		}
	}
	auto &list_child = ListVector::GetEntry(vector);
	Vector child_vector(list_child);
	if (!child_contiguous) {
		// if the child of the list vector is a non-contiguous vector (i.e. list elements are repeating or have gaps)
		// we first push a selection vector and flatten the child vector to turn it into a contiguous vector
		SelectionVector child_sel(child_count);
		idx_t current_count = 0;
		for (idx_t i = 0; i < count; i++) {
			auto input_idx = list_data.sel->get_index(i);
			if (list_validity.RowIsValid(input_idx)) {
				auto &input_list = input_offsets[input_idx];
				for (idx_t list_idx = 0; list_idx < input_list.length; list_idx++) {
					//std::cout << "input_idx: " << input_idx << " list_idx: " << list_idx << " child_sel[" << current_count << "]: " << input_list.offset + list_idx << std::endl;
					child_sel.set_index(current_count++, input_list.offset + list_idx);
				}
			}
		}
		D_ASSERT(current_count == child_count);
		child_vector.Slice(list_child, child_sel, child_count);
	}

	validity.Append(stats, state.child_appends[0], vector, count);
	auto offset_length_vector = Vector(offset_length_column->type, offset_length_entries.size());
	for (idx_t i = 0; i < offset_length_entries.size(); i++) {
		offset_length_vector.SetValue(i, offset_length_entries[i]);
	}

	offset_length_column->Append(ListStats::GetOffsetLengthStats(stats), state.child_appends[2], offset_length_vector, offset_length_entries.size());

	// append the child vector
	if (child_count > 0) {
		child_column->Append(ListStats::GetChildStats(stats), state.child_appends[1], child_vector, child_count);
	}
	this->count += count;
}

void ListColumnData::AppendForUpdate(TransactionData transaction, idx_t column_index, const vector<row_t> real_row_ids, BaseStatistics &stats, ColumnAppendState &state, Vector &vector, idx_t count) {
	D_ASSERT(count > 0);

	UnifiedVectorFormat list_data;
	vector.ToUnifiedFormat(count, list_data);
	auto &list_validity = list_data.validity;

	// construct the list_entry_t entries to append to the column data
	auto input_offsets = UnifiedVectorFormat::GetData<list_entry_t>(list_data);
	auto start_offset = child_column->GetMaxEntry();
	idx_t child_count = 0;

	bool child_contiguous = true;
	duckdb::vector<Value> offset_length_entries;
	for (idx_t i = 0; i < count; i++) {
		auto input_idx = list_data.sel->get_index(i);
		if (list_validity.RowIsValid(input_idx)) {
			auto &input_list = input_offsets[input_idx];
			if (input_list.offset != child_count) {
				child_contiguous = false;
			}
			child_list_t<Value> offset_length;
			offset_length.push_back(make_pair("offset", Value::BIGINT(start_offset + child_count)));
			offset_length.push_back(make_pair("length", Value::BIGINT(input_list.length)));
			offset_length_entries.push_back(Value::STRUCT(std::move(offset_length)));
			child_count += input_list.length;
		} else {
			child_list_t<Value> offset_length;
			offset_length.push_back(make_pair("offset", Value::BIGINT(start_offset + child_count)));
			offset_length.push_back(make_pair("length", Value::BIGINT(0)));
			offset_length_entries.push_back(Value::STRUCT(std::move(offset_length)));
		}
	}
	auto &list_child = ListVector::GetEntry(vector);
	Vector child_vector(list_child);
	if (!child_contiguous) {
		// if the child of the list vector is a non-contiguous vector (i.e. list elements are repeating or have gaps)
		// we first push a selection vector and flatten the child vector to turn it into a contiguous vector
		SelectionVector child_sel(child_count);
		idx_t current_count = 0;
		for (idx_t i = 0; i < count; i++) {
			auto input_idx = list_data.sel->get_index(i);
			if (list_validity.RowIsValid(input_idx)) {
				auto &input_list = input_offsets[input_idx];
				for (idx_t list_idx = 0; list_idx < input_list.length; list_idx++) {
					child_sel.set_index(current_count++, input_list.offset + list_idx);
				}
			}
		}
		D_ASSERT(current_count == child_count);
		child_vector.Slice(list_child, child_sel, child_count);
	}

	validity.AppendForUpdate(transaction, column_index, real_row_ids, stats, state.child_appends[0], vector, count);
	auto offset_length_vector = Vector(offset_length_column->type, offset_length_entries.size());
	for (idx_t i = 0; i < offset_length_entries.size(); i++) {
		offset_length_vector.SetValue(i, offset_length_entries[i]);
	}

	offset_length_column->AppendForUpdate(transaction, column_index, real_row_ids, ListStats::GetOffsetLengthStats(stats), state.child_appends[2], offset_length_vector, offset_length_entries.size());

	// append the child vector
	if (child_count > 0) {
		child_column->AppendForUpdate(transaction, column_index, real_row_ids, ListStats::GetChildStats(stats), state.child_appends[1], child_vector, child_count);
	}
	this->count += count;
}

void ListColumnData::AppendColumnForUpdate(TransactionData transaction, BaseStatistics &stats, const vector<column_t> &column_path,
										   Vector &vector, row_t *row_ids, idx_t count, idx_t depth) {
	auto append_column = column_path[depth];
	switch (append_column) {
	case 0:
		// validity column
		validity.AppendColumnForUpdate(transaction, stats, column_path, vector, row_ids, count, depth + 1);
		this->count += count;
		break;
	case 1:
		// child column. Updates to this column are all appends for now, so we shouldn't be here
		child_column->AppendColumnForUpdate(transaction, ListStats::GetChildStats(stats), column_path, vector, row_ids, count, depth + 1);
		break;
	case 2:
		// offset_length column
		offset_length_column->AppendColumnForUpdate(transaction, ListStats::GetOffsetLengthStats(stats), column_path, vector, row_ids, count, depth + 1);
		break;
	default:
		throw NotImplementedException("Update column_path is invalid");
	}
}

void ListColumnData::RevertAppend(row_t start_row) {
	validity.RevertAppend(start_row);
	auto column_count = GetMaxEntry();
	if (column_count > start) {
		offset_length_column->RevertAppend(start_row);

		// revert append in the child column
		auto offset_length = FetchListOffset(column_count - 1);
		child_column->RevertAppend(offset_length.offset);
	}
	this->count = start_row - this->start;
}

idx_t ListColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	throw NotImplementedException("List Fetch");
}

void ListColumnData::Update(TransactionData transaction, idx_t column_index, Vector &update_vector, row_t *row_ids,
                            idx_t update_count) {
	UnifiedVectorFormat list_data;
	update_vector.ToUnifiedFormat(update_count, list_data);
	auto &list_validity = list_data.validity;
	auto start_offset = child_column->GetMaxEntry();
	idx_t child_count = 0;
	bool child_contiguous = true;
	auto input_offsets = UnifiedVectorFormat::GetData<list_entry_t>(list_data);
	duckdb::vector<Value> offset_length_entries;

	for (idx_t i = 0; i < update_count; i++) {
		auto input_idx = list_data.sel->get_index(i);
		child_list_t<Value> offset_length;
		if (list_validity.RowIsValid(input_idx)) {
			auto &input_list = input_offsets[input_idx];
			if (input_list.offset != child_count) {
				child_contiguous = false;
			}
			offset_length.push_back(make_pair("offset", Value::BIGINT(start_offset + child_count)));
			offset_length.push_back(make_pair("length", Value::BIGINT(input_list.length)));
			child_count += input_list.length;
		} else {
			offset_length.push_back(make_pair("offset", Value::BIGINT(start_offset + child_count)));
			offset_length.push_back(make_pair("length", Value::BIGINT(0)));
		}
		offset_length_entries.push_back(Value::STRUCT(std::move(offset_length)));
	}

	auto &list_child = ListVector::GetEntry(update_vector);
	Vector child_vector(list_child);
	if (!child_contiguous) {
		// if the child of the list vector is a non-contiguous vector (i.e. list elements are repeating or have gaps)
		// we first push a selection vector and flatten the child vector to turn it into a contiguous vector
		SelectionVector child_sel(child_count);
		idx_t current_count = 0;
		for (idx_t i = 0; i < update_count; i++) {
			auto input_idx = list_data.sel->get_index(i);
			if (list_validity.RowIsValid(input_idx)) {
				auto &input_list = input_offsets[input_idx];
				for (idx_t list_idx = 0; list_idx < input_list.length; list_idx++) {
					child_sel.set_index(current_count++, input_list.offset + list_idx);
				}
			}
		}
		D_ASSERT(current_count == child_count);
		child_vector.Slice(list_child, child_sel, child_count);
	}

	ColumnAppendState child_append_state;
	child_column->InitializeAppend(child_append_state);
	if (child_count > 0) {
		vector<row_t> real_row_ids(update_count);
		for (idx_t i = 0; i < update_count; i++) {
			real_row_ids[i] = row_ids[i];
		}
		child_column->AppendForUpdate(transaction, column_index, real_row_ids, ListStats::GetChildStats(stats->statistics),
									  child_append_state, child_vector, child_count);
	}
	validity.Update(transaction, column_index, update_vector, row_ids, update_count);

	auto offset_length_vector = Vector(offset_length_column->type, offset_length_entries.size());
	for (idx_t i = 0; i < offset_length_entries.size(); i++) {
		offset_length_vector.SetValue(i, offset_length_entries[i]);
	}
	offset_length_column->Update(transaction, column_index, offset_length_vector, row_ids, update_count);
}

void ListColumnData::UpdateColumn(TransactionData transaction, const vector<column_t> &column_path,
                                  Vector &update_vector, row_t *row_ids, idx_t update_count, idx_t depth) {
	auto update_column = column_path[depth];
	switch (update_column) {
	case 0:
		// validity column
		validity.UpdateColumn(transaction, column_path, update_vector, row_ids, update_count, depth + 1);
		break;
	case 1:
		// child column. Updates to this column are all appends for now, so we shouldn't be here
		child_column->AppendColumnForUpdate(transaction, ListStats::GetChildStats(stats->statistics),
											column_path, update_vector, row_ids, update_count, depth + 1);
		break;
	case 2:
		// offset_length column
		offset_length_column->UpdateColumn(transaction, column_path, update_vector, row_ids, update_count, depth + 1);
		break;
	default:
		throw NotImplementedException("Update column_path is invalid");
	}
}

unique_ptr<BaseStatistics> ListColumnData::GetUpdateStatistics() {
	// check if any child column has updates
	auto stats = BaseStatistics::CreateEmpty(type);
	auto validity_stats = validity.GetUpdateStatistics();
	if (validity_stats) {
		stats.Merge(*validity_stats);
	}
	auto offset_length_stats = offset_length_column->GetUpdateStatistics();
	if (offset_length_stats) {
		ListStats::SetOffsetLengthStats(stats, std::move(offset_length_stats));
	}

	auto child_stats = child_column->GetUpdateStatistics();
	if (child_stats) {
		ListStats::SetChildStats(stats, std::move(child_stats));
	}

	return stats.ToUnique();
}

void ListColumnData::FetchRow(TransactionData transaction, ColumnFetchState &state, row_t row_id, Vector &result,
                              idx_t result_idx) {
	// insert any child states that are required
	// we need two (validity & list child)
	// note that we need a scan state for the child vector
	// this is because we will (potentially) fetch more than one tuple from the list child
	if (state.child_states.empty()) {
		auto child_state = make_uniq<ColumnFetchState>();
		state.child_states.push_back(std::move(child_state));
	}

	// now perform the fetch within the segment
	validity.FetchRow(transaction, *state.child_states[0], row_id, result, result_idx);

	auto &validity = FlatVector::Validity(result);

	Vector offset_length_vector(offset_length_column->type, 1);
	ColumnFetchState fetch_state;
	offset_length_column->FetchRow(transaction, fetch_state, row_id, offset_length_vector, 0);

	auto offset_length_value = StructValue::GetChildren(offset_length_vector.GetValue(0));

	auto list_data = FlatVector::GetData<list_entry_t>(result);
	auto &list_entry = list_data[result_idx];
	// set the list entry offset to the size of the current list
	auto start_offset = offset_length_value[0].GetValue<uint64_t>();
	list_entry.offset = ListVector::GetListSize(result);
	list_entry.length = offset_length_value[1].GetValue<uint64_t>();

	if (!validity.RowIsValid(result_idx)) {
		// the list is NULL! no need to fetch the child
		D_ASSERT(list_entry.length == 0);
		return;
	}

	// now we need to read from the child all the elements between [offset...length]
	auto child_scan_count = list_entry.length;
	if (child_scan_count > 0) {
		auto child_state = make_uniq<ColumnScanState>();
		auto &child_type = ListType::GetChildType(result.GetType());
		Vector child_scan(child_type, child_scan_count);
		// seek the scan towards the specified position and read [length] entries
		child_state->Initialize(child_type);
		child_column->InitializeScanWithOffset(*child_state, start + start_offset);
		D_ASSERT(child_type.InternalType() == PhysicalType::STRUCT ||
		         child_state->row_index + child_scan_count - this->start <= child_column->GetMaxEntry());
		child_column->ScanCount(*child_state, child_scan, child_scan_count);

		ListVector::Append(result, child_scan, child_scan_count);
	}
}

void ListColumnData::CommitDropColumn() {
	validity.CommitDropColumn();
	child_column->CommitDropColumn();
	offset_length_column->CommitDropColumn();
}

struct ListColumnCheckpointState : public ColumnCheckpointState {
	ListColumnCheckpointState(RowGroup &row_group, ColumnData &column_data, PartialBlockManager &partial_block_manager)
	    : ColumnCheckpointState(row_group, column_data, partial_block_manager) {
		global_stats = ListStats::CreateEmpty(column_data.type).ToUnique();
	}

	unique_ptr<ColumnCheckpointState> validity_state;
	unique_ptr<ColumnCheckpointState> child_state;
	unique_ptr<ColumnCheckpointState> offset_length_state;

public:
	unique_ptr<BaseStatistics> GetStatistics() override {
		auto stats = global_stats->Copy();
		ListStats::SetChildStats(stats, child_state->GetStatistics());
		ListStats::SetOffsetLengthStats(stats, offset_length_state->GetStatistics());
		return stats.ToUnique();
	}

	void WriteDataPointers(RowGroupWriter &writer, Serializer &serializer) override {
		serializer.WriteObject(101, "validity",
		                       [&](Serializer &serializer) { validity_state->WriteDataPointers(writer, serializer); });
		serializer.WriteObject(102, "child_column",
		                       [&](Serializer &serializer) { child_state->WriteDataPointers(writer, serializer); });
		serializer.WriteObject(103, "offset_length_column",
		                       [&](Serializer &serializer) { offset_length_state->WriteDataPointers(writer, serializer); });
	}
};

unique_ptr<ColumnCheckpointState> ListColumnData::CreateCheckpointState(RowGroup &row_group,
                                                                        PartialBlockManager &partial_block_manager) {
	return make_uniq<ListColumnCheckpointState>(row_group, *this, partial_block_manager);
}

unique_ptr<ColumnCheckpointState> ListColumnData::Checkpoint(RowGroup &row_group,
                                                             PartialBlockManager &partial_block_manager,
                                                             ColumnCheckpointInfo &checkpoint_info) {
	auto base_state = ColumnData::Checkpoint(row_group, partial_block_manager, checkpoint_info);
	auto validity_state = validity.Checkpoint(row_group, partial_block_manager, checkpoint_info);
	auto child_state = child_column->Checkpoint(row_group, partial_block_manager, checkpoint_info);
	auto offset_length_state = offset_length_column->Checkpoint(row_group, partial_block_manager, checkpoint_info);

	auto &checkpoint_state = base_state->Cast<ListColumnCheckpointState>();
	checkpoint_state.validity_state = std::move(validity_state);
	checkpoint_state.child_state = std::move(child_state);
	checkpoint_state.offset_length_state = std::move(offset_length_state);
	return base_state;
}

void ListColumnData::DeserializeColumn(Deserializer &deserializer) {
	deserializer.ReadObject(101, "validity",
	                        [&](Deserializer &deserializer) { validity.DeserializeColumn(deserializer); });

	deserializer.ReadObject(102, "child_column",
	                        [&](Deserializer &deserializer) { child_column->DeserializeColumn(deserializer); });

	deserializer.ReadObject(103, "offset_length_column",
	                        [&](Deserializer &deserializer) { offset_length_column->DeserializeColumn(deserializer); });
	this->count = validity.count;
}

void ListColumnData::GetColumnSegmentInfo(duckdb::idx_t row_group_index, vector<duckdb::idx_t> col_path,
                                          vector<duckdb::ColumnSegmentInfo> &result) {
	col_path.push_back(0);
	validity.GetColumnSegmentInfo(row_group_index, col_path, result);
	col_path.back() = 1;
	child_column->GetColumnSegmentInfo(row_group_index, col_path, result);
	col_path.back() = 2;
	offset_length_column->GetColumnSegmentInfo(row_group_index, col_path, result);
}

} // namespace duckdb
