#include "duckdb/transaction/undo_buffer.hpp"

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/list.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/write_ahead_log.hpp"
#include "duckdb/transaction/cleanup_state.hpp"
#include "duckdb/transaction/commit_state.hpp"
#include "duckdb/transaction/rollback_state.hpp"

namespace duckdb {
constexpr uint32_t UNDO_ENTRY_HEADER_SIZE = sizeof(UndoFlags) + sizeof(uint32_t);

UndoBuffer::UndoBuffer(ClientContext &context_p) : allocator(BufferAllocator::Get(context_p)) {
}

data_ptr_t UndoBuffer::CreateEntry(UndoFlags type, idx_t len) {
	D_ASSERT(len <= NumericLimits<uint32_t>::Maximum());
	len = AlignValue(len);
	idx_t needed_space = len + UNDO_ENTRY_HEADER_SIZE;
	auto data = allocator.Allocate(needed_space);
	ordered_entries.push_back(data);
	Store<UndoFlags>(type, data);
	data += sizeof(UndoFlags);
	Store<uint32_t>(len, data);
	data += sizeof(uint32_t);
	return data;
}

data_ptr_t UndoBuffer::ResizeEntryInPlace(data_ptr_t old, UndoFlags type, idx_t len) {
	D_ASSERT(len <= NumericLimits<uint32_t>::Maximum());
	size_t index = 0;
	for (; index < ordered_entries.size(); index++) {
		auto entry = ordered_entries[index];
		auto entry_type = Load<UndoFlags>(entry);
		entry += sizeof(UndoFlags);
		auto entry_len = Load<uint32_t>(entry);
		entry += sizeof(uint32_t);
		if (entry_type == type && entry == old) {
			// found the entry, now check if it is large enough
			if (entry_len >= len) {
				// The old entry is already large enough, return it
				return old;
			}
			break;
		}
	}

	len = AlignValue(len);
	idx_t needed_space = len + UNDO_ENTRY_HEADER_SIZE;
	data_ptr_t data = nullptr;
	if (ordered_entries.size() > 0 && index < ordered_entries.size()) {
		// found the entry, replace it
		auto old_len = Load<uint32_t>(old - sizeof(uint32_t)) + UNDO_ENTRY_HEADER_SIZE;
		data = allocator.Reallocate(old - UNDO_ENTRY_HEADER_SIZE, old_len, needed_space);
		ordered_entries[index] = data;
	} else {
		// did not find the entry, append it
		data = allocator.Allocate(needed_space);
		ordered_entries.push_back(data);
	}
	Store<UndoFlags>(type, data);
	data += sizeof(UndoFlags);
	Store<uint32_t>(len, data);
	data += sizeof(uint32_t);
	return data;
}

template <class T>
void UndoBuffer::IterateEntries(UndoBuffer::IteratorState &state, T &&callback) {
	// iterate in insertion order
	for (size_t index = 0; index < ordered_entries.size(); ++index) {
		state.current_index = index;
		auto entry = ordered_entries[index];
		auto entry_type = Load<UndoFlags>(entry);
		entry += UNDO_ENTRY_HEADER_SIZE;
		callback(entry_type, entry);
	}
}

template <class T>
void UndoBuffer::IterateEntries(UndoBuffer::IteratorState &state, UndoBuffer::IteratorState &end_state, T &&callback) {
	// iterate in insertion order
	for (size_t index = 0; index < ordered_entries.size(); ++index) {
		state.current_index = index;
		auto entry = ordered_entries[index];
		auto entry_type = Load<UndoFlags>(entry);
		entry += UNDO_ENTRY_HEADER_SIZE;
		callback(entry_type, entry);
		if (state.current_index == end_state.current_index) {
			// finished executing until the current end state
			return;
		}
	}
}

template <class T>
void UndoBuffer::ReverseIterateEntries(T &&callback) {
	// iterate in reverse insertion order
	size_t num_entries = ordered_entries.size();
	for (size_t index = 0; index < num_entries; ++index) {
		auto entry = ordered_entries[num_entries - index - 1];
		auto entry_type = Load<UndoFlags>(entry);
		entry += UNDO_ENTRY_HEADER_SIZE;
		callback(entry_type, entry);
	}
}

bool UndoBuffer::ChangesMade() {
	return !allocator.IsEmpty();
}

idx_t UndoBuffer::EstimatedSize() {

	idx_t estimated_size = 0;
	auto node = allocator.GetHead();
	while (node) {
		estimated_size += node->current_position;
		node = node->next.get();
	}

	// we need to search for any index creation entries
	IteratorState iterator_state;
	IterateEntries(iterator_state, [&](UndoFlags entry_type, data_ptr_t data) {
		if (entry_type == UndoFlags::CATALOG_ENTRY) {
			auto catalog_entry = Load<CatalogEntry *>(data);
			if (catalog_entry->Parent().type == CatalogType::INDEX_ENTRY) {
				auto &index = catalog_entry->Parent().Cast<DuckIndexEntry>();
				estimated_size += index.initial_index_size;
			}
		}
	});

	return estimated_size;
}

void UndoBuffer::Cleanup() {
	// garbage collect everything in the Undo Chunk
	// this should only happen if
	//  (1) the transaction this UndoBuffer belongs to has successfully
	//  committed
	//      (on Rollback the Rollback() function should be called, that clears
	//      the chunks)
	//  (2) there is no active transaction with start_id < commit_id of this
	//  transaction
	CleanupState state;
	UndoBuffer::IteratorState iterator_state;
	IterateEntries(iterator_state, [&](UndoFlags type, data_ptr_t data) { state.CleanupEntry(type, data); });

	// possibly vacuum indexes
	for (const auto &table : state.indexed_tables) {
		table.second->info->indexes.Scan([&](Index &index) {
			index.Vacuum();
			return false;
		});
	}
}

void UndoBuffer::Commit(UndoBuffer::IteratorState &iterator_state, optional_ptr<WriteAheadLog> log,
                        transaction_t commit_id) {
	CommitState state(commit_id, log);
	if (log) {
		// commit WITH write ahead log
		IterateEntries(iterator_state, [&](UndoFlags type, data_ptr_t data) { state.CommitEntry<true>(type, data); });
	} else {
		// commit WITHOUT write ahead log
		IterateEntries(iterator_state, [&](UndoFlags type, data_ptr_t data) { state.CommitEntry<false>(type, data); });
	}
}

void UndoBuffer::RevertCommit(UndoBuffer::IteratorState &end_state, transaction_t transaction_id) {
	CommitState state(transaction_id, nullptr);
	UndoBuffer::IteratorState start_state;
	IterateEntries(start_state, end_state, [&](UndoFlags type, data_ptr_t data) { state.RevertCommit(type, data); });
}

void UndoBuffer::Rollback() noexcept {
	// rollback needs to be performed in reverse
	RollbackState state;
	ReverseIterateEntries([&](UndoFlags type, data_ptr_t data) { state.RollbackEntry(type, data); });
}
} // namespace duckdb
