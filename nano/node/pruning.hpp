#pragma once

#include <nano/lib/numbers.hpp>
#include <nano/node/fwd.hpp>

#include <deque>
#include <thread>

namespace nano
{
class pruning final
{
public:
	pruning (nano::node_config const &, nano::node_flags const &, nano::ledger &, nano::stats &, nano::logger &);
	~pruning ();

	void start ();
	void stop ();

	nano::container_info container_info () const;

	void ongoing_ledger_pruning ();
	void ledger_pruning (uint64_t batch_size, bool bootstrap_weight_reached);
	bool collect_ledger_pruning_targets (std::deque<nano::block_hash> & pruning_targets_out, nano::account & last_account_out, uint64_t batch_read_size, uint64_t max_depth, uint64_t cutoff_time);

private: // Dependencies
	nano::node_config const & config;
	nano::node_flags const & flags;
	nano::ledger & ledger;
	nano::stats & stats;
	nano::logger & logger;

private:
	void run ();

	std::atomic<bool> stopped{ false };
	nano::thread_pool workers;
};
}