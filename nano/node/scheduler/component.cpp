#include <nano/node/node.hpp>
#include <nano/node/scheduler/component.hpp>
#include <nano/node/scheduler/hinted.hpp>
#include <nano/node/scheduler/manual.hpp>
#include <nano/node/scheduler/optimistic.hpp>
#include <nano/node/scheduler/priority.hpp>

nano::scheduler::component::component (nano::node & node) :
	hinted_impl{ std::make_unique<nano::scheduler::hinted> (node.config->hinted_scheduler, node, node.vote_cache, node.active, node.online_reps, *node.stats) },
	manual_impl{ std::make_unique<nano::scheduler::manual> (node) },
	optimistic_impl{ std::make_unique<nano::scheduler::optimistic> (node.config->optimistic_scheduler, node, node.ledger, node.active, node.network_params.network, *node.stats) },
	priority_impl{ std::make_unique<nano::scheduler::priority> (node, *node.stats) },
	hinted{ *hinted_impl },
	manual{ *manual_impl },
	optimistic{ *optimistic_impl },
	priority{ *priority_impl }
{
}

nano::scheduler::component::component (rsnano::NodeHandle * handle) :
	hinted_impl{ std::make_unique<nano::scheduler::hinted> (rsnano::rsn_node_hinted (handle)) },
	manual_impl{ std::make_unique<nano::scheduler::manual> (rsnano::rsn_node_manual (handle)) },
	optimistic_impl{ std::make_unique<nano::scheduler::optimistic> (rsnano::rsn_node_optimistic (handle)) },
	priority_impl{ std::make_unique<nano::scheduler::priority> (rsnano::rsn_node_priority (handle)) },
	hinted{ *hinted_impl },
	manual{ *manual_impl },
	optimistic{ *optimistic_impl },
	priority{ *priority_impl }
{
}

nano::scheduler::component::~component ()
{
}

void nano::scheduler::component::start ()
{
	hinted.start ();
	manual.start ();
	optimistic.start ();
	priority.start ();
}

void nano::scheduler::component::stop ()
{
	hinted.stop ();
	manual.stop ();
	optimistic.stop ();
	priority.stop ();
}
