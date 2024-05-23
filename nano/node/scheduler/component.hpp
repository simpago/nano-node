#pragma once

#include "nano/lib/rsnano.hpp"

#include <nano/lib/locks.hpp>
#include <nano/node/election.hpp>

#include <memory>
#include <string>

namespace nano
{
class container_info_component;
class node;
class block;
namespace store
{
	class read_transaction;
}
}
namespace nano::scheduler
{
class hinted;
class manual;
class optimistic;
class priority;

class component final
{
	std::unique_ptr<nano::scheduler::hinted> hinted_impl;
	std::unique_ptr<nano::scheduler::manual> manual_impl;
	std::unique_ptr<nano::scheduler::optimistic> optimistic_impl;
	std::unique_ptr<nano::scheduler::priority> priority_impl;

public:
	explicit component (nano::node & node);
	explicit component (rsnano::NodeHandle *);
	~component ();

	// Starts all schedulers
	void start ();
	// Stops all schedulers
	void stop ();

	nano::scheduler::hinted & hinted;
	nano::scheduler::manual & manual;
	nano::scheduler::optimistic & optimistic;
	nano::scheduler::priority & priority;
};
}
