#include <nano/lib/assert.hpp>
#include <nano/lib/interval.hpp>
#include <nano/lib/thread_roles.hpp>
#include <nano/node/network.hpp>
#include <nano/node/vote_processor.hpp>
#include <nano/node/vote_rebroadcaster.hpp>
#include <nano/node/vote_router.hpp>
#include <nano/node/wallet.hpp>
#include <nano/secure/vote.hpp>

nano::vote_rebroadcaster::vote_rebroadcaster (nano::vote_router & vote_router_a, nano::network & network_a, nano::wallets & wallets_a, nano::stats & stats_a, nano::logger & logger_a) :
	vote_router{ vote_router_a },
	network{ network_a },
	wallets{ wallets_a },
	stats{ stats_a },
	logger{ logger_a }
{
	vote_router.vote_processed.add ([this] (std::shared_ptr<nano::vote> const & vote, nano::vote_source source, std::unordered_map<nano::block_hash, nano::vote_code> const & results) {
		bool processed = std::any_of (results.begin (), results.end (), [] (auto const & result) {
			return result.second == nano::vote_code::vote;
		});
		if (processed && enable)
		{
			put (vote);
		}
	});
}

nano::vote_rebroadcaster::~vote_rebroadcaster ()
{
	debug_assert (!thread.joinable ());
}

void nano::vote_rebroadcaster::start ()
{
	debug_assert (!thread.joinable ());

	thread = std::thread ([this] () {
		nano::thread_role::set (nano::thread_role::name::vote_rebroadcasting);
		run ();
	});
}

void nano::vote_rebroadcaster::stop ()
{
	{
		std::lock_guard guard{ mutex };
		stopped = true;
	}
	condition.notify_all ();
	if (thread.joinable ())
	{
		thread.join ();
	}
}

bool nano::vote_rebroadcaster::put (std::shared_ptr<nano::vote> const & vote)
{
	bool added{ false };
	{
		std::lock_guard guard{ mutex };
		if (queue.size () < max_queue)
		{
			if (!reps.exists (vote->account))
			{
				queue.push_back (vote);
				added = true;
			}
		}
	}
	if (added)
	{
		stats.inc (nano::stat::type::vote_rebroadcaster, nano::stat::detail::queued);
		condition.notify_one ();
	}
	else
	{
		stats.inc (nano::stat::type::vote_rebroadcaster, nano::stat::detail::overfill);
	}
	return added;
}

void nano::vote_rebroadcaster::run ()
{
	std::unique_lock lock{ mutex };
	while (!stopped)
	{
		condition.wait (lock, [&] {
			return stopped || !queue.empty ();
		});

		if (stopped)
		{
			return;
		}

		stats.inc (nano::stat::type::vote_rebroadcaster, nano::stat::detail::loop);

		if (refresh_interval.elapse (15s))
		{
			stats.inc (nano::stat::type::vote_rebroadcaster, nano::stat::detail::refresh);

			reps = wallets.reps ();
			enable = !reps.have_half_rep (); // Disable vote rebroadcasting if the node has a principal representative (or close to)
		}

		if (!queue.empty ())
		{
			auto vote = queue.front ();
			queue.pop_front ();

			lock.unlock ();

			stats.inc (nano::stat::type::vote_rebroadcaster, nano::stat::detail::rebroadcast);
			stats.add (nano::stat::type::vote_rebroadcaster, nano::stat::detail::rebroadcast_hashes, vote->hashes.size ());
			network.flood_vote (vote, 0.5f, /* rebroadcasted */ true); // TODO: Track number of peers that we sent the vote to

			lock.lock ();
		}
	}
}

nano::container_info nano::vote_rebroadcaster::container_info () const
{
	std::lock_guard guard{ mutex };

	nano::container_info info;
	info.put ("queue", queue.size ());
	return info;
}