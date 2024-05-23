#pragma once

#include <nano/lib/utility.hpp>
#include <nano/node/common.hpp>
#include <nano/node/messages.hpp>
#include <nano/node/nodeconfig.hpp>
#include <nano/secure/common.hpp>

#include <memory>
#include <optional>

namespace nano
{
class node;
class network;
class node_observers;
class stats;
class ledger;
class thread_pool;
class unchecked_map;
namespace transport
{
	class channel;
}

/**
 * This class periodically broadcasts and requests telemetry from peers.
 * Those intervals are configurable via `telemetry_request_interval` & `telemetry_broadcast_interval` network constants
 * Telemetry datas are only removed after becoming stale (configurable via `telemetry_cache_cutoff` network constant), so peer data will still be available for a short period after that peer is disconnected
 *
 * Requests can be disabled via `disable_ongoing_telemetry_requests` node flag
 * Broadcasts can be disabled via `disable_providing_telemetry_metrics` node flag
 *
 */
class telemetry
{
public:
	struct config
	{
		bool enable_ongoing_requests{ true };
		bool enable_ongoing_broadcasts{ true };

		config (nano::node_config const & config, nano::node_flags const & flags) :
			enable_ongoing_requests{ !flags.disable_ongoing_telemetry_requests () },
			enable_ongoing_broadcasts{ !flags.disable_providing_telemetry_metrics () }
		{
		}
	};

public:
	telemetry (rsnano::TelemetryHandle * handle);
	telemetry (telemetry const &) = delete;
	~telemetry ();

	void start ();
	void stop ();

	/**
	 * Trigger manual telemetry request to all peers
	 */
	void trigger ();

	nano::telemetry_data local_telemetry () const;

	std::size_t size () const;

	/**
	 * Returns telemetry for selected endpoint
	 */
	std::optional<nano::telemetry_data> get_telemetry (nano::endpoint const &) const;

	/**
	 * Returns all available telemetry
	 */
	std::unordered_map<nano::endpoint, nano::telemetry_data> get_all_telemetries () const;

	rsnano::TelemetryHandle * handle;
};

nano::telemetry_data consolidate_telemetry_data (std::vector<telemetry_data> const & telemetry_data);
}
