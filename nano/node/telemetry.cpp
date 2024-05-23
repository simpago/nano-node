#include "nano/lib/rsnano.hpp"
#include "nano/lib/rsnanoutils.hpp"
#include "nano/node/messages.hpp"

#include <nano/lib/blocks.hpp>
#include <nano/lib/stats.hpp>
#include <nano/lib/threading.hpp>
#include <nano/node/network.hpp>
#include <nano/node/node.hpp>
#include <nano/node/node_observers.hpp>
#include <nano/node/telemetry.hpp>
#include <nano/node/transport/transport.hpp>
#include <nano/secure/ledger.hpp>

#include <boost/algorithm/string.hpp>

#include <memory>
#include <optional>

using namespace std::chrono_literals;

nano::telemetry::telemetry (rsnano::TelemetryHandle * handle) :
	handle{ handle }
{
}

nano::telemetry::~telemetry ()
{
	rsnano::rsn_telemetry_destroy (handle);
}

void nano::telemetry::start ()
{
	rsnano::rsn_telemetry_start (handle);
}

void nano::telemetry::stop ()
{
	rsnano::rsn_telemetry_stop (handle);
}

void nano::telemetry::trigger ()
{
	rsnano::rsn_telemetry_trigger (handle);
}

nano::telemetry_data nano::telemetry::local_telemetry () const
{
	return { rsnano::rsn_telemetry_local_telemetry (handle) };
}

std::size_t nano::telemetry::size () const
{
	return rsnano::rsn_telemetry_len (handle);
}

std::optional<nano::telemetry_data> nano::telemetry::get_telemetry (const nano::endpoint & endpoint) const
{
	auto dto{ rsnano::udp_endpoint_to_dto (endpoint) };
	auto data_handle = rsnano::rsn_telemetry_get_telemetry (handle, &dto);
	if (data_handle != nullptr)
	{
		return { nano::telemetry_data{ data_handle } };
	}
	else
	{
		return std::nullopt;
	}
}

std::unordered_map<nano::endpoint, nano::telemetry_data> nano::telemetry::get_all_telemetries () const
{
	auto map_handle = rsnano::rsn_telemetry_get_all (handle);
	auto size = rsnano::rsn_telemetry_data_map_len (map_handle);
	std::unordered_map<nano::endpoint, nano::telemetry_data> result;
	for (auto i = 0; i < size; ++i)
	{
		rsnano::EndpointDto endpoint_dto;
		auto data_handle = rsnano::rsn_telemetry_data_map_get (map_handle, i, &endpoint_dto);
		auto endpoint = rsnano::dto_to_udp_endpoint (endpoint_dto);
		result.emplace (endpoint, nano::telemetry_data{ data_handle });
	}
	rsnano::rsn_telemetry_data_map_destroy (map_handle);
	return result;
}

std::unique_ptr<nano::container_info_component> nano::telemetry::collect_container_info (const std::string & name)
{
	return std::make_unique<container_info_composite> (rsnano::rsn_telemetry_collect_container_info (handle, name.c_str ()));
}

nano::telemetry_data nano::consolidate_telemetry_data (std::vector<nano::telemetry_data> const & telemetry_datas)
{
	std::vector<rsnano::TelemetryDataHandle *> data_handles;
	data_handles.reserve (telemetry_datas.size ());
	for (auto const & i : telemetry_datas)
	{
		data_handles.push_back (i.handle);
	}
	return { rsnano::rsn_consolidate_telemetry_data (data_handles.data (), data_handles.size ()) };
}
