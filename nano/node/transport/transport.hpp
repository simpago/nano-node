#pragma once

#include <nano/lib/locks.hpp>
#include <nano/lib/stats.hpp>
#include <nano/node/common.hpp>
#include <nano/node/socket.hpp>

#include <boost/asio/ip/network_v6.hpp>

namespace rsnano
{
class BandwidthLimiterHandle;
class ChannelHandle;
}
namespace nano
{
class bandwidth_limiter final
{
public:
	// initialize with limit 0 = unbounded
	bandwidth_limiter (double, std::size_t);
	~bandwidth_limiter ();
	bool should_drop (std::size_t const &);
	void reset (double, std::size_t);

private:
	bandwidth_limiter (const bandwidth_limiter &);
	rsnano::BandwidthLimiterHandle * handle;
};

namespace transport
{
	nano::endpoint map_endpoint_to_v6 (nano::endpoint const &);
	nano::endpoint map_tcp_to_endpoint (nano::tcp_endpoint const &);
	nano::tcp_endpoint map_endpoint_to_tcp (nano::endpoint const &);
	boost::asio::ip::address map_address_to_subnetwork (boost::asio::ip::address const &);
	boost::asio::ip::address ipv4_address_or_ipv6_subnet (boost::asio::ip::address const &);
	boost::asio::ip::address_v6 mapped_from_v4_bytes (unsigned long);
	boost::asio::ip::address_v6 mapped_from_v4_or_v6 (boost::asio::ip::address const &);
	bool is_ipv4_or_v4_mapped_address (boost::asio::ip::address const &);

	// Unassigned, reserved, self
	bool reserved_address (nano::endpoint const &, bool = false);
	static std::chrono::seconds constexpr syn_cookie_cutoff = std::chrono::seconds (5);
	enum class transport_type : uint8_t
	{
		undefined = 0,
		udp = 1,
		tcp = 2,
		loopback = 3
	};
	class channel
	{
	public:
		channel (rsnano::ChannelHandle * handle_a, nano::stat & stats_a, nano::logger_mt & logger_a, nano::bandwidth_limiter & limiter_a, boost::asio::io_context & io_ctx_a, bool network_packet_logging_a);
		channel (nano::transport::channel const &) = delete;
		virtual ~channel ();
		virtual std::size_t hash_code () const = 0;
		virtual bool operator== (nano::transport::channel const &) const = 0;
		void send (nano::message & message_a, std::function<void (boost::system::error_code const &, std::size_t)> const & callback_a = nullptr, nano::buffer_drop_policy policy_a = nano::buffer_drop_policy::limiter);
		// TODO: investigate clang-tidy warning about default parameters on virtual/override functions
		//
		virtual void send_buffer (nano::shared_const_buffer const &, std::function<void (boost::system::error_code const &, std::size_t)> const & = nullptr, nano::buffer_drop_policy = nano::buffer_drop_policy::limiter) = 0;
		virtual std::string to_string () const = 0;
		virtual nano::endpoint get_endpoint () const = 0;
		virtual nano::tcp_endpoint get_tcp_endpoint () const = 0;
		virtual nano::transport::transport_type get_type () const = 0;
		virtual bool max ()
		{
			return false;
		}

		virtual std::chrono::steady_clock::time_point get_last_bootstrap_attempt () const = 0;
		virtual void set_last_bootstrap_attempt (std::chrono::steady_clock::time_point const time_a) = 0;

		virtual std::chrono::steady_clock::time_point get_last_packet_received () const = 0;
		virtual void set_last_packet_received (std::chrono::steady_clock::time_point const time_a) = 0;

		virtual std::chrono::steady_clock::time_point get_last_packet_sent () const = 0;
		virtual void set_last_packet_sent (std::chrono::steady_clock::time_point const time_a) = 0;

		virtual boost::optional<nano::account> get_node_id_optional () const = 0;
		virtual nano::account get_node_id () const = 0;
		virtual void set_node_id (nano::account node_id_a) = 0;

		virtual uint8_t get_network_version () const = 0;
		virtual void set_network_version (uint8_t network_version_a) = 0;

	private:
		boost::asio::io_context & io_ctx;
		nano::stat & stats;
		nano::logger_mt & logger;
		nano::bandwidth_limiter & limiter;
		bool network_packet_logging;

	public:
		rsnano::ChannelHandle * handle;
	};
} // namespace transport
} // namespace nano

namespace std
{
template <>
struct hash<::nano::transport::channel>
{
	std::size_t operator() (::nano::transport::channel const & channel_a) const
	{
		return channel_a.hash_code ();
	}
};
template <>
struct equal_to<std::reference_wrapper<::nano::transport::channel const>>
{
	bool operator() (std::reference_wrapper<::nano::transport::channel const> const & lhs, std::reference_wrapper<::nano::transport::channel const> const & rhs) const
	{
		return lhs.get () == rhs.get ();
	}
};
}

namespace boost
{
template <>
struct hash<::nano::transport::channel>
{
	std::size_t operator() (::nano::transport::channel const & channel_a) const
	{
		std::hash<::nano::transport::channel> hash;
		return hash (channel_a);
	}
};
template <>
struct hash<std::reference_wrapper<::nano::transport::channel const>>
{
	std::size_t operator() (std::reference_wrapper<::nano::transport::channel const> const & channel_a) const
	{
		std::hash<::nano::transport::channel> hash;
		return hash (channel_a.get ());
	}
};
}
